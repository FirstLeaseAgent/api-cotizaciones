import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal, getcontext
from docx import Document
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import uuid
import requests
import subprocess
from utils.parser import extraer_variables


# -------------------------------------------------
# CONFIGURACIÓN GENERAL
# -------------------------------------------------
getcontext().prec = 28

app = FastAPI(
    title="API Unificada de Cotización y Documentos",
    description="Servicio que calcula cotizaciones, administra plantillas y genera documentos Word/PDF."
)

TEMPLATES_DIR = "templates"
OUTPUT_DIR = "outputs"
DB_PATH = "db.json"
DIV_PLAN = Decimal("48")   # 48%
GESTORIA = Decimal("2000") # $2,000
TIMEZONE = ZoneInfo("America/Mexico_City")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------
# PLANTILLA PRINCIPAL (carga automática desde GitHub)
# -------------------------------------------------
GITHUB_RAW_URL = (
    "https://github.com/FirstLeaseAgent/api-cotizaciones/raw/refs/heads/main/"
    "api-cotizaciones/templates/Plantilla_Cotizacion.docx"
)
TEMPLATE_NAME = "Plantilla_Cotizacion.docx"


def ensure_template_available():
    if not os.path.exists(DB_PATH) or os.stat(DB_PATH).st_size == 0:
        with open(DB_PATH, "w") as f:
            json.dump({"plantillas": []}, f, indent=4)

    template_path = os.path.join(TEMPLATES_DIR, TEMPLATE_NAME)

    # Descarga si no existe
    if not os.path.exists(template_path):
        print("🔄 Descargando plantilla desde GitHub…")
        resp = requests.get(GITHUB_RAW_URL)
        resp.raise_for_status()
        with open(template_path, "wb") as f:
            f.write(resp.content)
        print("✅ Plantilla descargada.")

    # Registrar plantilla en DB
    with open(DB_PATH, "r+") as f:
        data = json.load(f)
        if not data["plantillas"]:
            plantilla_id = str(uuid.uuid4())
            data["plantillas"].append({
                "id": plantilla_id,
                "nombre": TEMPLATE_NAME,
                "variables": []
            })
            f.seek(0)
            f.truncate()
            json.dump(data, f, indent=4)
            print("✅ Plantilla registrada en db.json")


ensure_template_available()


# -------------------------------------------------
# MODELO DE ENTRADA
# -------------------------------------------------
class CotizacionRequest(BaseModel):
    nombre: str
    nombre_activo: str
    valor: float
    enganche: Optional[float] = 10.0
    tasa_anual: Optional[float] = 27.0
    comision: Optional[float] = 3.0
    rentas_deposito: Optional[float] = 0.0

    seguro_anual: Optional[float] = -1     # None o -1 = se calcula
    seguro_contado: bool = False     # True = contado / False = financiado


# -------------------------------------------------
# SEGURO ANUAL
# -------------------------------------------------
def calcular_seguro_anual(valor_con_iva: float, entrada: Optional[float]) -> Decimal:
    """
    Calcula el seguro anual SIN IVA.

    - Si entrada es None o -1 → usa tabla con VALOR CON IVA.
    - Si entrada es 0 → seguro gratuito.
    - Si entrada > 0 → se usa el valor proporcionado (ya sin IVA).
    """

    # Caso: usar tabla
    if entrada is None or entrada == -1:
        v_con_iva = Decimal(str(valor_con_iva))
        valor_sin_iva = v_con_iva / Decimal("1.16")

        # Rangos basados EN VALOR CON IVA
        if v_con_iva <= Decimal("500000"):
            pct = Decimal("0.04")
        elif v_con_iva <= Decimal("750000"):
            pct = Decimal("0.035")
        elif v_con_iva <= Decimal("1000000"):
            pct = Decimal("0.03")
        elif v_con_iva <= Decimal("1500000"):
            pct = Decimal("0.0275")
        elif v_con_iva <= Decimal("5000000"):
            pct = Decimal("0.025")
        else:
            pct = Decimal("0.025")

        # El seguro siempre se calcula sobre el valor SIN IVA
        return (valor_sin_iva * pct).quantize(Decimal("0.01"))

    # Caso: seguro gratuito
    if entrada == 0:
        return Decimal("0")

    # Caso: monto proporcionado (ya sin IVA)
    return Decimal(str(entrada))


# -------------------------------------------------
# CÁLCULO FINANCIERO COMPLETO
# -------------------------------------------------
def calcular_pago_mensual(
    valor,
    enganche,
    tasa_anual,
    plazo_meses,
    valor_residual,
    comision,
    rentas_deposito,
    seguro_anual,
    seguro_contado_flag,
    div_plan,
    gestoria,
):

    valor_sin_iva = Decimal(valor) / Decimal("1.16")
    r = Decimal(tasa_anual) / Decimal(1200)
    n = Decimal(plazo_meses)

    # Valor presente del activo
    pv = valor_sin_iva * (1 - Decimal(enganche) / 100)
    fv = valor_sin_iva * (Decimal(valor_residual) / 100)

    # ------------------------- PAGO DE RENTA -------------------------
    if r == 0:
        pago = -(pv - fv) / n
    else:
        pago = ((pv - fv * ((1 + r) ** (-n))) * r) / (1 - (1 + r) ** (-n))

    # ------------------------- SEGURO -------------------------
    pv_seguro = seguro_anual

    if seguro_contado_flag and pv_seguro > 0:
        pago_seguro = Decimal("0")
        monto_seguro_contado = pv_seguro
    else:
        n_seg = Decimal(12)
        if r == 0:
            pago_seguro = -pv_seguro / n_seg
        else:
            pago_seguro = (pv_seguro * r) / (1 - (1 + r) ** (-n_seg))
        monto_seguro_contado = Decimal("0")

    # ------------------------- GESTORÍA -------------------------
    pv_gest = gestoria
    if r == 0:
        pago_gestoria = -pv_gest / n
    else:
        pago_gestoria = (pv_gest * r) / (1 - (1 + r) ** (-n))

    # ------------------------- PAGO TOTAL MENSUAL -------------------------
    subtotal_mensual = pago + pago_seguro + pago_gestoria

    total_mensual_sin_gestoria_con_iva = subtotal_mensual * Decimal("1.16")
    monto_deposito = Decimal(rentas_deposito) * total_mensual_sin_gestoria_con_iva
    primera_mensualidad = subtotal_mensual

    monto_enganche = valor_sin_iva * Decimal(enganche / 100)
    monto_comision = pv * Decimal(comision / 100)

    subtotal_inicial = (
        monto_enganche
        + monto_comision
        + monto_deposito
        + primera_mensualidad
        + monto_seguro_contado
    )

    iva_inicial = (
        monto_enganche
        + monto_comision
        + primera_mensualidad
        + monto_seguro_contado
    ) * Decimal("0.16")

    total_inicial = subtotal_inicial + iva_inicial

    # ------------------------- PLAN VS RENTA -------------------------
    div = div_plan / Decimal(100)

    renta_plan = subtotal_mensual * div
    renta_mensual = subtotal_mensual * (1 - div)

    iva_mensual = subtotal_mensual * Decimal("0.16")
    total_mensual = subtotal_mensual * Decimal("1.16")

    # ------------------------- RESIDUAL -------------------------
    monto_residual = valor_sin_iva * (Decimal(valor_residual) / 100)
    iva_residual = monto_residual * Decimal("0.16")
    total_residual = monto_residual * Decimal("1.16")

    total_final = total_residual - monto_deposito

    return {
        "Enganche": float(round(monto_enganche, 2)),
        "Comision": float(round(monto_comision, 2)),
        "Seguro_Contado": float(round(monto_seguro_contado, 2)),
        "Renta_en_Deposito": float(round(monto_deposito, 2)),
        "Primera_Mensualidad": float(round(primera_mensualidad, 2)),
        "Subtotal_Pago_Inicial": float(round(subtotal_inicial, 2)),
        "IVA_Pago_Inicial": float(round(iva_inicial, 2)),
        "Total_Inicial": float(round(total_inicial, 2)),

        "Renta_Plan": float(round(renta_plan, 2)),
        "Renta_Mensual": float(round(renta_mensual, 2)),
        "Subtotal_Mensual": float(round(subtotal_mensual, 2)),
        "IVA_Mensual": float(round(iva_mensual, 2)),
        "Total_Mensual": float(round(total_mensual, 2)),

        "Residual": float(round(monto_residual, 2)),
        "IVA_Residual": float(round(iva_residual, 2)),
        "Total_Residual": float(round(total_residual, 2)),
        "Reembolso_Deposito": float(round(-monto_deposito, 2)),
        "Total_Final": float(round(total_final, 2)),
    }


# -------------------------------------------------
# FORMATO MILES SIN DECIMALES
# -------------------------------------------------
def formato_miles(v):
    try:
        num = int(round(float(v)))
        return f"{num:,}"
    except:
        return v


# -------------------------------------------------
# CONVERTIR WORD → PDF
# -------------------------------------------------
def convertir_pdf(path_word, output_dir):
    comando = [
        "soffice",
        "--headless",
        "--convert-to", "pdf",
        "--outdir", output_dir,
        path_word
    ]

    try:
        subprocess.run(comando, check=True)
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, f"Error al convertir a PDF: {e}")

    pdf_name = os.path.splitext(os.path.basename(path_word))[0] + ".pdf"
    pdf_path = os.path.join(output_dir, pdf_name)

    if not os.path.exists(pdf_path):
        raise HTTPException(500, "No se generó el archivo PDF")

    return pdf_name, pdf_path


# -------------------------------------------------
# ENDPOINT PRINCIPAL /cotizar
# -------------------------------------------------
@app.post("/cotizar")
def cotizar(data: CotizacionRequest, request: Request):

    nombre_upper = data.nombre.upper()
    activo_upper = data.nombre_activo.upper()

    # Folio único consistente para JSON + Word + PDF
    folio = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S")

    escenarios = [
        {"plazo": 24, "residual": 40},
        {"plazo": 36, "residual": 30},
        {"plazo": 48, "residual": 25},
        {"plazo": 60, "residual": 20},
    ]

    seguro_anual = calcular_seguro_anual(data.valor, data.seguro_anual)
    seguro_contado_flag = data.seguro_contado

    valores_para_doc = {
        "nombre": nombre_upper,
        "descripcion": activo_upper,
        "precio": formato_miles(data.valor),
        "fecha": datetime.now(TIMEZONE).strftime("%d/%m/%Y"),
        "folio": folio,
    }

    detalle = []

    # CALCULO DE LOS 3 PLAZOS
    for esc in escenarios:
        calc = calcular_pago_mensual(
            valor=data.valor,
            enganche=data.enganche,
            tasa_anual=data.tasa_anual,
            plazo_meses=esc["plazo"],
            valor_residual=esc["residual"],
            comision=data.comision,
            rentas_deposito=data.rentas_deposito,
            seguro_anual=seguro_anual,
            seguro_contado_flag=seguro_contado_flag,
            div_plan=DIV_PLAN,
            gestoria=GESTORIA,
        )

        detalle.append({"Plazo": esc["plazo"], **calc})

        p = esc["plazo"]

        # Variables Word por plazo
        valores_para_doc.update({
            f"enganche{p}": formato_miles(calc["Enganche"]),
            f"comision{p}": formato_miles(calc["Comision"]),
            f"deposito{p}": formato_miles(calc["Renta_en_Deposito"]),
            f"subinicial{p}": formato_miles(calc["Subtotal_Pago_Inicial"]),
            f"IVAinicial{p}": formato_miles(calc["IVA_Pago_Inicial"]),
            f"totalinicial{p}": formato_miles(calc["Total_Inicial"]),
            f"primermes{p}": formato_miles(calc["Primera_Mensualidad"]),
            f"segurocontado{p}": formato_miles(calc["Seguro_Contado"]),

            f"mensualidad{p}": formato_miles(calc["Renta_Mensual"]),
            f"IVAmes{p}": formato_miles(calc["IVA_Mensual"]),
            f"totalmes{p}": formato_miles(calc["Total_Mensual"]),

            # nuevas equivalencias
            f"rentaplan{p}": formato_miles(calc["Renta_Plan"]),
            f"subtotalmes{p}": formato_miles(calc["Subtotal_Mensual"]),
            f"IVAmensual{p}": formato_miles(calc["IVA_Mensual"]),
            f"totalmensual{p}": formato_miles(calc["Total_Mensual"]),

            f"residual{p}": formato_miles(calc["Residual"]),
            f"IVAresidual{p}": formato_miles(calc["IVA_Residual"]),
            f"totalresidual{p}": formato_miles(calc["Total_Residual"]),

            f"reembolso{p}": formato_miles(calc["Reembolso_Deposito"]),
            f"totalfinal{p}": formato_miles(calc["Total_Final"]),
        })

    # GENERAR DOCUMENTO
    with open(DB_PATH, "r") as f:
        data_db = json.load(f)

    plantilla = data_db["plantillas"][0]

    word_info = generar_documento_word_local(
        plantilla_id=plantilla["id"],
        valores=valores_para_doc,
        request=request
    )

    # RESPUESTA FINAL
    return {
        "Nombre": nombre_upper,
        "Activo": activo_upper,
        "Valor": round(data.valor, 2),
        "Detalle": detalle,
        "documentos": word_info
    }


# -------------------------------------------------
# GENERAR WORD + PDF
# -------------------------------------------------
def generar_documento_word_local(plantilla_id: str, valores: dict, request: Request):
    with open(DB_PATH, "r") as f:
        data = json.load(f)

    plantilla = next(p for p in data["plantillas"] if p["id"] == plantilla_id)
    plantilla_path = os.path.join(TEMPLATES_DIR, plantilla["nombre"])

    doc = Document(plantilla_path)

    # Reemplazo texto
    for p in doc.paragraphs:
        for run in p.runs:
            for var, val in valores.items():
                placeholder = f"{{{{{var}}}}}"
                if placeholder in run.text:
                    run.text = run.text.replace(placeholder, str(val))

    for t in doc.tables:
        for row in t.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    for run in p.runs:
                        for var, val in valores.items():
                            placeholder = f"{{{{{var}}}}}"
                            if placeholder in run.text:
                                run.text = run.text.replace(placeholder, str(val))

    folio = valores["folio"]
    word_name = f"FirstLease-Cotizacion-{folio}.docx"
    word_path = os.path.join(OUTPUT_DIR, word_name)
    doc.save(word_path)

    # URL base
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host")
    base = f"{proto}://{host}"

    url_word = f"{base}/download_word/{word_name}"

    # Intentar PDF
    pdf_name, pdf_path = convertir_pdf(word_path, OUTPUT_DIR)
    url_pdf = f"{base}/download_pdf/{pdf_name}"

    return {
        "archivo_word": word_name,
        "descargar_word": url_word,
        "folio": folio,
        "archivo_pdf": pdf_name,
        "descargar_pdf": url_pdf
    }


# -------------------------------------------------
# ENDPOINTS AUXILIARES
# -------------------------------------------------
@app.get("/download_word/{filename}")
def download_word(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "Archivo no encontrado")
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")


@app.get("/download_pdf/{filename}")
def download_pdf(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "Archivo no encontrado")
    return FileResponse(path, media_type="application/pdf")


@app.post("/upload_template")
async def upload_template(file: UploadFile = File(...)):
    if not file.filename.endswith(".docx"):
        raise HTTPException(400, "Solo se permiten archivos .docx")

    plantilla_id = str(uuid.uuid4())
    path = os.path.join(TEMPLATES_DIR, file.filename)

    with open(path, "wb") as f:
        f.write(await file.read())

    variables = extraer_variables(path)

    with open(DB_PATH, "r+") as f:
        data = json.load(f)
        data["plantillas"].append({
            "id": plantilla_id,
            "nombre": file.filename,
            "variables": variables
        })
        f.seek(0)
        f.truncate()
        json.dump(data, f, indent=4)

    return {
        "id": plantilla_id,
        "nombre_archivo": file.filename,
        "variables_detectadas": variables
    }


@app.get("/templates")
def list_templates():
    with open(DB_PATH, "r") as f:
        return json.load(f)["plantillas"]


@app.get("/")
def root():
    return {"mensaje": "API funcionando correctamente 🚀"}

# -------------------------------------------------
# HEALTH CHECK PARA RENDER
# -------------------------------------------------
@app.get("/")
def health_check():
    return {"status": "ok"}