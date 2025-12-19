import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Header
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal, getcontext
from docx import Document
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import uuid
import requests
import subprocess
from utils.parser import extraer_variables
from dotenv import load_dotenv

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
TIMEZONE = ZoneInfo("America/Mexico_City")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

load_dotenv()
# API KEY para actualizar variables
API_ADMIN_KEY = os.getenv("API_ADMIN_KEY")
if not API_ADMIN_KEY:
    raise RuntimeError(
        "❌ ERROR: Debes definir la variable de entorno API_ADMIN_KEY (en .env o en el entorno del servidor)."
    )

# -------------------------------------------------
# VARIABLES / PARÁMETROS DE NEGOCIO (por defecto)
# -------------------------------------------------
DEFAULT_VARIABLES = {
    # Porcentajes y parámetros generales
    "tasa_anual_default": 27.0,
    "enganche_default": 10.0,
    "rentas_deposito_default": 0.0,
    "comision_default": 3.0,
    "div_plan": 48.0,      # % del subtotal que se va a "Renta_Plan"
    "gestoria": 2000.0,    # costo fijo de gestoría (con IVA en lógica original)

    # Residuales por plazo (si no vienen en el request)
    "residuales_default": [
        {"plazo": 24, "residual": 40},
        {"plazo": 36, "residual": 30},
        {"plazo": 48, "residual": 25},
        {"plazo": 60, "residual": 20},
    ],

    # Seguro por monto (se evalúa contra VALOR CON IVA)
    "seguro_por_monto": [
        {"max_valor_con_iva": 500000, "porcentaje": 0.04},
        {"max_valor_con_iva": 750000, "porcentaje": 0.035},
        {"max_valor_con_iva": 1000000, "porcentaje": 0.03},
        {"max_valor_con_iva": 1500000, "porcentaje": 0.0275},
        {"max_valor_con_iva": 5000000, "porcentaje": 0.025},
        {"max_valor_con_iva": 9999999999, "porcentaje": 0.025},
    ],

    # Localizador
    "localizador_inicial_default": 0.0,
    "localizador_anual_default": 0.0,
}

VARIABLES = DEFAULT_VARIABLES.copy()

# -------------------------------------------------
# PLANTILLA PRINCIPAL (carga automática desde GitHub)
# -------------------------------------------------
GITHUB_RAW_URL = (
    "https://github.com/FirstLeaseAgent/api-cotizaciones/raw/refs/heads/main/"
    "api-cotizaciones/templates/Plantilla_Cotizacion.docx"
)
TEMPLATE_NAME = "Plantilla_Cotizacion.docx"


def ensure_db_and_variables():
    """
    Asegura que db.json exista y tenga llaves:
      - plantillas: []
      - variables: {...}
    """
    global VARIABLES

    if not os.path.exists(DB_PATH) or os.stat(DB_PATH).st_size == 0:
        with open(DB_PATH, "w") as f:
            json.dump({"plantillas": [], "variables": DEFAULT_VARIABLES}, f, indent=4)
    else:
        with open(DB_PATH, "r+") as f:
            data = json.load(f)
            changed = False

            if "plantillas" not in data:
                data["plantillas"] = []
                changed = True

            if "variables" not in data:
                data["variables"] = DEFAULT_VARIABLES
                changed = True

            # Asegurar todas las llaves de DEFAULT_VARIABLES
            for k, v in DEFAULT_VARIABLES.items():
                if k not in data["variables"]:
                    data["variables"][k] = v
                    changed = True

            if changed:
                f.seek(0)
                f.truncate()
                json.dump(data, f, indent=4)

            VARIABLES = data["variables"]


def ensure_template_available():
    ensure_db_and_variables()

    template_path = os.path.join(TEMPLATES_DIR, TEMPLATE_NAME)

    # Descarga si no existe la plantilla
    if not os.path.exists(template_path):
        print("🔄 Descargando plantilla desde GitHub…")
        resp = requests.get(GITHUB_RAW_URL)
        resp.raise_for_status()
        with open(template_path, "wb") as f:
            f.write(resp.content)
        print("✅ Plantilla descargada.")

    # Registrar plantilla en DB si no hay ninguna
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
# MODELOS Pydantic
# -------------------------------------------------
class ResidualItem(BaseModel):
    plazo: int
    residual: float


class CotizacionRequest(BaseModel):
    nombre: str
    nombre_activo: str
    valor: float

    # Si son None, se toma DEFAULT_VARIABLES
    enganche: Optional[float] = None
    tasa_anual: Optional[float] = None
    comision: Optional[float] = None
    rentas_deposito: Optional[float] = None

    # Seguro:
    #   None o -1 -> calcular por tabla (seguro_por_monto)
    #   0 -> gratis
    #   >0 -> monto anual sin IVA
    seguro_anual: Optional[float] = -1
    seguro_contado: bool = False  # True = contado / False = financiado

    # Nuevos campos
    accesorios: Optional[float] = 0.0              # con IVA
    localizador_inicial: Optional[float] = None     # con IVA
    localizador_anual: Optional[float] = None       # con IVA

    # Residuales opcionales por cotización
    residuales: Optional[List[ResidualItem]] = None

# ------------------------------------------
# ACTUALIZAR EJEMPLO DE SWAGGER DINÁMICAMENTE
# ------------------------------------------
def refresh_cotizar_example():
    """
    Refresca el ejemplo mostrado en Swagger, usando los valores
    actuales de VARIABLES.
    """
    ejemplo = {
        "nombre": "Cliente Ejemplo",
        "nombre_activo": "Camioneta Tiguan 2025",
        "valor": 0,
        "enganche": VARIABLES["enganche_default"],
        "tasa_anual": VARIABLES["tasa_anual_default"],
        "comision": VARIABLES["comision_default"],
        "rentas_deposito": VARIABLES["rentas_deposito_default"],
        "seguro_anual": -1,
        "seguro_contado": False,
        "accesorios": 0,
        "localizador_inicial": VARIABLES["localizador_inicial_default"],
        "localizador_anual": VARIABLES["localizador_anual_default"],
        "residuales": VARIABLES["residuales_default"],
    }

    CotizacionRequest.model_config["json_schema_extra"] = {
        "example": ejemplo
    }


# Ejecutar tras definir la clase
refresh_cotizar_example()

class SeguroRango(BaseModel):
    max_valor_con_iva: float
    porcentaje: float


class ResidualConfig(BaseModel):
    plazo: int
    residual: float


class VariablesUpdate(BaseModel):
    tasa_anual_default: Optional[float] = None
    enganche_default: Optional[float] = None
    rentas_deposito_default: Optional[float] = None
    comision_default: Optional[float] = None
    div_plan: Optional[float] = None
    gestoria: Optional[float] = None
    localizador_inicial_default: Optional[float] = None
    localizador_anual_default: Optional[float] = None
    residuales_default: Optional[List[ResidualConfig]] = None
    seguro_por_monto: Optional[List[SeguroRango]] = None


# -------------------------------------------------
# SEGURO ANUAL
# -------------------------------------------------
def calcular_seguro_anual(valor_con_iva: float, entrada: Optional[float]) -> Decimal:
    """
    Calcula el seguro anual SIN IVA.

    - Si entrada es None o -1 → usa tabla VARIABLES["seguro_por_monto"] (basada en VALOR CON IVA).
    - Si entrada es 0 → seguro gratuito.
    - Si entrada > 0 → se usa el valor proporcionado (ya SIN IVA).
    """
    # Caso: usar tabla
    if entrada is None or entrada == -1:
        v_con_iva = Decimal(str(valor_con_iva))
        valor_sin_iva = v_con_iva / Decimal("1.16")

        rangos = VARIABLES.get("seguro_por_monto", DEFAULT_VARIABLES["seguro_por_monto"])
        pct = Decimal("0.025")
        for rango in rangos:
            max_v = Decimal(str(rango["max_valor_con_iva"]))
            if v_con_iva <= max_v:
                pct = Decimal(str(rango["porcentaje"]))
                break

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
    div_plan_pct,
    gestoria,
    accesorios_con_iva=0.0,
    localizador_inicial_con_iva=0.0,
    localizador_anual_con_iva=0.0,
):
    """
    Incluye:
    - renta principal (con residual)
    - seguro (contado o financiado)
    - gestoría
    - accesorios (sin residual, precio con IVA)
    - localizador inicial (sin residual, financiado a n)
    - localizador anual (sin residual, financiado a 12 meses)
    """

    valor_sin_iva = Decimal(valor) / Decimal("1.16")
    r = Decimal(tasa_anual) / Decimal(1200)
    n = Decimal(plazo_meses)

    # Valor presente del activo (sin IVA)
    pv = valor_sin_iva * (1 - Decimal(enganche) / 100)
    fv = valor_sin_iva * (Decimal(valor_residual) / 100)

    # ------------------------- PAGO DE RENTA PRINCIPAL -------------------------
    if r == 0:
        pago = -(pv - fv) / n
    else:
        pago = ((pv - fv * ((1 + r) ** (-n))) * r) / (1 - (1 + r) ** (-n))

    # ------------------------- SEGURO -------------------------
    pv_seguro = seguro_anual  # ya viene sin IVA

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
    pv_gest = Decimal(str(gestoria)) / Decimal("1.16")  # lo tratamos como valor con IVA
    if r == 0:
        pago_gestoria = -pv_gest / n
    else:
        pago_gestoria = (pv_gest * r) / (1 - (1 + r) ** (-n))

    # ------------------------- ACCESORIOS -------------------------
    # accesorios_con_iva se financia sin residual, valor presente SIN IVA
    if accesorios_con_iva and accesorios_con_iva > 0:
        pv_acc_sin_iva = Decimal(str(accesorios_con_iva)) / Decimal("1.16")
        if r == 0:
            pago_accesorios = -pv_acc_sin_iva / n
        else:
            pago_accesorios = (pv_acc_sin_iva * r) / (1 - (1 + r) ** (-n))
    else:
        pago_accesorios = Decimal("0")

    # ------------------------- LOCALIZADOR -------------------------
    # Inicial: n meses, sin residual
    if localizador_inicial_con_iva and localizador_inicial_con_iva > 0:
        pv_loc_ini = Decimal(str(localizador_inicial_con_iva)) / Decimal("1.16")
        if r == 0:
            pago_loc_ini = -pv_loc_ini / n
        else:
            pago_loc_ini = (pv_loc_ini * r) / (1 - (1 + r) ** (-n))
    else:
        pago_loc_ini = Decimal("0")

    # Anual: 12 meses, sin residual
    if localizador_anual_con_iva and localizador_anual_con_iva > 0:
        pv_loc_anual = Decimal(str(localizador_anual_con_iva)) / Decimal("1.16")
        n_loc = Decimal(12)
        if r == 0:
            pago_loc_anual = -pv_loc_anual / n_loc
        else:
            pago_loc_anual = (pv_loc_anual * r) / (1 - (1 + r) ** (-n_loc))
    else:
        pago_loc_anual = Decimal("0")

    pago_localizador_total = pago_loc_ini + pago_loc_anual

    # ------------------------- PAGO TOTAL MENSUAL -------------------------
    subtotal_mensual = (
        pago
        + pago_seguro
        + pago_gestoria
        + pago_accesorios
        + pago_localizador_total
    )

    # El depósito se calcula sobre la renta total (como en lógica original)
    total_mensual_con_iva = subtotal_mensual * Decimal("1.16")
    monto_deposito = Decimal(rentas_deposito) * total_mensual_con_iva
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
    div = Decimal(div_plan_pct) / Decimal(100)

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
    except Exception:
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
    # Cargar variables actuales
    div_plan = VARIABLES.get("div_plan", DEFAULT_VARIABLES["div_plan"])
    gestoria = VARIABLES.get("gestoria", DEFAULT_VARIABLES["gestoria"])

    # Resolver defaults dinámicos
    enganche = data.enganche if data.enganche is not None else VARIABLES["enganche_default"]
    tasa_anual = data.tasa_anual if data.tasa_anual is not None else VARIABLES["tasa_anual_default"]
    comision = data.comision if data.comision is not None else VARIABLES["comision_default"]
    rentas_deposito = data.rentas_deposito if data.rentas_deposito is not None else VARIABLES["rentas_deposito_default"]

    accesorios = data.accesorios or 0.0
    loc_ini = data.localizador_inicial if data.localizador_inicial is not None else VARIABLES["localizador_inicial_default"]
    loc_anual = data.localizador_anual if data.localizador_anual is not None else VARIABLES["localizador_anual_default"]

    nombre_upper = data.nombre.upper()
    activo_upper = data.nombre_activo.upper()

    # Folio único consistente para JSON + Word + PDF
    folio = datetime.now(TIMEZONE).strftime("%Y%m%d%H%M%S")

    # Escenarios de plazos y residuales
    default_residuales = VARIABLES.get("residuales_default", DEFAULT_VARIABLES["residuales_default"])

    if data.residuales and len(data.residuales) > 0:
        # Convertimos lista → dict para poder reemplazar solo los plazos enviados
        enviados = {r.plazo: r.residual for r in data.residuales}

        # Completamos usando defaults cuando el usuario no manda algo
        escenarios = []
        for item in default_residuales:
            plazo = item["plazo"]
            residual = enviados.get(plazo, item["residual"])
            escenarios.append({"plazo": plazo, "residual": residual})
    else:
        # Si no envían nada, usar todos los residuales defaults
        escenarios = default_residuales

    # Seguro anual (sin IVA)
    seguro_anual = calcular_seguro_anual(data.valor, data.seguro_anual)
    seguro_contado_flag = data.seguro_contado

    valores_para_doc = {
        "nombre": nombre_upper,
        "descripcion": activo_upper,
        "precio": formato_miles(data.valor),
        "accesorios": formato_miles(accesorios),
        "ptotal": formato_miles(data.valor + accesorios),
        "fecha": datetime.now(TIMEZONE).strftime("%d/%m/%Y"),
        "folio": folio,
    }

    detalle = []

    # CÁLCULO DE CADA PLAZO
    for esc in escenarios:
        calc = calcular_pago_mensual(
            valor=data.valor,
            enganche=enganche,
            tasa_anual=tasa_anual,
            plazo_meses=esc["plazo"],
            valor_residual=esc["residual"],
            comision=comision,
            rentas_deposito=rentas_deposito,
            seguro_anual=seguro_anual,
            seguro_contado_flag=seguro_contado_flag,
            div_plan_pct=div_plan,
            gestoria=gestoria,
            accesorios_con_iva=accesorios,
            localizador_inicial_con_iva=loc_ini,
            localizador_anual_con_iva=loc_anual,
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

            # Equivalencias
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
        "documentos": word_info,
        "parametros": {
            "accesorios": accesorios,
            "comision": comision,
            "enganche": enganche,
            "localizador_anual": loc_anual,
            "localizador_inicial": loc_ini,
            "rentas_deposito": rentas_deposito,
            "residuales": escenarios,   # ← Los plazos+residual realmente utilizados
            "seguro_anual": float(seguro_anual),
            "seguro_contado": seguro_contado_flag,
            "tasa_anual": tasa_anual,
        }
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

    # Reemplazo texto en párrafos
    for p in doc.paragraphs:
        for run in p.runs:
            for var, val in valores.items():
                placeholder = f"{{{{{var}}}}}"
                if placeholder in run.text:
                    run.text = run.text.replace(placeholder, str(val))

    # Reemplazo texto en tablas
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


# -------------------------------------------------
# ENDPOINTS DE VARIABLES (con x-api-key)
# -------------------------------------------------
@app.get("/variables")
def get_variables(x_api_key: str = Header(None)):
    if x_api_key != API_ADMIN_KEY:
        raise HTTPException(status_code=401, detail="No autorizado")
    return VARIABLES


@app.put("/variables")
def update_variables(payload: VariablesUpdate, x_api_key: str = Header(None)):
    global VARIABLES

    if x_api_key != API_ADMIN_KEY:
        raise HTTPException(status_code=401, detail="No autorizado")

    # Convertir Pydantic → dict
    data_dict = payload.model_dump(exclude_unset=True)

    # -------------------------------------------
    # 1) Residuales
    # -------------------------------------------
    if "residuales_default" in data_dict and data_dict["residuales_default"] is not None:
        data_dict["residuales_default"] = [
            {"plazo": r["plazo"], "residual": r["residual"]}
            for r in data_dict["residuales_default"]
        ]

    # -------------------------------------------
    # 2) Seguro por monto
    # -------------------------------------------
    if "seguro_por_monto" in data_dict and data_dict["seguro_por_monto"] is not None:
        data_dict["seguro_por_monto"] = [
            {
                "max_valor_con_iva": s["max_valor_con_iva"],
                "porcentaje": s["porcentaje"]
            }
            for s in data_dict["seguro_por_monto"]
        ]

    # -------------------------------------------
    # 3) Otros campos simples → se actualizan directo
    # -------------------------------------------
    for k, v in data_dict.items():
        VARIABLES[k] = v

    # -------------------------------------------
    # 4) Persistir en db.json
    # -------------------------------------------
    with open(DB_PATH, "r+") as f:
        data = json.load(f)

        if "variables" not in data:
            data["variables"] = {}

        for k, v in VARIABLES.items():
            data["variables"][k] = v

        f.seek(0)
        f.truncate()
        json.dump(data, f, indent=4)

    refresh_cotizar_example()

    return {"status": "ok", "variables": VARIABLES}

# -------------------------------------------------
# HEALTH CHECK
# -------------------------------------------------
@app.get("/")
def health_check():
    return {"status": "ok"}
