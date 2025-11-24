import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI, UploadFile, File, HTTPException, Path, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal, getcontext
from docx import Document
from datetime import datetime
import os
import json
import uuid
import requests
from utils.parser import extraer_variables
import subprocess


# -------------------------------------------------
# Configuración inicial
# -------------------------------------------------
getcontext().prec = 28

app = FastAPI(
    title="API Unificada de Cotización y Documentos",
    description="Servicio único que calcula cotizaciones, administra plantillas y genera documentos Word.",
)

TEMPLATES_DIR = "templates"
OUTPUT_DIR = "outputs"
DB_PATH = "db.json"

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

if not os.path.exists(DB_PATH) or os.stat(DB_PATH).st_size == 0:
    with open(DB_PATH, "w") as f:
        json.dump({"plantillas": []}, f, indent=4)

#--------------------------------------------------
# Verificación y autocarga de plantilla Github
#--------------------------------------------------
GITHUB_RAW_URL = "https://github.com/FirstLeaseAgent/api-cotizaciones/raw/refs/heads/main/api-cotizaciones/templates/Plantilla_Cotizacion.docx"
TEMPLATE_NAME = "Plantilla_Cotizacion.docx"

def ensure_template_available():
    template_path = os.path.join(TEMPLATES_DIR, TEMPLATE_NAME)
    if not os.path.exists(template_path):
        print("🔄 Descargando plantilla desde GitHub...")
        resp = requests.get(GITHUB_RAW_URL)
        resp.raise_for_status()
        with open(template_path, "wb") as f:
            f.write(resp.content)
        print("✅ Plantilla descargada correctamente.")

    with open(DB_PATH, "r+") as db_file:
        data = json.load(db_file)
        if not data["plantillas"]:
            plantilla_id = str(uuid.uuid4())
            data["plantillas"].append({
                "id": plantilla_id,
                "nombre": TEMPLATE_NAME,
                "variables": []
            })
            db_file.seek(0)
            db_file.truncate()
            json.dump(data, db_file, indent=4)
            print("✅ Registro de plantilla agregado a db.json")

ensure_template_available()

# -------------------------------------------------
# MODELOS DE DATOS PARA COTIZACIÓN
# -------------------------------------------------
class CotizacionRequest(BaseModel):
    nombre: str
    nombre_activo: str
    valor: float
    enganche: Optional[float] = 10.0
    tasa_anual: Optional[float] = 30.0
    comision: Optional[float] = 3.0
    rentas_deposito: Optional[float] = 1.0

# -------------------------------------------------
# Cálculo financiero
# -------------------------------------------------
def calcular_pago_mensual(valor, enganche, tasa_anual, plazo_meses, valor_residual, comision, rentas_deposito):
    pv = Decimal(valor / 1.16) * Decimal(1 - enganche / 100)
    r = Decimal(tasa_anual) / Decimal(100 * 12)
    n = Decimal(plazo_meses)
    fv = Decimal(valor / 1.16 * valor_residual / 100)

    if r == 0:
        pago = -(pv - fv) / n
    else:
        pago = ((pv - fv * ((1 + r) ** (-n))) * r) / (1 - (1 + r) ** (-n))

    monto_comision = (Decimal(comision) / Decimal(100)) * pv
    monto_enganche = (Decimal(enganche) / Decimal(100)) * (Decimal(valor) / Decimal("1.16"))
    monto_deposito = Decimal(rentas_deposito) * pago * Decimal("1.16")
    monto_residual = (Decimal(valor) / Decimal("1.16")) * (Decimal(valor_residual) / Decimal(100))

    subtotal_inicial = monto_enganche + monto_comision + monto_deposito + pago
    iva_inicial = (monto_enganche + monto_comision + pago) * Decimal("0.16")
    total_inicial = subtotal_inicial + iva_inicial

    iva_renta = pago * Decimal("0.16")
    total_renta = pago * Decimal("1.16")

    iva_residual = monto_residual * Decimal("0.16")
    total_residual = monto_residual * Decimal("1.16")

    total_final = total_residual - monto_deposito

    return {
        "Enganche": float(round(monto_enganche, 2)),
        "Comision": float(round(monto_comision, 2)),
        "Renta_en_Deposito": float(round(monto_deposito, 2)),
        "Primera_Mensualidad": float(round(pago, 2)),
        "Subtotal_Pago_Inicial": float(round(subtotal_inicial, 2)),
        "IVA_Pago_Inicial": float(round(iva_inicial, 2)),
        "Total_Inicial": float(round(total_inicial, 2)),
        "Renta_Mensual": float(round(pago, 2)),
        "IVA_Renta_Mensual": float(round(iva_renta, 2)),
        "Total_Renta_Mensual": float(round(total_renta, 2)),
        "Residual": float(round(monto_residual, 2)),
        "IVA_Residual": float(round(iva_residual, 2)),
        "Total_Residual": float(round(total_residual, 2)),
        "Reembolso_Deposito": float(round(-monto_deposito, 2)),
        "Total_Final": float(round(total_final, 2)),
    }

# -------------------------------------------------
# Formato miles
# -------------------------------------------------
def formato_miles(valor):
    try:
        num = float(valor)
        return f"{num:,.2f}"
    except:
        return valor

#Nueva función para convertir a PDF
def convertir_pdf(word_path: str, output_dir: str):
    """
    Convierte un archivo .docx a .pdf usando LibreOffice (soffice).
    Retorna (nombre_archivo_pdf, ruta_pdf).
    """
    comando = [
        "soffice",
        "--headless",
        "--convert-to", "pdf",
        "--outdir", output_dir,
        word_path
    ]

    try:
        subprocess.run(comando, check=True)
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error al convertir a PDF: {e}")

    pdf_name = os.path.splitext(os.path.basename(word_path))[0] + ".pdf"
    pdf_path = os.path.join(output_dir, pdf_name)

    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=500, detail="No se generó el archivo PDF")

    return pdf_name, pdf_path


# -------------------------------------------------
# ENDPOINT /cotizar
# Calcula + genera documento Word con la primera plantilla disponible
# -------------------------------------------------
@app.post("/cotizar")
def cotizar(data: CotizacionRequest, request: Request):
    escenarios = [
        {"plazo": 24, "residual": 40},
        {"plazo": 36, "residual": 30},
        {"plazo": 48, "residual": 25},
    ]

    # Este dict se va a mandar a la plantilla Word
    valores_para_doc = {
        "nombre": data.nombre,
        "descripcion": data.nombre_activo,
        "precio": formato_miles(data.valor),
        "fecha": datetime.now().strftime("%d/%m/%Y"),
        "folio": datetime.now().strftime("%Y%m%d%H%M%S"),
    }

    detalle_resultado = []

    # Como ahora solo hay UN activo, usamos directamente data.*
    for e in escenarios:
        calculos = calcular_pago_mensual(
            valor=data.valor,
            enganche=data.enganche,
            tasa_anual=data.tasa_anual,
            plazo_meses=e["plazo"],
            valor_residual=e["residual"],
            comision=data.comision,
            rentas_deposito=data.rentas_deposito,
        )

        # Guardamos info para la respuesta JSON
        detalle_resultado.append({
            "Plazo": e["plazo"],
            **calculos
        })

        # ====== MUY IMPORTANTE ======
        # Mantener mismos nombres de variables que ya usaba tu plantilla
        # ============================
        if e["plazo"] == 24:
            valores_para_doc.update({
                # Pago inicial
                "enganche24": formato_miles(calculos["Enganche"]),
                "comision24": formato_miles(calculos["Comision"]),
                "deposito24": formato_miles(calculos["Renta_en_Deposito"]),
                "subinicial24": formato_miles(calculos["Subtotal_Pago_Inicial"]),
                "IVAinicial24": formato_miles(calculos["IVA_Pago_Inicial"]),
                "totalinicial24": formato_miles(calculos["Total_Inicial"]),

                # Mensualidad
                "mensualidad24": formato_miles(calculos["Renta_Mensual"]),
                "IVAmes24": formato_miles(calculos["IVA_Renta_Mensual"]),
                "totalmes24": formato_miles(calculos["Total_Renta_Mensual"]),

                # Residual
                "residual24": formato_miles(calculos["Residual"]),
                "IVAresidual24": formato_miles(calculos["IVA_Residual"]),
                "totalresidual24": formato_miles(calculos["Total_Residual"]),

                # Final
                "reembolso24": formato_miles(calculos["Reembolso_Deposito"]),
                "totalfinal24": formato_miles(calculos["Total_Final"]),
            })

        if e["plazo"] == 36:
            valores_para_doc.update({
                "enganche36": formato_miles(calculos["Enganche"]),
                "comision36": formato_miles(calculos["Comision"]),
                "deposito36": formato_miles(calculos["Renta_en_Deposito"]),
                "subinicial36": formato_miles(calculos["Subtotal_Pago_Inicial"]),
                "IVAinicial36": formato_miles(calculos["IVA_Pago_Inicial"]),
                "totalinicial36": formato_miles(calculos["Total_Inicial"]),

                "mensualidad36": formato_miles(calculos["Renta_Mensual"]),
                "IVAmes36": formato_miles(calculos["IVA_Renta_Mensual"]),
                "totalmes36": formato_miles(calculos["Total_Renta_Mensual"]),

                "residual36": formato_miles(calculos["Residual"]),
                "IVAresidual36": formato_miles(calculos["IVA_Residual"]),
                "totalresidual36": formato_miles(calculos["Total_Residual"]),

                "reembolso36": formato_miles(calculos["Reembolso_Deposito"]),
                "totalfinal36": formato_miles(calculos["Total_Final"]),
            })

        if e["plazo"] == 48:
            valores_para_doc.update({
                "enganche48": formato_miles(calculos["Enganche"]),
                "comision48": formato_miles(calculos["Comision"]),
                "deposito48": formato_miles(calculos["Renta_en_Deposito"]),
                "subinicial48": formato_miles(calculos["Subtotal_Pago_Inicial"]),
                "IVAinicial48": formato_miles(calculos["IVA_Pago_Inicial"]),
                "totalinicial48": formato_miles(calculos["Total_Inicial"]),

                "mensualidad48": formato_miles(calculos["Renta_Mensual"]),
                "IVAmes48": formato_miles(calculos["IVA_Renta_Mensual"]),
                "totalmes48": formato_miles(calculos["Total_Renta_Mensual"]),

                "residual48": formato_miles(calculos["Residual"]),
                "IVAresidual48": formato_miles(calculos["IVA_Residual"]),
                "totalresidual48": formato_miles(calculos["Total_Residual"]),

                "reembolso48": formato_miles(calculos["Reembolso_Deposito"]),
                "totalfinal48": formato_miles(calculos["Total_Final"]),
            })

    # ==============================
    # Generar documento Word
    # ==============================

    with open(DB_PATH, "r") as db_file:
        db_data = json.load(db_file)

    plantilla = None
    if db_data["plantillas"]:
        plantilla = db_data["plantillas"][0]  # Usa la primera plantilla cargada

    if plantilla:
        word_info = generar_documento_word_local(
            plantilla_id=plantilla["id"],
            valores=valores_para_doc,
            request=request
        )
        documentos = word_info
    else:
        documentos = {
            "aviso": "No hay plantilla registrada en el sistema todavía. Usa /upload_template primero."
        }

    # 🔚 Respuesta final con estructura nueva
    return {
        "Nombre": data.nombre,
        "Activo": data.nombre_activo,
        "Valor": round(data.valor, 2),  # valor original de entrada
        "Detalle": detalle_resultado,
        "documentos": documentos
    }

# -------------------------------------------------
# Generar documento Word
# -------------------------------------------------
def generar_documento_word_local(plantilla_id: str, valores: dict, request: Request):
    with open(DB_PATH, "r") as f:
        data = json.load(f)
    plantilla = next((p for p in data["plantillas"] if p["id"] == plantilla_id), None)
    if not plantilla:
        raise HTTPException(status_code=404, detail="Plantilla no encontrada")

    plantilla_path = os.path.join(TEMPLATES_DIR, plantilla["nombre"])
    if not os.path.exists(plantilla_path):
        GITHUB_RAW_URL = "https://raw.githubusercontent.com/FirstLeaseAgent/api-cotizaciones/main/api-cotizaciones/templates/Plantilla_Cotizacion.docx"
        resp = requests.get(GITHUB_RAW_URL)
        resp.raise_for_status()
        with open(plantilla_path, "wb") as f:
            f.write(resp.content)

    doc = Document(plantilla_path)
    for p in doc.paragraphs:
        for run in p.runs:
            for var, valor in valores.items():
                placeholder = f"{{{{{var}}}}}"
                if placeholder in run.text:
                    run.text = run.text.replace(placeholder, str(valor))

    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    for run in p.runs:
                        for var, valor in valores.items():
                            placeholder = f"{{{{{var}}}}}"
                            if placeholder in run.text:
                                run.text = run.text.replace(placeholder, str(valor))

    folio = valores.get("folio", datetime.now().strftime("%Y%m%d_%H%M%S"))
    word_name = f"cotizacion_{folio}.docx"
    word_path = os.path.join(OUTPUT_DIR, word_name)
    doc.save(word_path)

    # URL base del servicio
    base_url = str(request.base_url).rstrip("/")

    # Link para descargar el Word (igual que antes)
    download_word = f"{base_url}/download_word/{word_name}"

    # Intentar generar el PDF
    pdf_name = None
    download_pdf = None
    try:
        pdf_name, pdf_path = convertir_pdf(word_path, OUTPUT_DIR)
        download_pdf = f"{base_url}/download_pdf/{pdf_name}"
    except HTTPException:
        # Si falla la conversión a PDF, no rompemos todo; solo no regresamos el PDF
        download_pdf = None

    resultado = {
        "archivo_word": word_name,
        "descargar_word": download_word,
        "folio": folio
    }

    if download_pdf:
        resultado["archivo_pdf"] = pdf_name
        resultado["descargar_pdf"] = download_pdf

    return resultado


# -------------------------------------------------
# ENDPOINTS plantillas
# -------------------------------------------------
@app.post("/upload_template")
async def upload_template(file: UploadFile = File(...)):
    if not file.filename.endswith(".docx"):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos .docx")

    plantilla_id = str(uuid.uuid4())
    file_path = os.path.join(TEMPLATES_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    variables = extraer_variables(file_path)
    with open(DB_PATH, "r+") as db_file:
        data = json.load(db_file)
        data["plantillas"].append({
            "id": plantilla_id,
            "nombre": file.filename,
            "variables": variables
        })
        db_file.seek(0)
        db_file.truncate()
        json.dump(data, db_file, indent=4)

    return {"id": plantilla_id, "nombre_archivo": file.filename, "variables_detectadas": variables}

@app.get("/templates")
def list_templates():
    with open(DB_PATH, "r") as f:
        data = json.load(f)
    return data["plantillas"]

@app.get("/download_word/{filename}")
def download_word(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=filename
    )

@app.get("/download_pdf/{filename}")
def download_pdf(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(
        file_path,
        media_type="application/pdf",
        filename=filename
    )

@app.get("/")
def root():
    return {"mensaje": "API Unificada funcionando correctamente"}