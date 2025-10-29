import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI, UploadFile, File, HTTPException, Path, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from decimal import Decimal, getcontext
from docx import Document
from datetime import datetime
import os
import json
import uuid
import requests  # opcional si después hablas con otros servicios internos
from utils.parser import extraer_variables

# -------------------------------------------------
# Configuración inicial
# -------------------------------------------------
getcontext().prec = 28

app = FastAPI(
    title="API Unificada de Cotización y Documentos",
    description="Servicio único que calcula cotizaciones, administra plantillas y genera documentos Word.",
)

# Rutas de almacenamiento
TEMPLATES_DIR = "templates"
OUTPUT_DIR = "outputs"
DB_PATH = "db.json"

# Crear carpetas necesarias
os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Crear base de datos si no existe o está vacía
if not os.path.exists(DB_PATH) or os.stat(DB_PATH).st_size == 0:
    with open(DB_PATH, "w") as f:
        json.dump({"plantillas": []}, f, indent=4)
        
#--------------------------------------------------
# Verificación y autocarga de plantilla Github
#--------------------------------------------------

GITHUB_RAW_URL = "https://github.com/FirstLeaseAgent/api-cotizaciones/raw/refs/heads/main/api-cotizaciones/templates/Plantilla_Cotizacion.docx"
TEMPLATE_NAME = "Plantilla_Cotizacion.docx"

def ensure_template_available():
    # 1. Descarga plantilla si no existe localmente
    template_path = os.path.join(TEMPLATES_DIR, TEMPLATE_NAME)
    if not os.path.exists(template_path):
        print("🔄 Descargando plantilla desde GitHub...")
        resp = requests.get(GITHUB_RAW_URL)
        resp.raise_for_status()
        with open(template_path, "wb") as f:
            f.write(resp.content)
        print("✅ Plantilla descargada correctamente.")

    # 2. Asegura que exista un registro en db.json
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

# Ejecutar al inicio
ensure_template_available()



# Inicializamos la DB si no existe
if not os.path.exists(DB_PATH) or os.stat(DB_PATH).st_size == 0:
    with open(DB_PATH, "w") as f:
        json.dump({"plantillas": []}, f, indent=4)

# -------------------------------------------------
# MODELOS DE DATOS PARA COTIZACIÓN
# -------------------------------------------------
class Activo(BaseModel):
    nombre_activo: str
    valor: float
    enganche: Optional[float] = 10.0
    tasa_anual: Optional[float] = 30.0
    comision: Optional[float] = 3.0
    rentas_deposito: Optional[float] = 1.0

class CotizacionRequest(BaseModel):
    nombre: str
    activos: List[Activo]

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

    total_final = total_residual - monto_deposito  # depósito se reembolsa

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
# Función auxiliar: formato miles (para documentos)
# -------------------------------------------------
def formato_miles(valor):
    try:
        num = float(valor)
        return f"{num:,.2f}"
    except:
        return valor

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

    resultado = {"Nombre": data.nombre, "Cotizaciones": []}

    # Este dict se va a mandar a la plantilla Word
    valores_para_doc = {
        "nombre": data.nombre,
        "descripcion": "",
        "precio": "",
        "fecha": datetime.now().strftime("%d/%m/%Y"),
        "folio": datetime.now().strftime("%Y%m%d%H%M%S"),
        # Los valores por plazo (24/36/48) se llenan más abajo:
    }

    for activo in data.activos:
        cotizaciones_activo = []

        # Guardamos el nombre y precio del activo para la cotización
        valores_para_doc["descripcion"] = activo.nombre_activo
        valores_para_doc["precio"] = formato_miles(activo.valor)

        for e in escenarios:
            calculos = calcular_pago_mensual(
                valor=activo.valor,
                enganche=activo.enganche,
                tasa_anual=activo.tasa_anual,
                plazo_meses=e["plazo"],
                valor_residual=e["residual"],
                comision=activo.comision,
                rentas_deposito=activo.rentas_deposito,
            )

            # Guardamos info cruda por si la quieres en la respuesta JSON
            cotizaciones_activo.append({
                "Plazo": e["plazo"],
                **calculos
            })

            # ====== MUY IMPORTANTE ======
            # Aquí estamos DENTRO del for e in escenarios.
            # La indentación de este bloque es crítica.
            # ============================
            plazo = str(e["plazo"])
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

        # Guardamos la lista de escenarios calculados de este activo
        resultado["Cotizaciones"].append({
            "Activo": activo.nombre_activo,
            "Detalle": cotizaciones_activo
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

    return {
        "Nombre": data.nombre,
        "Cotizaciones": resultado["Cotizaciones"],
        "documentos": documentos
    }

# -------------------------------------------------
# Lógica interna para generar documento Word
# (antes estaba en Template Manager)
# -------------------------------------------------
def generar_documento_word_local(plantilla_id: str, valores: dict, request: Request):
    import requests

    # 1. Cargar DB
    with open(DB_PATH, "r") as f:
        data = json.load(f)

    # 2. Buscar plantilla por ID
    plantilla = next((p for p in data["plantillas"] if p["id"] == plantilla_id), None)
    if not plantilla:
        raise HTTPException(status_code=404, detail="Plantilla no encontrada")

    plantilla_path = os.path.join(TEMPLATES_DIR, plantilla["nombre"])

    # 3. Si la plantilla no existe localmente, descargarla desde GitHub
    if not os.path.exists(plantilla_path):
        GITHUB_RAW_URL = "https://raw.githubusercontent.com/FirstLeaseAgent/api-cotizaciones/main/api-cotizaciones/templates/Plantilla_Cotizacion.docx"
        try:
            response = requests.get(GITHUB_RAW_URL, timeout=30)
            response.raise_for_status()
            with open(plantilla_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Plantilla descargada desde GitHub: {plantilla['nombre']}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al descargar plantilla desde GitHub: {e}")

    # 4. Cargar Word
    doc = Document(plantilla_path)

    # 5. Reemplazo de variables (manteniendo formato)
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

    # 6. Guardar archivo final en /outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    word_name = f"cotizacion_{timestamp}.docx"
    word_path = os.path.join(OUTPUT_DIR, word_name)
    doc.save(word_path)

    # 7. Construir URL de descarga
    base_url = str(request.base_url).rstrip("/")
    download_url = f"{base_url}/download_word/{word_name}"

    return {
        "archivo_word": word_name,
        "descargar_word": download_url,
    }

# -------------------------------------------------
# ENDPOINTS de gestión de plantillas (antes: Template Manager)
# -------------------------------------------------

@app.post("/upload_template")
async def upload_template(file: UploadFile = File(...)):
    """
    Sube una plantilla .docx, extrae variables {{ }} y la registra en db.json
    """
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

    return {
        "id": plantilla_id,
        "nombre_archivo": file.filename,
        "variables_detectadas": variables
    }

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

@app.get("/")
def root():
    return {"mensaje": "API Unificada funcionando correctamente"}