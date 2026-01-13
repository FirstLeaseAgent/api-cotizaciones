import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


import re
import unicodedata
from typing import Any, Dict
import psycopg2
from psycopg2.extras import execute_values, Json

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

from datetime import datetime, timedelta



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
# SYNC CARTERA (Postgres en Render)
# -------------------------------------------------
CARTERA_DATABASE_URL = os.getenv("CARTERA_DATABASE_URL")
CARTERA_SYNC_API_KEY = os.getenv("CARTERA_SYNC_API_KEY")

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
# /sync/historico  (Power Automate -> Postgres)
# -------------------------------------------------

_num_re = re.compile(r"[^0-9\.\-]")

_xcode_re = re.compile(r"_x([0-9A-Fa-f]{4})_", re.IGNORECASE)

def _decode_xcodes(s: str) -> str:
    # Convierte _x002e_ -> '.'  y _x0020_ -> ' '
    return _xcode_re.sub(lambda m: chr(int(m.group(1), 16)), s)

def _norm_key(k: str) -> str:
    """
    Normaliza keys provenientes de Excel / Power Automate:
    - Decodifica _xNNNN_ (ej. _x002e_)
    - Quita acentos
    - Unifica separadores
    """
    if k is None:
        return ""
    s = str(k).strip()

    # 🔹 PASO CLAVE: decodificar formato Excel
    s = _decode_xcodes(s)

    # normalización estándar
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Busca un campo por varios nombres posibles, usando normalización.
    """
    if not row:
        return None
    norm_map = {_norm_key(k): v for k, v in row.items()}
    for n in names:
        v = norm_map.get(_norm_key(n))
        if v is not None and str(v).strip() != "":
            return v
    return None

def _to_number(v: Any):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = _num_re.sub("", s)  # quita $, comas, espacios, etc.
    if s in ("", ".", "-", "-.", ".-"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def _to_int(v: Any):
    n = _to_number(v)
    if n is None:
        return None
    try:
        return int(round(n))
    except Exception:
        return None

def _excel_serial_to_date(serial: float):
    # Excel: día 1 = 1899-12-31 (con bug 1900), estándar práctico: base 1899-12-30
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=serial)).date()

def _to_date_str(v: Any):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None

    # Si viene número de Excel (ej. 43467)
    try:
        n = float(s)
        if n > 20000:  # umbral razonable para fechas modernas
            return _excel_serial_to_date(n).isoformat()
    except Exception:
        pass

    # Si viene ISO o texto, lo devolvemos tal cual
    return s

class SyncTables(BaseModel):
    cartera: List[Dict[str, Any]] = []
    activos: List[Dict[str, Any]] = []

class SyncPayload(BaseModel):
    tables: SyncTables

def _get_cartera_conn():
    if not CARTERA_DATABASE_URL:
        raise HTTPException(status_code=500, detail="Missing CARTERA_DATABASE_URL")
    return psycopg2.connect(CARTERA_DATABASE_URL)

@app.post("/sync/historico")
def sync_historico(payload: SyncPayload, x_api_key: Optional[str] = Header(None)):
    # Seguridad: clave independiente a API_ADMIN_KEY
    if not CARTERA_SYNC_API_KEY:
        raise HTTPException(status_code=500, detail="Missing CARTERA_SYNC_API_KEY")
    if x_api_key != CARTERA_SYNC_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    cartera_rows = payload.tables.cartera or []
    activos_rows = payload.tables.activos or []

    # -------- UPSERT: cartera_historica --------
    cartera_values = []
    for r in cartera_rows:
        contrato = _get_any(r, "NO. CONTRATO", "NO CONTRATO", "NO_CONTRATO")
        if not contrato:
            continue
        contrato = str(contrato).strip()

        cartera_values.append((
            contrato,
            (_get_any(r, "CLIENTE") or None),
            (_get_any(r, "TIPO DE ACTIVO") or None),
            _to_int(_get_any(r, "PLAZO DEL ARRENDAMIENTO")),
            _to_date_str(_get_any(r, "FECHA DE INICIO")),
            _to_date_str(_get_any(r, "FECHA DE VENCIMIENTO")),
            _to_number(_get_any(r, "TASA DE INTERES", "TASA DE INTERÉS")),
            _to_number(_get_any(r, "SALDO INSOLUTO INICIO MES")),
            _to_number(_get_any(r, "PAGOS HISTORICOS C/IVA", "PAGOS HISTÓRICOS C/IVA")),
            Json(r),
        ))

    # -------- UPSERT: activos_historico --------
    activos_values = []
    for r in activos_rows:
        ident = _get_any(r, "IDENTIFICADOR")
        contrato = _get_any(r, "NO. CONTRATO", "NO CONTRATO", "NO_CONTRATO")
        if not ident or not contrato:
            continue
        ident = str(ident).strip()
        contrato = str(contrato).strip()

        activos_values.append((
            ident,
            contrato,
            (_get_any(r, "CONTRATO INTERNO") or None),
            (_get_any(r, "CLIENTE") or None),
            (_get_any(r, "TIPO DE ACTIVO") or None),
            (_get_any(r, "DESCRIPCION", "DESCRIPCIÓN") or None),
            (_get_any(r, "NÚMERO DE SERIE", "NUMERO DE SERIE") or None),
            (_get_any(r, "NÚMERO DE MOTOR", "NUMERO DE MOTOR") or None),
            (_to_date_str(_get_any(r, "FECHA DE INICIO"))),
            (_to_date_str(_get_any(r, "FECHA DE VENCIMIENTO"))),
            (_get_any(r, "ASEGURADORA") or None),
            (_get_any(r, "POLIZA", "PÓLIZA") or None),
            (_to_date_str(_get_any(r, "INICIO VIGENCIA POLIZA", "INICIO VIGENCIA PÓLIZA"))),
            (_to_date_str(_get_any(r, "FIN VIGENCIA POLIZA", "FIN VIGENCIA PÓLIZA"))),
            Json(r),
        ))

    conn = _get_cartera_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if cartera_values:
                    execute_values(cur, """
                        insert into public.cartera_historica
                          (no_contrato, cliente, tipo_de_activo, plazo_del_arrendamiento,
                           fecha_de_inicio, fecha_de_vencimiento,
                           tasa_de_interes, saldo_insoluto_inicio_mes, pagos_historicos_c_iva,
                           raw)
                        values %s
                        on conflict (no_contrato) do update set
                          cliente = excluded.cliente,
                          tipo_de_activo = excluded.tipo_de_activo,
                          plazo_del_arrendamiento = excluded.plazo_del_arrendamiento,
                          fecha_de_inicio = excluded.fecha_de_inicio,
                          fecha_de_vencimiento = excluded.fecha_de_vencimiento,
                          tasa_de_interes = excluded.tasa_de_interes,
                          saldo_insoluto_inicio_mes = excluded.saldo_insoluto_inicio_mes,
                          pagos_historicos_c_iva = excluded.pagos_historicos_c_iva,
                          raw = excluded.raw,
                          updated_at = now();
                    """, cartera_values)

                if activos_values:
                    execute_values(cur, """
                        insert into public.activos_historico
                          (identificador, no_contrato, contrato_interno, cliente, tipo_de_activo,
                           descripcion, numero_de_serie, numero_de_motor,
                           fecha_de_inicio, fecha_de_vencimiento,
                           aseguradora, poliza, inicio_vigencia_poliza, fin_vigencia_poliza,
                           raw)
                        values %s
                        on conflict (identificador) do update set
                          no_contrato = excluded.no_contrato,
                          contrato_interno = excluded.contrato_interno,
                          cliente = excluded.cliente,
                          tipo_de_activo = excluded.tipo_de_activo,
                          descripcion = excluded.descripcion,
                          numero_de_serie = excluded.numero_de_serie,
                          numero_de_motor = excluded.numero_de_motor,
                          fecha_de_inicio = excluded.fecha_de_inicio,
                          fecha_de_vencimiento = excluded.fecha_de_vencimiento,
                          aseguradora = excluded.aseguradora,
                          poliza = excluded.poliza,
                          inicio_vigencia_poliza = excluded.inicio_vigencia_poliza,
                          fin_vigencia_poliza = excluded.fin_vigencia_poliza,
                          raw = excluded.raw,
                          updated_at = now();
                    """, activos_values)

        return {
            "cartera_received": len(cartera_rows),
            "cartera_upserted": len(cartera_values),
            "activos_received": len(activos_rows),
            "activos_upserted": len(activos_values),
        }
    finally:
        conn.close()

# ==============================
# Cartera Query (READ ONLY)
# ==============================
CARTERA_READ_API_KEY = os.getenv("CARTERA_READ_API_KEY")

def _require_read_key(x_api_key: Optional[str]):
    if not CARTERA_READ_API_KEY:
        raise HTTPException(status_code=500, detail="Missing CARTERA_READ_API_KEY")
    if x_api_key != CARTERA_READ_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _clamp_limit(n: Optional[int], default: int = 200, max_n: int = 2000) -> int:
    if n is None:
        return default
    try:
        n = int(n)
    except Exception:
        return default
    return max(1, min(max_n, n))

def _rows_to_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

class QueryInclude(BaseModel):
    contratos: bool = False
    activos: bool = False

class CarteraQuery(BaseModel):
    scope: str = "cliente"  # cliente | contrato | cartera | activo
    q: Optional[str] = None
    no_contrato: Optional[str] = None
    metrics: List[str] = []
    include: QueryInclude = QueryInclude()
    limit: Optional[int] = 200

@app.post("/cartera/query")
def cartera_query(payload: CarteraQuery, x_api_key: Optional[str] = Header(None)):
    _require_read_key(x_api_key)

    scope = (payload.scope or "cliente").strip().lower()
    q = (payload.q or "").strip()
    no_contrato = (payload.no_contrato or "").strip()
    limit = _clamp_limit(payload.limit)

    # Filtros base
    where_cartera = "1=1"
    params_cartera = []

    # Filtros para activos (se inicializan siempre)
    where_activos = None
    params_activos = None

    if scope == "cliente":
        if not q:
            raise HTTPException(status_code=400, detail="scope=cliente requires q (cliente)")
        where_cartera = "c.cliente ilike %s"
        params_cartera = [f"%{q}%"]

    elif scope == "contrato":
        if not no_contrato:
            no_contrato = q
        if not no_contrato:
            raise HTTPException(status_code=400, detail="scope=contrato requires no_contrato (or q)")

        no_contrato_clean = no_contrato.strip()

        # Si son 4 dígitos, interpretarlo como el 3er bloque del contrato: XXXXX-XXXXX-0166-XXX
        if len(no_contrato_clean) == 4 and no_contrato_clean.isdigit():
            where_cartera = "c.no_contrato ILIKE %s"
            params_cartera = [f"%-{no_contrato_clean}-%"]
        else:
            where_cartera = "c.no_contrato = %s"
            params_cartera = [no_contrato_clean]

        # 🔎 PRECHECK: validar que el contrato exista
        with _get_cartera_conn() as conn_check:
            with conn_check.cursor() as cur_check:
                cur_check.execute(
                    f"SELECT 1 FROM public.cartera_historica c WHERE {where_cartera} LIMIT 1;",
                    params_cartera
                )
                if cur_check.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Contrato no encontrado con el identificador proporcionado"
                    )
            
    elif scope == "cartera":
        where_cartera = "1=1"
        params_cartera = []

    elif scope == "activo":
        if not q:
            raise HTTPException(status_code=400, detail="scope=activo requires q")

        q_clean = q.strip()

        # 1) Si parece contrato completo (tiene 3+ guiones), match exacto por no_contrato
        if q_clean.count("-") >= 3:
            where_activos = "a.no_contrato = %s"
            params_activos = [q_clean]

        # 2) Si son 4 dígitos, match por 3er bloque (ignora ceros a la izquierda)
        elif len(q_clean) == 4 and q_clean.isdigit():
            where_activos = "ltrim(split_part(a.no_contrato, '-', 3), '0') = ltrim(%s, '0')"
            params_activos = [q_clean]

        # 3) Búsqueda amplia (texto libre)
        else:
            like = f"%{q_clean}%"
            where_activos = """
            (
                a.no_contrato ILIKE %s OR
                coalesce(a.contrato_interno,'') ILIKE %s OR
                a.cliente ILIKE %s OR
                a.tipo_de_activo ILIKE %s OR
                a.descripcion ILIKE %s OR
                a.numero_de_serie ILIKE %s OR
                a.numero_de_motor ILIKE %s OR
                a.aseguradora ILIKE %s OR
                a.poliza ILIKE %s
            )
            """
            params_activos = [like, like, like, like, like, like, like, like, like]

        cur.execute(f"""
            select
                a.identificador,
                a.no_contrato,
                a.cliente,
                a.tipo_de_activo,
                a.descripcion,
                a.numero_de_serie,
                a.numero_de_motor,
                a.aseguradora,
                a.poliza,
                a.inicio_vigencia_poliza,
                a.fin_vigencia_poliza
            from public.activos_historico a
            where {where_activos}
            order by a.no_contrato asc, a.identificador asc
            limit %s;
        """, params_activos + [limit])

        activos = cur.fetchall()
        cols = [d[0] for d in cur.description]
        out_rows["activos"] = [dict(zip(cols, r)) for r in activos]
    else:
        raise HTTPException(status_code=400, detail="Invalid scope")

    # Helpers SQL para extraer numericos desde raw (tolerante a comas/$)
    raw_num = lambda key: f"""nullif(regexp_replace(coalesce(c.raw->>'{key}',''), '[^0-9\\.\\-]', '', 'g'),'')::numeric"""
    raw_int = lambda key: f"""nullif(regexp_replace(coalesce(c.raw->>'{key}',''), '[^0-9\\-]', '', 'g'),'')::int"""

    metrics_out = {}
    conn = _get_cartera_conn()
    try:
        with conn, conn.cursor() as cur:

            # ---------- MÉTRICAS ----------
            for m in payload.metrics:
                m = (m or "").strip().lower()

                if m == "conteo_contratos":
                    cur.execute(f"select count(*) as conteo_contratos from public.cartera_historica c where {where_cartera};", params_cartera)
                    metrics_out["conteo_contratos"] = int(cur.fetchone()[0] or 0)

                elif m == "vigentes_terminados":
                    cur.execute(f"""
                        select
                          count(*) filter (where coalesce(c.saldo_insoluto_inicio_mes,0) > 0) as vigentes,
                          count(*) filter (where coalesce(c.saldo_insoluto_inicio_mes,0) = 0) as terminados
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    r = cur.fetchone()
                    metrics_out["vigentes_terminados"] = {"vigentes": int(r[0] or 0), "terminados": int(r[1] or 0)}

                elif m == "suma_rentas_vigentes":
                    # Flujo mensual (S/IVA) *solo vigentes* (saldo insoluto > 0)
                    cur.execute(f"""
                        select coalesce(sum({raw_num('FLUJO MENSUAL S/IVA')}),0) as suma_rentas_vigentes
                        from public.cartera_historica c
                        where {where_cartera}
                          and coalesce(c.saldo_insoluto_inicio_mes,0) > 0;
                    """, params_cartera)
                    metrics_out["suma_rentas_vigentes"] = float(cur.fetchone()[0] or 0)

                elif m == "suma_cartera":
                    # Cartera = suma de flujos_futuros_inicio_mes (columna normalizada)
                    cur.execute(f"""
                        select coalesce(sum(coalesce(c.flujos_futuros_inicio_mes, 0)), 0) as suma_cartera
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    metrics_out["suma_cartera"] = float(cur.fetchone()[0] or 0)

                elif m == "suma_pagos":
                    cur.execute(f"""
                        select coalesce(sum(coalesce(c.pagos_historicos_c_iva,0)),0) as suma_pagos
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    metrics_out["suma_pagos"] = float(cur.fetchone()[0] or 0)

                elif m == "saldo_insoluto":
                    cur.execute(f"""
                        select coalesce(sum(coalesce(c.saldo_insoluto_inicio_mes,0)),0) as saldo_insoluto
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    metrics_out["saldo_insoluto"] = float(cur.fetchone()[0] or 0)

                elif m == "rentas_por_devengar":
                    # Si es contrato, devolvemos el valor del contrato; si es cliente/cartera, suma
                    if scope == "contrato":
                        cur.execute(f"""
                            select c.no_contrato,
                                   {raw_int('NO. DE RENTAS POR DEVENGAR')} as rentas_por_devengar
                            from public.cartera_historica c
                            where {where_cartera}
                            limit 1;
                        """, params_cartera)
                        row = cur.fetchone()
                        metrics_out["rentas_por_devengar"] = (int(row[1]) if row and row[1] is not None else None)
                    else:
                        cur.execute(f"""
                            select coalesce(sum(coalesce({raw_int('NO. DE RENTAS POR DEVENGAR')},0)),0) as rentas_por_devengar
                            from public.cartera_historica c
                            where {where_cartera};
                        """, params_cartera)
                        metrics_out["rentas_por_devengar"] = int(cur.fetchone()[0] or 0)

                elif m == "monto_financiado":
                    cur.execute(f"""
                        select coalesce(sum({raw_num('MONTO A FINANCIAR S/IVA')}),0) as monto_financiado
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    metrics_out["monto_financiado"] = float(cur.fetchone()[0] or 0)

                elif m == "valor_residual":
                    cur.execute(f"""
                        select coalesce(sum({raw_num('VALOR RESIDUAL S/IVA')}),0) as valor_residual
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    metrics_out["valor_residual"] = float(cur.fetchone()[0] or 0)

                elif m == "condiciones":
                    # Solo tiene sentido por contrato (regresamos un objeto)
                    if scope != "contrato":
                        metrics_out["condiciones"] = "scope=contrato required"
                    else:
                        cur.execute(f"""
                            select
                              c.no_contrato,
                              c.plazo_del_arrendamiento,
                              c.tasa_de_interes,
                              {raw_num('APORTACION EXTRAORDINARIA S/IVA')} as aportacion_extraordinaria_s_iva,
                              {raw_num('COMISION POR APERTURA S/IVA')} as comision_por_apertura_s_iva,
                              {raw_num('DEPOSITO EN GARANTIA S/IVA')} as deposito_en_garantia_s_iva,
                              {raw_num('MONTO A FINANCIAR S/IVA')} as monto_a_financiar_s_iva,
                              {raw_num('VALOR FACTURA ACTIVO S/IVA')} as valor_factura_activo_s_iva,
                              {raw_num('VALOR RESIDUAL S/IVA')} as valor_residual_s_iva
                            from public.cartera_historica c
                            where {where_cartera}
                            limit 1;
                        """, params_cartera)
                        r = cur.fetchone()
                        if not r:
                            metrics_out["condiciones"] = None
                        else:
                            metrics_out["condiciones"] = {
                                "no_contrato": r[0],
                                "plazo": r[1],
                                "tasa_interes": float(r[2]) if r[2] is not None else None,
                                "aportacion_extraordinaria_s_iva": float(r[3]) if r[3] is not None else None,
                                "comision_por_apertura_s_iva": float(r[4]) if r[4] is not None else None,
                                "deposito_en_garantia_s_iva": float(r[5]) if r[5] is not None else None,
                                "monto_a_financiar_s_iva": float(r[6]) if r[6] is not None else None,
                                "valor_factura_activo_s_iva": float(r[7]) if r[7] is not None else None,
                                "valor_residual_s_iva": float(r[8]) if r[8] is not None else None,
                            }

                elif m == "fechas":
                    if scope == "contrato":
                        cur.execute(f"""
                            select c.no_contrato, c.fecha_de_inicio, c.fecha_de_vencimiento
                            from public.cartera_historica c
                            where {where_cartera}
                            limit 1;
                        """, params_cartera)
                        r = cur.fetchone()
                        metrics_out["fechas"] = {
                            "no_contrato": r[0] if r else None,
                            "fecha_inicio": (r[1].isoformat() if r and r[1] else None),
                            "fecha_vencimiento": (r[2].isoformat() if r and r[2] else None),
                        }
                    else:
                        cur.execute(f"""
                            select min(c.fecha_de_inicio) as min_inicio,
                                   max(c.fecha_de_inicio) as max_inicio,
                                   min(c.fecha_de_vencimiento) as min_venc,
                                   max(c.fecha_de_vencimiento) as max_venc
                            from public.cartera_historica c
                            where {where_cartera};
                        """, params_cartera)
                        r = cur.fetchone()
                        metrics_out["fechas"] = {
                            "min_inicio": r[0].isoformat() if r and r[0] else None,
                            "max_inicio": r[1].isoformat() if r and r[1] else None,
                            "min_vencimiento": r[2].isoformat() if r and r[2] else None,
                            "max_vencimiento": r[3].isoformat() if r and r[3] else None,
                        }

                elif m == "desde_cuando_cliente":
                    # min(fecha_inicio) por cliente (o filtro actual)
                    cur.execute(f"""
                        select min(c.fecha_de_inicio) as desde_cuando
                        from public.cartera_historica c
                        where {where_cartera};
                    """, params_cartera)
                    r = cur.fetchone()
                    metrics_out["desde_cuando_cliente"] = (r[0].isoformat() if r and r[0] else None)

                else:
                    # ignoramos métricas desconocidas para no romper al agente
                    metrics_out[m] = "unknown_metric"

            # ---------- LISTADOS (opcionales) ----------
            out_rows = {"contratos": [], "activos": []}

            if payload.include and payload.include.contratos:
                cur.execute(f"""
                    select
                      c.no_contrato, c.cliente, c.tipo_de_activo,
                      c.plazo_del_arrendamiento, c.fecha_de_inicio, c.fecha_de_vencimiento,
                      c.saldo_insoluto_inicio_mes, c.pagos_historicos_c_iva,
                      c.raw->>'FLUJO MENSUAL S/IVA' as flujo_mensual_s_iva,
                      c.raw->>'FLUJOS FUTUROS INICIO MES' as flujos_futuros_inicio_mes
                    from public.cartera_historica c
                    where {where_cartera}
                    order by c.updated_at desc
                    limit %s;
                """, params_cartera + [limit])
                out_rows["contratos"] = _rows_to_dicts(cur)

            # ---------- LISTADOS (opcionales) ----------
            out_rows = {"contratos": [], "activos": []}

            if payload.include and payload.include.contratos:
                cur.execute(f"""
                    select
                    c.no_contrato, c.cliente, c.tipo_de_activo,
                    c.plazo_del_arrendamiento, c.fecha_de_inicio, c.fecha_de_vencimiento,
                    c.saldo_insoluto_inicio_mes, c.pagos_historicos_c_iva,
                    c.raw->>'FLUJO MENSUAL S/IVA' as flujo_mensual_s_iva,
                    c.raw->>'FLUJOS FUTUROS INICIO MES' as flujos_futuros_inicio_mes
                    from public.cartera_historica c
                    where {where_cartera}
                    order by c.updated_at desc
                    limit %s;
                """, params_cartera + [limit])
                out_rows["contratos"] = _rows_to_dicts(cur)

            # Mostrar activos cuando:
            # - el scope sea "activo" (aunque no venga include.activos)
            # - o cuando venga include.activos=true
            want_activos = (scope == "activo") or (payload.include and payload.include.activos)

            if want_activos:
                if scope == "activo":
                    # usar el filtro preparado arriba
                    cur.execute(f"""
                        select
                            a.identificador, a.no_contrato, a.cliente, a.tipo_de_activo, a.descripcion,
                            a.numero_de_serie, a.numero_de_motor, a.aseguradora, a.poliza,
                            a.inicio_vigencia_poliza, a.fin_vigencia_poliza
                        from public.activos_historico a
                        where {where_activos}
                        order by a.no_contrato asc, a.identificador asc
                        limit %s;
                    """, params_activos + [limit])
                    out_rows["activos"] = _rows_to_dicts(cur)

                elif scope == "contrato":
                    # soportar contrato completo o 4 dígitos
                    nc = (no_contrato or q or "").strip()
                    if not nc:
                        raise HTTPException(status_code=400, detail="include.activos with scope=contrato requires no_contrato (or q)")

                    if len(nc) == 4 and nc.isdigit():
                        cur.execute("""
                            select identificador, no_contrato, cliente, tipo_de_activo, descripcion,
                                numero_de_serie, numero_de_motor, aseguradora, poliza,
                                inicio_vigencia_poliza, fin_vigencia_poliza
                            from public.activos_historico
                            where ltrim(split_part(no_contrato,'-',3),'0') = ltrim(%s,'0')
                            order by identificador
                            limit %s;
                        """, (nc, limit))
                    else:
                        cur.execute("""
                            select identificador, no_contrato, cliente, tipo_de_activo, descripcion,
                                numero_de_serie, numero_de_motor, aseguradora, poliza,
                                inicio_vigencia_poliza, fin_vigencia_poliza
                            from public.activos_historico
                            where no_contrato = %s
                            order by identificador
                            limit %s;
                        """, (nc, limit))

                    out_rows["activos"] = _rows_to_dicts(cur)

                else:
                    # otros scopes: solo si hay q, búsqueda simple por texto (opcional)
                    if q:
                        cur.execute("""
                            select identificador, no_contrato, cliente, tipo_de_activo, descripcion,
                                numero_de_serie, numero_de_motor, aseguradora, poliza,
                                inicio_vigencia_poliza, fin_vigencia_poliza
                            from public.activos_historico
                            where descripcion ilike %s
                            or numero_de_serie ilike %s
                            or numero_de_motor ilike %s
                            or poliza ilike %s
                            order by updated_at desc
                            limit %s;
                        """, (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", limit))
                        out_rows["activos"] = _rows_to_dicts(cur)

        return {
            "filters_applied": {"scope": scope, "q": q or None, "no_contrato": no_contrato or None},
            "metrics": metrics_out,
            "rows": out_rows
        }
    finally:
        conn.close()


# -------------------------------------------------
# HEALTH CHECK
# -------------------------------------------------
@app.get("/")
def health_check():
    return {"status": "ok"}
