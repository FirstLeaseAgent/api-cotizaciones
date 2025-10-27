from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from decimal import Decimal, getcontext
from datetime import datetime
import requests

# ===========================================================
# CONFIGURACIÓN BASE
# ===========================================================
getcontext().prec = 28
app = FastAPI(title="API de Cotización de Arrendamiento")

TEMPLATE_MANAGER_URL = "https://template-manager-3mt1.onrender.com/generate_word"
TEMPLATE_LIST_URL = "https://template-manager-3mt1.onrender.com/templates"
PLANTILLA_ID = "2ab9df51-ac07-4c8c-b23a-e430ca1b4b90"  # ID de tu plantilla en Template Manager


# ===========================================================
# MODELOS DE DATOS
# ===========================================================
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


# ===========================================================
# FUNCIÓN DE CÁLCULO
# ===========================================================
def calcular_pago_mensual(valor, enganche, tasa_anual, plazo_meses, valor_residual, comision, rentas_deposito):
    pv = Decimal(valor / 1.16) * Decimal(1 - enganche / 100)
    r = Decimal(tasa_anual) / Decimal(100 * 12)
    n = Decimal(plazo_meses)
    fv = Decimal(valor / 1.16 * valor_residual / 100)

    if r == 0:
        pago = -(pv - fv) / n
    else:
        pago = ((pv - fv * ((1 + r) ** (-n))) * r) / (1 - (1 + r) ** (-n))

    monto_comision = Decimal(comision) / Decimal(100) * pv
    monto_enganche = Decimal(enganche) / Decimal(100) * Decimal(valor) / Decimal('1.16')
    monto_deposito = Decimal(rentas_deposito) * pago * Decimal('1.16')
    monto_residual = (Decimal(valor) / Decimal('1.16')) * Decimal(valor_residual) / Decimal(100)

    subtotal_inicial = monto_enganche + monto_comision + monto_deposito + pago
    iva_inicial = (monto_enganche + monto_comision + pago) * Decimal('0.16')
    total_inicial = subtotal_inicial + iva_inicial

    iva_renta = pago * Decimal('0.16')
    total_renta = pago * Decimal('1.16')

    iva_residual = monto_residual * Decimal('0.16')
    total_residual = monto_residual * Decimal('1.16')

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
        "Total_Final": float(round(total_final, 2))
    }


# ===========================================================
# FUNCIÓN: Traducir JSON de Cotización a formato Plantilla
# ===========================================================
def traducir_json_a_plantilla(json_cotizacion):
    """
    Traduce el JSON de salida de api-cotizaciones al formato esperado por la plantilla de Template Manager.
    Valida que todas las variables existan antes de enviar la solicitud.
    """
    try:
        cotizacion = json_cotizacion["Cotizaciones"][0]
        detalles = cotizacion["Detalle"]
        activo = cotizacion.get("Activo", "")
        nombre = json_cotizacion.get("Nombre", "")
        precio = cotizacion["Detalle"][0].get("Residual", 0) * 1.16  # o json_cotizacion.get("Precio", 0)

        valores = {
            "nombre": nombre,
            "descripcion": activo,
            "precio": precio,
            "fecha": datetime.now().strftime("%d/%m/%Y"),
            "folio": f"COT-{datetime.now().strftime('%Y%m%d%H%M')}",
            "Be_Hibirido": "",
            "Be_Gasolina": ""
        }

        for item in detalles:
            plazo = str(item.get("Plazo"))
            valores[f"enganche{plazo}"] = item.get("Enganche", 0)
            valores[f"comision{plazo}"] = item.get("Comision", 0)
            valores[f"deposito{plazo}"] = item.get("Renta_en_Deposito", 0)
            valores[f"mensualidad{plazo}"] = item.get("Renta_Mensual", 0)
            valores[f"IVAmes{plazo}"] = item.get("IVA_Renta_Mensual", 0)
            valores[f"totalmes{plazo}"] = item.get("Total_Renta_Mensual", 0)
            valores[f"subinicial{plazo}"] = item.get("Subtotal_Pago_Inicial", 0)
            valores[f"IVAinicial{plazo}"] = item.get("IVA_Pago_Inicial", 0)
            valores[f"totalinicial{plazo}"] = item.get("Total_Inicial", 0)
            valores[f"totalresidual{plazo}"] = item.get("Residual", 0)
            valores[f"IVAresidual{plazo}"] = item.get("IVA_Residual", 0)
            valores[f"residual{plazo}"] = item.get("Total_Residual", 0)
            valores[f"reembolso{plazo}"] = item.get("Reembolso_Deposito", 0)
            valores[f"totalfinal{plazo}"] = item.get("Total_Final", 0)

        # --- Validación de variables ---
        r = requests.get(TEMPLATE_LIST_URL)
        if r.status_code == 200:
            data = r.json()
            plantilla = next((p for p in data if p["id"] == PLANTILLA_ID), None)
            if plantilla:
                vars_plantilla = plantilla["variables"]
                faltantes = [v for v in vars_plantilla if v not in valores]
                if faltantes:
                    valores["variables_faltantes"] = faltantes
        else:
            valores["variables_faltantes"] = ["Error al consultar Template Manager"]

        return valores

    except Exception as e:
        raise Exception(f"Error al traducir JSON: {str(e)}")


# ===========================================================
# FUNCIÓN: Generar PDF/Word en Template Manager
# ===========================================================
# URL base de Template Manager
TEMPLATE_MANAGER_URL = "https://template-manager-3mt1.onrender.com/generate_word"

# ID fijo de la plantilla que usarás (puedes obtenerlo desde /templates)
PLANTILLA_ID = "2ab9df51-ac07-4c8c-b23a-e430ca1b4b90"

def generar_cotizacion_pdf(valores_plantilla):
    """
    Envía los valores calculados a Template Manager para generar
    un documento Word con la plantilla especificada.
    """
    payload = {
        "plantilla_id": PLANTILLA_ID,
        "valores": valores_plantilla
    }

    try:
        response = requests.post(TEMPLATE_MANAGER_URL, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()

        return {
            "mensaje": "Documento generado correctamente",
            "descargar_word": data.get("descargar_word"),
            "archivo_word": data.get("archivo_word"),
            "faltantes": valores_plantilla.get("variables_faltantes", [])
        }

    except requests.exceptions.RequestException as e:
        return {
            "error": f"Error al conectar con Template Manager: {str(e)}"
        }


# ===========================================================
# ENDPOINT PRINCIPAL
# ===========================================================
@app.post("/cotizar")
def cotizar(data: CotizacionRequest):
    escenarios = [
        {"plazo": 24, "residual": 40},
        {"plazo": 36, "residual": 30},
        {"plazo": 48, "residual": 25},
    ]

    resultado = {"Nombre": data.nombre, "Cotizaciones": []}

    for activo in data.activos:
        cotizaciones_activo = []
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

            cotizaciones_activo.append({
                "Plazo": e["plazo"],
                **calculos
            })

        resultado["Cotizaciones"].append({
            "Activo": activo.nombre_activo,
            "Detalle": cotizaciones_activo
        })

    # --- Generar PDF con Template Manager ---
    try:
        valores_plantilla = traducir_json_a_plantilla(resultado)
        pdf_data = generar_cotizacion_pdf(valores_plantilla)
        resultado["documentos"] = pdf_data
    except Exception as e:
        resultado["documentos"] = {"error": str(e)}

    return resultado


@app.get("/")
def root():
    return {"mensaje": "API de Cotizaciones de Arrendamiento funcionando correctamente"}