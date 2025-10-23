from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from decimal import Decimal, getcontext

# Configuramos precisión decimal
getcontext().prec = 28

app = FastAPI(title="API de Cotización de Arrendamiento")

# --- MODELOS DE DATOS ---
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

# --- FUNCIÓN DE CÁLCULO ---
def calcular_pago_mensual(valor, enganche, tasa_anual, plazo_meses, valor_residual, comision, rentas_deposito):
    pv = Decimal(valor / 1.16) * Decimal(1 - enganche / 100)
    r = Decimal(tasa_anual) / Decimal(100 * 12)
    n = Decimal(plazo_meses)
    fv = Decimal(valor / 1.16 * valor_residual / 100)

    if r == 0:
        pago = -(pv - fv) / n
    else:
        pago = ((pv - fv * ((1 + r) ** (-n))) * r) / (1 - (1 + r) ** (-n))

    # Convertimos float a Decimal antes de operar
    monto_comision = Decimal(comision) / Decimal(100) * pv
    monto_enganche = Decimal(enganche) / Decimal(100) * Decimal(valor)/ Decimal('1.16')
    monto_deposito = Decimal(rentas_deposito) * pago * Decimal('1.16')
    monto_residual = (Decimal(valor) / Decimal('1.16')) * Decimal(valor_residual) / Decimal(100)

    # Pago inicial
    subtotal_inicial = monto_enganche + monto_comision + monto_deposito + pago
    iva_inicial = (monto_enganche + monto_comision + pago) * Decimal('0.16')
    total_inicial = subtotal_inicial + iva_inicial

    # Renta mensual
    iva_renta = pago * Decimal('0.16')
    total_renta = pago * Decimal('1.16')

    # Residual
    iva_residual = monto_residual * Decimal('0.16')
    total_residual = monto_residual * Decimal('1.16')

    # Reembolso depósito
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

# --- ENDPOINT PRINCIPAL ---
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

    return resultado

@app.get("/")
def root():
    return {"mensaje": "API de Cotizaciones de Arrendamiento funcionando correctamente"}
