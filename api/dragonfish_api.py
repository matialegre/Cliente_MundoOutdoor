"""Wrapper sencillo para la API Dragonfish"""
from __future__ import annotations

import requests
import json
import time
from datetime import datetime, timezone, timedelta

from utils import config
from utils.logger import get_logger
from .base import APIError

log = get_logger(__name__)

BASE_URL = "http://190.211.201.217:8009/api.Dragonfish"


def _headers() -> dict:
    return {
        "accept": "application/json",
        "Authorization": config.DRAGONFISH_TOKEN,
        "IdCliente": getattr(config, "DRAGONFISH_IDCLIENTE", "PRUEBA-WEB"),
        "Content-Type": "application/json",
        "BaseDeDatos": "DEPOSITO",
    }


def _fecha_dragonfish() -> str:
    zona = timezone(timedelta(hours=-3))
    ms = int(datetime.now(zona).timestamp() * 1000)
    return f"/Date({ms}-0300)/"


def _hora_dragonfish() -> str:
    return datetime.now().strftime("%H:%M:%S")


def send_stock_movement(pedido_id: int | str, codigo_barra: str, cantidad: int, datos_articulo: dict) -> tuple[bool, str]:
    url = f"{BASE_URL}/Movimientodestock/"
    body = {
        "OrigenDestino": "MELI",
        "Tipo": 2,
        "Motivo": "API",
        "vendedor": "API",
        "Remito": "-",
        "CompAfec": [],
        "Fecha": _fecha_dragonfish(),
        "Observacion": f"MELI API {pedido_id}",
        "MovimientoDetalle": [
            {
                "Articulo": datos_articulo.get("CODIGO_BARRA", codigo_barra),
                "ArticuloDetalle": datos_articulo.get("ARTDES", ""),
                "Color": datos_articulo.get("CODIGO_COLOR"),
                "Talle": datos_articulo.get("CODIGO_TALLE"),
                "Cantidad": cantidad,
                "NroItem": 1,
            }
        ],
        "InformacionAdicional": {
            "FechaAltaFW": _fecha_dragonfish(),
            "HoraAltaFW": _hora_dragonfish(),
            "EstadoTransferencia": "PENDIENTE",
            "BaseDeDatosAltaFW": "MELI",
            "BaseDeDatosModificacionFW": "MELI",
            "SerieAltaFW": "901224",
            "SerieModificacionFW": "901224",
            "UsuarioAltaFW": "API",
            "UsuarioModificacionFW": "API",
        },
    }

    log.debug("Dragonfish payload: %s", json.dumps(body, ensure_ascii=False)[:500])
    retries = getattr(config, "DRAGONFISH_MOVE_RETRIES", 10)
    delay_s = getattr(config, "DRAGONFISH_MOVE_DELAY_S", 3)

    for intento in range(1, retries + 1):
        log.info("Dragonfish movimiento INTENTO %s/%s", intento, retries)
        try:
            resp = requests.post(url, headers=_headers(), data=json.dumps(body), timeout=10)
            last_status = resp.status_code
            if 200 <= resp.status_code < 300:
                log.info("Dragonfish movement OK – status %s", resp.status_code)
                return True, "OK"
            log.warning("Dragonfish movimiento FAIL intento %s/%s – status %s – cuerpo: %s", intento, retries, resp.status_code, resp.text[:300])
        except requests.RequestException as e:
            log.error("Dragonfish movimiento EXC intento %s/%s: %s", intento, retries, e)
        if intento < retries:
            time.sleep(delay_s)
    log.error("Dragonfish movimiento agotó %s intentos sin éxito", retries)
    return False, f"FAIL ({last_status})"
