"""
conexion.py — Autenticación y conexión con XOMS/ROFEX
======================================================
Contiene: autenticar, configurar_pyrofex, obtener_instrumentos.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyRofex
import requests

from config import MARKET_DATA_ENTRIES, SUBSCRIPTION_BATCH, log
from utils import validate_config


def iniciar_conexion(user: str, password: str, broker: str) -> str:
    """
    Realiza el handshake REST con XOMS para obtener el token,
    inyecta las URLs personalizadas e inicializa pyRofex.
    Retorna el token por si se necesita para llamadas REST manuales.
    Lanza RuntimeError si la autenticación falla.
    """
    broker_lower = broker.lower()

    # ── 1. Solicitar el Token (Handshake) ──
    auth_url = f"https://api.{broker_lower}.xoms.com.ar/auth/getToken"
    log.info("Solicitando token REST a: %s", auth_url)

    headers = {
        "X-Username": user,
        "X-Password": password,
        "Accept": "application/json",
    }
    response = requests.post(auth_url, headers=headers, timeout=10)

    if response.status_code == 200 and "X-Auth-Token" in response.headers:
        token = response.headers["X-Auth-Token"]
        log.info("✅ Token obtenido correctamente.")

        # ── 2. Configurar e Inicializar pyRofex ──
        data_url = f"https://api.{broker_lower}.xoms.com.ar/"
        ws_url = f"wss://api.{broker_lower}.xoms.com.ar/"

        # pylint: disable=protected-access
        env = pyRofex.Environment.LIVE
        pyRofex._set_environment_parameter("url", data_url, env)  # type: ignore
        pyRofex._set_environment_parameter("ws", ws_url, env)    # type: ignore

        pyRofex.initialize(
            user=user,
            password=password,
            active_token=token,
            account=user,
            environment=pyRofex.Environment.LIVE,
        )
        log.info("✅ pyRofex inicializado en entorno LIVE (%s).", broker)

        return token

    # Si la petición falló, salta directo aquí:
    raise RuntimeError(
        f"Error en handshake REST. Status: {response.status_code} | Body: {response.text[:200]}"
    )


def obtener_instrumentos() -> list[str]:
    """
    Descarga el catálogo completo del broker y retorna la lista de símbolos.
    También guarda una copia CSV diaria como caché.
    """
    log.info("Descargando catálogo completo de instrumentos...")
    res = pyRofex.get_all_instruments()

    if not res or "instruments" not in res:
        raise RuntimeError("No se pudo obtener la lista de instrumentos del broker.")

    symbols: list[str] = [
        inst["instrumentId"]["symbol"] for inst in res["instruments"]
    ]

    # Guardar caché CSV diaria
    fecha = datetime.now().strftime("%Y-%m-%d")
    csv_path = Path(f"instrumentos_{fecha}.csv")
    pd.DataFrame(symbols, columns=["symbol"]).to_csv(csv_path, index=False)

    log.info("✅ %d instrumentos descargados. Caché guardada en: %s", len(symbols), csv_path)
    return symbols


def suscribir_tickers(
    symbols: list[str],
    worker_id: int,
    logger: logging.Logger,
) -> None:
    """
    Suscribe los tickers al WebSocket en lotes de SUBSCRIPTION_BATCH,
    con una pausa de 0.3s entre lotes para evitar flood al broker.
    """
    try:
        total_subs = 0
        for batch_start in range(0, len(symbols), SUBSCRIPTION_BATCH):
            batch = symbols[batch_start : batch_start + SUBSCRIPTION_BATCH]
            pyRofex.market_data_subscription(
                tickers=batch,
                entries=MARKET_DATA_ENTRIES,
                depth=5,
            )
            total_subs += len(batch)
            time.sleep(0.3)  # Evitar flood de suscripciones

        logger.info("✅ Worker %d ONLINE. Total suscrito: %d tickers.", worker_id, total_subs)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("💥 Worker %d error en suscripción: %s", worker_id, exc)


def prepare_system() -> tuple[list[str], dict[str, str]]:
    """
    Realiza la carga de entorno, validación de credenciales
    y conexión inicial para obtener el catálogo de instrumentos.

    Retorna:
        (all_symbols, credentials_dict)
    """
    log.info("🔍 Iniciando preparación del sistema...")

    # 1. Validar configuración
    user, password, broker = validate_config()
    credentials = {
        "user": user,
        "password": password,
        "broker": broker
    }

    # 2. Handshake inicial y Catálogo
    try:
        iniciar_conexion(user, password, broker)
        all_symbols = obtener_instrumentos()
        log.info("✅ Sistema preparado. %d instrumentos cargados.", len(all_symbols))
        return all_symbols, credentials
    except (requests.RequestException, RuntimeError) as exc:
        log.error("💥 Error crítico de conexión: %s", exc)
        raise
    except Exception as exc:  # pylint: disable=broad-except
        log.error("💥 Error inesperado en inicialización: %s", exc)
        raise
