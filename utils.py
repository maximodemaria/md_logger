"""
utils.py — Funciones de utilidad y soporte
===========================================
Contiene lógica auxiliar para validación, señales, tiempo y distribución.
"""

from __future__ import annotations

import os
import signal
import sys
import time
import typing
from datetime import datetime, timedelta, time as dt_time

from config import log


def validate_config() -> tuple[str, str, str]:
    """
    Verifica que las variables de entorno necesarias estén presentes.
    Lanza un error y detiene la ejecución si falta alguna.
    Retorna la tupla (user, password, broker).
    """
    user = os.getenv("XOMS_USERNAME")
    password = os.getenv("XOMS_PASSWORD")
    broker = os.getenv("XOMS_BROKER")

    if not user or not password or not broker:
        log.error(
            "❌ Variables de entorno XOMS_USERNAME, XOMS_PASSWORD o XOMS_BROKER no configuradas."
        )
        sys.exit(1)

    return user, password, broker


def setup_signals(stop_event: typing.Any) -> None:
    """
    Registra los manejadores de señales SIGINT y SIGTERM para 
    activar el stop_event y realizar un apagado limpio.
    """
    def shutdown_handler(signum: int, _frame: typing.Any) -> None:
        sig_name = signal.Signals(signum).name
        log.warning("\n🛑 Señal %s recibida. Iniciando graceful shutdown...", sig_name)
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    log.info("🛡️  Manejadores de señales registrados (SIGINT/SIGTERM).")


def wait_for_market_open(start_time: dt_time, end_time: dt_time, stop_event: typing.Any) -> None:
    """
    Si la hora actual es previa a start_time, espera al inicio de hoy.
    Si está entre start_time y end_time, inicia inmediatamente.
    Si es posterior a end_time, espera al inicio del día siguiente.
    Comprueba el stop_event para permitir un apagado inmediato.
    """
    now = datetime.now()
    now_time = now.time()

    # ── 1. Caso: Mercado ya está abierto ──
    if start_time <= now_time < end_time:
        log.info("🌅 Mercado abierto. Iniciando inmediatamente.")
        return

    # ── 2. Caso: Mercado cerrado (Pre-apertura o Post-cierre) ──
    market_start_dt = datetime.combine(now.date(), start_time)

    if now_time >= end_time:
        # Ya cerró hoy, buscamos el inicio de mañana
        market_start_dt += timedelta(days=1)
        log.info("🕒 El mercado ya cerró por hoy. Programando para mañana.")
    else:
        # Aún no abre hoy
        log.info("💤 Mercado aún no abre. Esperando apertura de hoy.")

    wait_secs = (market_start_dt - now).total_seconds()
    log.info(
        "💤 Esperando hasta las %s (%.0f seg)...",
        market_start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        wait_secs
    )

    while datetime.now() < market_start_dt and not stop_event.is_set():
        time.sleep(1.0)

    if stop_event.is_set():
        log.info("🛑 Espera interrumpida por señal de apagado.")
    else:
        log.info("🌅 Hora de inicio alcanzada. Procediendo con la ejecución.")


def is_market_open(end_time: dt_time) -> bool:
    """
    Devuelve True si la hora actual es menor a end_time.
    """
    return datetime.now().time() < end_time


def splitter(all_symbols: list[str], num_workers: int) -> list[list[str]]:
    """
    Divide los instrumentos entre la cantidad de workers de forma balanceada.
    Utiliza el método de saltos [i::num_workers] para asegurar una distribución equitativa.
    """
    return [all_symbols[i::num_workers] for i in range(num_workers)]
