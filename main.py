"""
main.py — MD Logger ROFEX/XOMS (Orquestador Minimalista)
========================================================
"""

from __future__ import annotations

import multiprocessing
import time

from config import (
    MARKET_END,
    MARKET_START,
    NUM_WORKERS,
    PARQUET_DIR,
    log,
)
from datetime import datetime
from conexion import prepare_system
from utils import setup_signals, wait_for_market_open, is_market_open
from process_manager import ProcessManager
from stats import setup_central_metrics
from parquet_unifier import unify_day


def main() -> None:
    """
    Orquestación minimalista del sistema:
    1. Registra señales.
    2. Espera apertura de mercado (si aplica).
    3. Inicializa el sistema (credenciales + catálogo).
    4. Particiona la carga.
    5. Lanza ProcessManager y su Watchdog con Agregador de Métricas.
    6. Shutdown limpio al finalizar.
    """
    # ── 1. Señales y Horario ─────────────────────────────────────────────
    stop_event = multiprocessing.Event()
    setup_signals(stop_event)
    wait_for_market_open(MARKET_START, stop_event)

    # ── 2. Inicialización de Métricas Centralizadas ──────────────────────
    metrics_queue = setup_central_metrics(stop_event)

    # ── 3. Inicialización Integral ───────────────────────────────────────
    try:
        all_symbols, credentials = prepare_system()
    except Exception as exc:  # pylint: disable=broad-except
        log.error("💥 Error crítico al preparar el sistema: %s", exc)
        return

    # ── 4. Lanzamiento y Control (ProcessManager) ────────────────────────
    manager = ProcessManager(all_symbols, NUM_WORKERS, credentials, stop_event, metrics_queue)
    manager.start_all()

    log.info("🔥 Sistema ONLINE. Monitoreando salud y horario...")
    try:
        while not stop_event.is_set():
            # 1. Verificar si el mercado ya cerró
            if not is_market_open(MARKET_END):
                log.info("🔔 Mercado cerrado (%s). Iniciando apagado programado...", MARKET_END)
                stop_event.set()
                break

            # 2. Watchdog de salud
            try:
                manager.check_health()
            except Exception as e:  # pylint: disable=broad-except
                log.error("Error en el monitoreo de salud: %s", e)

            # 3. Espera controlada (10s)
            for _ in range(10):
                if stop_event.is_set():
                    break
                time.sleep(1)

    except KeyboardInterrupt:
        log.warning("⚠️ Interrupción manual detectada (Ctrl+C).")
        stop_event.set()
    finally:
        # ── 5. Shutdown Limpio ───────────────────────────────────────────────
        manager.join_all()

        # ── 6. Unificación Automática ────────────────────────────────────────
        try:
            date_str = datetime.now().strftime("%Y%m%d")
            log.info("📦 Iniciando unificación automática de chunks para %s...", date_str)
            unify_day(date_str, PARQUET_DIR, delete_chunks=True)
        except Exception as e:
            log.error("❌ Falló la unificación automática: %s", e)

        log.info("🏁 Sistema MD Logger finalizado.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
