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
    log,
)
from conexion import prepare_system
from utils import setup_signals, wait_for_market_open, is_market_open, splitter
from process_manager import ProcessManager


def main() -> None:
    """
    Orquestación minimalista del sistema:
    1. Registra señales.
    2. Espera apertura de mercado (si aplica).
    3. Inicializa el sistema (credenciales + catálogo).
    4. Particiona la carga.
    5. Lanza ProcessManager y su Watchdog.
    6. Shutdown limpio al finalizar.
    """
    # ── 1. Señales y Horario ─────────────────────────────────────────────
    stop_event = multiprocessing.Event()
    setup_signals(stop_event)
    wait_for_market_open(MARKET_START, stop_event)

    # ── 2. Inicialización Integral ───────────────────────────────────────
    try:
        all_symbols, credentials = prepare_system()
    except Exception as exc:  # pylint: disable=broad-except
        log.error("💥 Error crítico al preparar el sistema: %s", exc)
        return

    # ── 3. Lanzamiento y Control (ProcessManager) ────────────────────────
    manager = ProcessManager(all_symbols, NUM_WORKERS, credentials, stop_event)
    manager.start_all()

    log.info("🔥 Sistema ONLINE. Monitoreando salud y horario...")

    # Bucle de control principal
    while not stop_event.is_set():
        if not is_market_open(MARKET_END):
            log.warning("🔔 Horario de mercado finalizado (%s).", MARKET_END)
            stop_event.set()
            break

        manager.check_health()
        time.sleep(5.0)

    # ── 5. Cierre limpio ─────────────────────────────────────────────────
    manager.join_all()
    log.info("✅ Ejecución finalizada correctamente.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
