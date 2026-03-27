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
from utils import setup_signals, wait_for_market_open, is_market_open
from process_manager import ProcessManager
from stats import setup_central_metrics


def main() -> None:
    """
    Orquestación autónoma del sistema:
    1. Bucle infinito para ciclos diarios.
    2. Espera apertura de mercado (soporta el día siguiente).
    3. Inicialización y ejecución de la jornada.
    4. Unificación al cierre y espera del nuevo ciclo.
    """
    while True:
        # Cada ciclo diario (o reinicio), creamos un nuevo stop_event
        stop_event = multiprocessing.Event()
        setup_signals(stop_event)

        # ── 1. Espera apertura de mercado ──────────────────────────────────
        wait_for_market_open(MARKET_START, MARKET_END, stop_event)

        # Si durante la espera se recibió una señal de apagado manual (Ctrl+C / SIGTERM)
        if stop_event.is_set():
            break

        # ── 2. Inicialización de Métricas Centralizadas ──────────────────────
        metrics_queue = setup_central_metrics(stop_event)

        # ── 3. Inicialización Integral ───────────────────────────────────────
        try:
            all_symbols, credentials = prepare_system()
        except Exception as exc:  # pylint: disable=broad-except
            log.error("💥 Error crítico al preparar el sistema: %s", exc)
            log.info("💤 Reintentando en el próximo ciclo...")
            time.sleep(60)
            continue

        # ── 4. Lanzamiento y Control (ProcessManager) ────────────────────────
        manager = ProcessManager(
            all_symbols, NUM_WORKERS, credentials, stop_event, metrics_queue
        )
        manager.start_all()

        log.info("🔥 Sistema ONLINE. Monitoreando salud y horario...")
        try:
            while not stop_event.is_set():
                # 1. Verificar si el mercado ya cerró
                if not is_market_open(MARKET_END):
                    log.info(
                        "🔔 Mercado cerrado (%s). Iniciando apagado programado...",
                        MARKET_END
                    )
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

            log.info("💤 Jornada finalizada. Esperando apertura del próximo día...")

        # Si se activó el stop_event por una señal externa (no por horario), rompemos el bucle
        # pero el stop_event.is_set() captura la intención de apagado.
        if not is_market_open(MARKET_END):
            # Fue cierre programado, NO rompemos el bucle While True.
            continue
        else:
            # Fue cierre manual o error crítico antes de tiempo.
            break

    log.info("🏁 Sistema MD Logger finalizado.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
