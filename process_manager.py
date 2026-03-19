"""
process_manager.py — Orquestador del ciclo de vida de procesos worker
=====================================================================
"""

from __future__ import annotations

import multiprocessing

from config import log
from utils import splitter
from worker import worker_process


class ProcessManager:
    """
    Gestiona el ciclo de vida completo de los procesos worker:
    lanzamiento, monitoreo de salud (watchdog) y cierre ordenado.
    """

    def __init__(
        self,
        all_symbols: list[str],
        num_workers: int,
        credentials: dict[str, str],
        stop_event: multiprocessing.Event,  # type: ignore[type-arg]
    ) -> None:
        self.shards = splitter(all_symbols, num_workers)
        self.credentials = credentials
        self.stop_event = stop_event
        self.processes: list[multiprocessing.Process] = []

    def start_all(self) -> None:
        """Crea y arranca todos los procesos worker."""
        self.processes = []
        for i, shard in enumerate(self.shards):
            p = multiprocessing.Process(
                target=worker_process,
                args=(i, shard, self.credentials, self.stop_event),
                name=f"md-worker-{i}",
                daemon=False,
            )
            p.start()
            self.processes.append(p)
            log.info("  ✅ Proceso W%d lanzado (PID: %d) con %d tickers.", i, p.pid, len(shard))

    def check_health(self) -> None:
        """
        Verifica si algún proceso ha muerto y lo reinicia si es necesario.
        """
        for i, p in enumerate(self.processes):
            if not (p and p.is_alive()):
                log.warning("🚨 Worker %d caído. Reiniciando...", i)

                nuevo_p = multiprocessing.Process(
                    target=worker_process,
                    args=(i, self.shards[i], self.credentials, self.stop_event),
                    name=f"md-worker-{i}",
                    daemon=False,
                )
                nuevo_p.start()
                self.processes[i] = nuevo_p
                log.info("🔄 Worker %d reiniciado exitosamente (Nuevo PID: %d)", i, nuevo_p.pid)

    def join_all(self) -> None:
        """Espera a que todos los procesos terminen su ejecución."""
        log.info("🛑 Apagando orquestador. Esperando a que todos los workers finalicen...")
        for p in self.processes:
            if p.is_alive():
                p.join()
            log.info("  Proceso %s terminado con código: %s", p.name, p.exitcode)
