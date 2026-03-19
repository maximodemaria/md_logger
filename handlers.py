"""
handlers.py — Clases de soporte para el manejo de WebSocket
============================================================
Contiene:
  - WebSocketHandler: Gestiona los mensajes y errores del WebSocket.
"""

from __future__ import annotations

import logging
import queue
import time
from typing import Any


class WebSocketHandler:
    """
    Gestiona los mensajes y errores del WebSocket de market data.
    Estrategia: No calcula métricas, solo encola y delega el procesamiento.
    """

    def __init__(
        self,
        worker_id: int,
        logger: logging.Logger,
        data_queue: queue.Queue[dict[str, Any]],
        metrics_queue: queue.Queue[tuple[float, float]],
    ) -> None:
        self.worker_id = worker_id
        self.logger = logger
        self.data_queue = data_queue
        self.metrics_queue = metrics_queue
        self.last_msg_mono = time.monotonic()

    def on_message(self, message: dict) -> None:
        """Callback para el WebSocket. Nunca debe bloquear."""
        if "marketData" not in message:
            return

        now_mono = time.monotonic()
        local_ts_ms = int(time.time() * 1000)
        message["_arrival_ts_ms"] = local_ts_ms

        # Extraer timestamp del broker para la métrica de latencia
        broker_ts = message.get("timestamp", local_ts_ms)
        latency_ms = local_ts_ms - broker_ts
        gap = now_mono - self.last_msg_mono
        self.last_msg_mono = now_mono

        # 1. Enviar mensaje a la cola de datos (Parquet)
        try:
            self.data_queue.put_nowait(message)
        except queue.Full:
            self.logger.warning(
                "⚠️ Cola de DATOS llena en W%d — mensaje descartado", self.worker_id
            )

        # 2. Enviar métricas a la cola de métricas (Asíncrono)
        # Si la cola de métricas está llena, se descarta para priorizar datos
        try:
            self.metrics_queue.put_nowait((latency_ms, gap))
        except queue.Full:
            pass  # Descartar métrica si el hilo de métricas está lento

    def on_error(self, message: dict) -> None:
        """Callback para errores del WebSocket."""
        self.logger.error("❌ [WS ERROR W%d]: %s", self.worker_id, message)
