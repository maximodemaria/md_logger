"""
config.py — Configuración central de MD Logger
================================================
Contiene: constantes globales, setup de logging y carga de variables de entorno.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import time

import pyRofex
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# ──────────────────────────────────────────────
# CONSTANTES DE CONFIGURACIÓN
# ──────────────────────────────────────────────
NUM_WORKERS: int = 8          # Número de procesos paralelos
BUFFER_SIZE: int = 5_000      # Mensajes acumulados antes de guardar Parquet
SUBSCRIPTION_BATCH: int = 250 # Tickers por llamada de suscripción
HEALTH_LOG_EVERY: int = 500   # (Legacy) Reportar cada N msgs
GAP_WARN_SECS: float = 1.5    # Umbral de gap WebSocket para advertencia
TIMEOUT_INACTIVO: float = 60.0 # Segundos sin mensajes antes de reintentar
PARQUET_DIR: str = os.getenv("PARQUET_DIR", os.path.join("data", "marketdata"))
LOGS_DIR: str = os.path.join("data", "logs")
METRICS_INTERVAL_SECS: float = 1.0

# Horario estricto de mercado (ROFEX)
MARKET_START = time(10, 25)
MARKET_END = time(17, 5)

# Campos de Market Data a suscribir
MARKET_DATA_ENTRIES = [
    pyRofex.MarketDataEntry.BIDS,
    pyRofex.MarketDataEntry.OFFERS,
    pyRofex.MarketDataEntry.LAST,
    pyRofex.MarketDataEntry.OPENING_PRICE,
    pyRofex.MarketDataEntry.CLOSING_PRICE,
    pyRofex.MarketDataEntry.HIGH_PRICE,
    pyRofex.MarketDataEntry.LOW_PRICE,
    pyRofex.MarketDataEntry.SETTLEMENT_PRICE,
    pyRofex.MarketDataEntry.TRADE_VOLUME,
    pyRofex.MarketDataEntry.TRADE_EFFECTIVE_VOLUME,
    pyRofex.MarketDataEntry.NOMINAL_VOLUME,
    pyRofex.MarketDataEntry.OPEN_INTEREST,
]

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("md_logger")
