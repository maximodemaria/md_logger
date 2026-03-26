# ──────────────────────────────────────────────────────────────
# Dockerfile — MD Logger ROFEX/XOMS
# Imagen de producción para captura 24/7 de Market Data
# ──────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="md_logger2" \
    description="Market Data Logger ROFEX/XOMS via pyRofex WebSockets"

# 1. Variables de entorno - Unifica la ruta aquí
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data/marketdata \
    TZ=America/Cordoba 


WORKDIR /app

# 2. Solo librerías necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
# Recomiendo manejar el .env desde fuera, no copiarlo dentro de la imagen.

# 3. Define UN SOLO punto de persistencia
VOLUME ["/data/marketdata"]

STOPSIGNAL SIGTERM

CMD ["python", "-u", "main.py"]