# ──────────────────────────────────────────────────────────────
# Dockerfile — MD Logger ROFEX/XOMS
# Imagen de producción para captura 24/7 de Market Data
# ──────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Metadata
LABEL maintainer="MD Logger" \
      description="Market Data Logger ROFEX/XOMS via pyRofex WebSockets"

# Variables de entorno del sistema
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PARQUET_DIR=/app/data/marketdata

    RUN mkdir -p /app/data/marketdata
    VOLUME ["/app/data/marketdata"]

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias del sistema necesarias para pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script principal y el .env (si existe localmente)
COPY main.py .
COPY .env* ./

# Crear el directorio de salida de datos Parquet
RUN mkdir -p /data/parquet

# Montar volumen para persistir los Parquet fuera del contenedor
VOLUME ["/data/parquet"]

# Puerto de métricas (opcional para futura integración con Prometheus)
# EXPOSE 8000

# Señal de parada: Docker enviará SIGTERM antes de SIGKILL
STOPSIGNAL SIGTERM

# Comando de inicio
CMD ["python", "-u", "main.py"]
