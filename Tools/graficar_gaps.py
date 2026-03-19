"""
graficar_gaps.py - Herramienta para visualizar latencia y silencios de mercado.
"""

import argparse
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def main():
    """
    Función principal para graficar los gaps de un archivo Parquet.
    """
    # 1. Configuración de argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Graficador de gaps de Market Data (Origen)")
    parser.add_argument("path", help="Ruta al archivo .parquet generado por MD Logger")
    args = parser.parse_args()

    try:
        # 2. Lectura del archivo Parquet
        df = pd.read_parquet(args.path)
        
        if 'timestamp' not in df.columns:
            print(f"Error: La columna 'timestamp' no existe en {args.path}")
            return

        # 3. Procesamiento de datos
        # Aseguramos el orden cronológico según el broker
        df = df.sort_values('timestamp')

        # Convertimos el timestamp de origen (ms) a objetos datetime ajustados a UTC-3
        df['dt_origen'] = pd.to_datetime(df['timestamp'], unit='ms') - pd.Timedelta(hours=3)

        # Calculamos el gap entre timestamps de origen en SEGUNDOS
        df['gap_s'] = df['timestamp'].diff() / 1000.0

        # Eliminamos el primer registro (que siempre tendrá gap NaN)
        df = df.dropna(subset=['gap_s'])

        # 4. Generación del Gráfico de Área
        plt.figure(figsize=(12, 6))
        
        # Gráfico de área
        plt.fill_between(
            df['dt_origen'], df['gap_s'], color="skyblue", alpha=0.4, label='Gap de Origen'
        )
        plt.plot(df['dt_origen'], df['gap_s'], color="Slateblue", alpha=0.6, linewidth=1)

        # Configuración estética
        plt.title(f"Gaps de Tiempo de Origen (Broker TS - UTC-3)\nArchivo: {args.path}", fontsize=14)
        plt.xlabel("Tiempo del Broker (Hora Local ARG)", fontsize=12)
        plt.ylabel("Gap Inter-mensaje (s)", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        
        # Formateo del eje X: solo mostrar el horario (HH:MM:SS)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        
        # Ajuste dinámico de etiquetas de tiempo
        plt.gcf().autofmt_xdate()
        
        print(f"Procesadas {len(df):,} filas. Mostrando gráfico...")
        plt.tight_layout()
        plt.show()

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en la ruta: {args.path}")
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        print(f"Error al procesar el archivo Parquet: {e}")
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
