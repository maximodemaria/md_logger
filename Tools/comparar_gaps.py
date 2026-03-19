"""
comparar_gaps.py - Herramienta para comparar gaps de dos fuentes de Market Data.
"""

import argparse
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def process_df(path):
    """
    Lee y procesa un archivo (parquet o db) para obtener gaps y tiempo local.
    """
    ext = os.path.splitext(path)[1].lower()
    
    if ext == '.parquet':
        df = pd.read_parquet(path)
    elif ext == '.db':
        conn = sqlite3.connect(path)
        # Intentamos leer la tabla marketdata por defecto
        try:
            df = pd.read_sql_query("SELECT * FROM marketdata", conn)
        except (pd.errors.DatabaseError, sqlite3.OperationalError):
            # Si falla, intentamos obtener la primera tabla disponible
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
            if tables.empty:
                conn.close()
                raise ValueError(f"No se encontraron tablas en la base de datos {path}")
            table_name = tables.iloc[0]['name']
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
    else:
        raise ValueError(f"Formato no soportado: {ext}. Use .parquet o .db")

    # 1. Asegurar el orden cronológico absoluto por el timestamp del broker
    df = df.sort_values('timestamp')

    # 2. Ajuste manual a UTC-3 (Hora Local Argentina)
    df['dt_origen'] = pd.to_datetime(df['timestamp'], unit='ms') - pd.Timedelta(hours=3)

    # 3. Calcular el gap en SEGUNDOS (ahora sobre datos ordenados)
    df['gap_s'] = df['timestamp'].diff() / 1000.0
    return df.dropna(subset=['gap_s'])

def main():
    """
    Función principal para comparar los gaps de dos archivos.
    """
    parser = argparse.ArgumentParser(description="Compara gaps de dos fuentes de Market Data")
    parser.add_argument("path1", help="Ruta al primer archivo (.parquet o .db)")
    parser.add_argument("path2", help="Ruta al segundo archivo (.parquet o .db)")
    parser.add_argument("--label1", default="Fuente 1", help="Etiqueta para la primera fuente")
    parser.add_argument("--label2", default="Fuente 2", help="Etiqueta para la segunda fuente")
    args = parser.parse_args()

    try:
        print(f"Leyendo {args.path1}...")
        df1 = process_df(args.path1)
        print(f"Leyendo {args.path2}...")
        df2 = process_df(args.path2)

        # 1. Encontrar el rango de tiempo solapado (Intersección)
        start_time = max(df1['dt_origen'].min(), df2['dt_origen'].min())
        end_time = min(df1['dt_origen'].max(), df2['dt_origen'].max())

        if start_time >= end_time:
            print("Error: No hay solapamiento de tiempo entre los dos archivos.")
            return

        # 2. Filtrar por el rango solapado
        df1 = df1[(df1['dt_origen'] >= start_time) & (df1['dt_origen'] <= end_time)]
        df2 = df2[(df2['dt_origen'] >= start_time) & (df2['dt_origen'] <= end_time)]

        # 3. Graficar
        plt.figure(figsize=(14, 7))
        
        # Fuente 1 (Azul)
        plt.fill_between(df1['dt_origen'], df1['gap_s'], color="skyblue", alpha=0.4, label=args.label1)
        plt.plot(df1['dt_origen'], df1['gap_s'], color="deepskyblue", alpha=0.7, linewidth=1)

        # Fuente 2 (Naranja)
        plt.fill_between(df2['dt_origen'], df2['gap_s'], color="orange", alpha=0.4, label=args.label2)
        plt.plot(df2['dt_origen'], df2['gap_s'], color="darkorange", alpha=0.7, linewidth=1)

        # Configuración estética
        title = (f"Comparativa de Gaps: {args.label1} vs {args.label2}\n"
                 f"(Rango solapado: {start_time.strftime('%H:%M:%S')} - "
                 f"{end_time.strftime('%H:%M:%S')})")
        plt.title(title, fontsize=14)
        plt.xlabel("Tiempo del Broker (Hora Local ARG)", fontsize=12)
        plt.ylabel("Gap Inter-mensaje (s)", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        
        # Formateo del eje X
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.gcf().autofmt_xdate()
        
        print(f"Graficando intersección: {len(df1):,} vs {len(df2):,} filas.")
        plt.tight_layout()
        plt.show()

    except FileNotFoundError as e:
        print(f"Error: Archivo no encontrado: {e.filename}")
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
