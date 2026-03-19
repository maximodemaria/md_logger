"""
comparar_fila_max_diff.py - Análisis forense de mensajes con máximo desfase.
"""

import json
import os
import sqlite3
import pandas as pd

PATH_A = r"c:\Users\max\Desktop\Drive\md_logger\data\marketdata\marketdata_20260319.parquet"
PATH_B = r"C:\Users\max\Downloads\2026-03-19.db"

def load_data(path):
    """Lee y prepara el DataFrame para la comparación forense."""
    ext = os.path.splitext(path)[1].lower()
    if ext == '.parquet':
        df = pd.read_parquet(path)
    else:
        conn = sqlite3.connect(path)
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        table_name = (
            'market_data' if 'market_data' in tables['name'].values
            else tables.iloc[0]['name']
        )
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

    if 'instrumentId' in df.columns:
        # Extraer símbolo simple
        df['symbol_norm'] = df['instrumentId'].apply(
            lambda x: json.loads(x).get('symbol', '') if isinstance(x, str) else ''
        )
    else:
        df['symbol_norm'] = df['symbol']

    return df.sort_values('timestamp')

def main():
    """
    Función principal para encontrar y reportar la fila con mayor diferencia.
    """
    symbol = 'MERV - XMEV - YPFD - 24hs'
    print(f"Buscando discrepancias para {symbol}...")

    df_a = load_data(PATH_A)
    df_b = load_data(PATH_B)

    # Filtrar por símbolo
    df_a = df_a[df_a['symbol_norm'] == symbol]
    df_b = df_b[df_b['symbol_norm'] == symbol]

    # Preparar B para merge_asof
    df_b_renamed = df_b.rename(columns={'timestamp': 'timestamp_b'})

    # Emparejar
    merged = pd.merge_asof(
        df_a, df_b_renamed,
        left_on='timestamp',
        right_on='timestamp_b',
        direction='nearest',
        tolerance=500
    )

    # Calcular diferencia
    merged['diff_ms'] = (merged['timestamp'] - merged['timestamp_b']).abs()

    # Encontrar la fila con la diferencia máxima (alrededor de 498ms)
    max_diff_row = merged.sort_values('diff_ms', ascending=False).iloc[0]

    with open("reporte_max_diff.txt", "w", encoding="utf-8") as f:
        f.write("="*50 + "\n")
        f.write(" MENSAJES COMPARADOS CON DESFASE MÁXIMO\n")
        f.write("="*50 + "\n")
        f.write(f"Símbolo:    {symbol}\n")
        f.write(f"Diferencia: {max_diff_row['diff_ms']} ms\n")
        f.write("-" * 50 + "\n")

        f.write("\n[FUENTE A - PARQUET]\n")
        f.write(f"Timestamp:      {max_diff_row['timestamp']}\n")
        f.write(f"Arrival Local:  {max_diff_row['_arrival_ts_ms']}\n")
        f.write(f"marketData:     {max_diff_row['marketData']}\n")

        f.write("\n[FUENTE B - SQLITE]\n")
        f.write(f"Timestamp B:    {max_diff_row['timestamp_b']}\n")
        f.write(f"Local Time B:   {max_diff_row['local_time']}\n")
        f.write(f"Last Price:     {max_diff_row['last_price']}\n")
        f.write(f"Nominal Vol:    {max_diff_row['nominal_volume']}\n")
        f.write("="*50 + "\n")

    print("Reporte generado en reporte_max_diff.txt")

if __name__ == "__main__":
    main()
