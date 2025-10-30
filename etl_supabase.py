# etl_supabase_v9_backup.py
import os
import glob
import zipfile
import shutil
import pandas as pd
import cdsapi
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import time

# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"

# Configuración base de datos
DB_USER = "postgres.gkzvbidocktfkwhvngpg"
DB_PASSWORD = "Hipopotamo123456"
DB_HOST = "aws-1-us-east-2.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- FUNCIONES ---

def crear_engine():
    """Crea conexión a la base de datos."""
    engine = create_engine(DB_URL, connect_args={'sslmode': 'require'})
    print("✅ Conexión a la base de datos establecida")
    return engine

def crear_tablas(engine):
    """Crea la tabla principal si no existe."""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS "temperatureedviyn5g" (
            valid_time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            t2m FLOAT,
            d2m FLOAT,
            fecha_actualizacion TIMESTAMP,
            PRIMARY KEY (valid_time, latitude, longitude)
        );
        """))
    print("✅ Tabla 'temperatureedviyn5g' creada o verificada")

def descargar_datos_csv(fecha):
    """Descarga datos ERA5-Land, extrae CSV del ZIP y retorna la ruta del CSV."""
    fecha_str = fecha.strftime("%Y-%m-%d")
    print(f"\n📥 Descargando datos de ERA5-Land para {fecha_str}...")

    c = cdsapi.Client()
    zip_file = f"data_{fecha_str}.zip"

    # Descargar el ZIP
    c.retrieve(
        "reanalysis-era5-land",
        {
            "variable": ["2m_temperature", "2m_dewpoint_temperature"],
            "year": [str(fecha.year)],
            "month": [f"{fecha.month:02d}"],
            "day": [f"{fecha.day:02d}"],
            "time": [f"{h:02d}:00" for h in range(24)],
            "format": "zip",
            "area": [14.45, -90.13, 12.98, -87.68],  # El Salvador
        },
        zip_file
    )

    # Extraer CSV del ZIP
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall("temp_data")
    csv_files = glob.glob("temp_data/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"No se encontró CSV dentro de {zip_file}")
    
    archivo_csv = csv_files[0]
    print(f"✅ CSV extraído: {archivo_csv}")
    return archivo_csv

def cargar_tabla_general(engine, archivo_csv):
    """Carga los datos del CSV a la base de datos y convierte K a °C."""
    df = pd.read_csv(archivo_csv)

    # Convertir de Kelvin a Celsius
    if "t2m" in df.columns:
        df["t2m"] = df["t2m"] - 273.15
    if "d2m" in df.columns:
        df["d2m"] = df["d2m"] - 273.15

    df["fecha_actualizacion"] = datetime.now(timezone.utc)

    df.to_sql("temperatureedviyn5g", engine, if_exists="append", index=False, method="multi", chunksize=500)
    print(f"✅ Datos cargados en 'temperatureedviyn5g' ({len(df)} filas)")

def limpiar_archivos():
    """Elimina archivos temporales descargados y extraídos."""
    for f in glob.glob("data_*.zip"):
        os.remove(f)
    for f in glob.glob("temp_data/*"):
        os.remove(f)
    os.rmdir("temp_data")
    print("🧹 Archivos temporales eliminados")

# --- EJECUCIÓN PRINCIPAL ---
def main():
    engine = crear_engine()
    crear_tablas(engine)

    # Crear carpetas temporales y backups
    os.makedirs("temp_data", exist_ok=True)
    os.makedirs("backup_csvs", exist_ok=True)

    fecha_inicio = datetime(2025, 10, 3, tzinfo=timezone.utc)
    fecha_fin = datetime(2025, 10, 24, tzinfo=timezone.utc)
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        try:
            archivo_csv = descargar_datos_csv(fecha_actual)
            
            # 🔹 Backup del CSV antes de cargarlo
            backup_path = f"backup_csvs/{fecha_actual.strftime('%Y-%m-%d')}.csv"
            shutil.copy(archivo_csv, backup_path)
            print(f"💾 CSV respaldado en {backup_path}")
            
            cargar_tabla_general(engine, archivo_csv)
            print(f"🎉 ETL completado para {fecha_actual.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"❌ Error en {fecha_actual.strftime('%Y-%m-%d')}: {e}")

        fecha_actual += timedelta(days=1)
        time.sleep(5)  # evitar sobrecarga al servidor CDS

    limpiar_archivos()
    print("\n✅ ETL completado para todo el rango del 3 al 24 de octubre de 2025")

if __name__ == "__main__":
    main()
