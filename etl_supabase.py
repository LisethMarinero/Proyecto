# etl_supabase_v8.py
import os
import glob
import pandas as pd
import xarray as xr
import cdsapi
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import time



# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"


os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"
# --- CONEXIÓN A BASE DE DATOS ---
def crear_engine():
    engine = create_engine(DB_URL)
    return engine

# --- CREAR TABLA PRINCIPAL ---
def crear_tablas(engine):
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

# --- DESCARGAR DATOS ERA5-LAND ---
def descargar_datos_csv(fecha):
    fecha_str = fecha.strftime("%Y-%m-%d")
    print(f"📥 Descargando datos de ERA5-Land para {fecha_str}...")

    c = cdsapi.Client()
    archivo_nc = f"data_{fecha_str}.nc"

    c.retrieve(
        "reanalysis-era5-land",
        {
            "variable": ["2m_temperature", "2m_dewpoint_temperature"],
            "year": [str(fecha.year)],
            "month": [f"{fecha.month:02d}"],
            "day": [f"{fecha.day:02d}"],
            "time": [f"{h:02d}:00" for h in range(24)],
            "format": "netcdf",
            "area": [14.45, -90.13, 12.98, -87.68],  # El Salvador
        },
        archivo_nc,
    )

    # Convertir NetCDF a CSV
    ds = xr.open_dataset(archivo_nc)
    df = ds.to_dataframe().reset_index()
    df["fecha_actualizacion"] = datetime.now(timezone.utc)
    archivo_csv = f"data_{fecha_str}.csv"
    df.to_csv(archivo_csv, index=False)
    print(f"✅ CSV generado: {archivo_csv}")
    return archivo_csv

# --- CARGAR DATOS EN BASE DE DATOS ---
def cargar_tabla_general(engine, archivo_csv):
    df = pd.read_csv(archivo_csv)
    if "t2m" in df.columns:
        df["t2m"] = df["t2m"] - 273.15  # Kelvin → Celsius
    if "d2m" in df.columns:
        df["d2m"] = df["d2m"] - 273.15

    df.to_sql("temperatureedviyn5g", engine, if_exists="append", index=False, method="multi", chunksize=500)
    print(f"✅ Datos cargados en 'temperatureedviyn5g'.")

# --- EJECUCIÓN PRINCIPAL ---
def main():
    engine = crear_engine()
    crear_tablas(engine)

    fecha_inicio = datetime(2025, 10, 3, tzinfo=timezone.utc)
    fecha_fin = datetime(2025, 10, 24, tzinfo=timezone.utc)
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        try:
            archivo_csv = descargar_datos_csv(fecha_actual)
            cargar_tabla_general(engine, archivo_csv)
            print(f"🎉 ETL completado para {fecha_actual.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"❌ Error en {fecha_actual.strftime('%Y-%m-%d')}: {e}")

        fecha_actual += timedelta(days=1)
        time.sleep(5)  # evitar sobrecarga al servidor CDS

    print("\n🧹 ETL completado para el rango del 3 al 24 de octubre de 2025.")

if __name__ == "__main__":
    main()
