# etl_supabase_ultimo_dia.py
import os
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta, timezone
import pytz


# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"  # 🔹 API key

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# --- CREAR .cdsapirc ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ['CDSAPI_URL']}\n")
    f.write(f"key: {os.environ['CDSAPI_KEY']}\n")

# --- CONEXIÓN A SUPABASE ---
def crear_engine():
    conexion = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(conexion, connect_args={'sslmode': 'require'})

# --- DETERMINAR ÚLTIMO DÍA DISPONIBLE ---
def obtener_ultimo_dia_disponible(max_dias=10):
    """
    Intenta encontrar la fecha más reciente con datos disponibles en ERA5-Land.
    Retrocede hasta 10 días si es necesario.
    """
    print("🔍 Buscando la última fecha disponible de ERA5-Land...")
    c = cdsapi.Client()
    hoy = datetime.now(timezone.utc)
    
    for i in range(1, max_dias + 1):
        fecha = hoy - timedelta(days=i)
        año, mes, dia = fecha.year, fecha.month, fecha.day
        archivo_prueba = f"prueba_{año}_{mes:02d}_{dia:02d}.nc"

        try:
            c.retrieve(
                'reanalysis-era5-land',
                {
                    'format': 'netcdf',
                    'variable': ['2m_temperature'],
                    'year': [str(año)],
                    'month': [f"{mes:02d}"],
                    'day': [f"{dia:02d}"],
                    'time': ['00:00'],
                    'area': [14, -90, 13, -89],
                },
                archivo_prueba
            )
            # Si se descargó, significa que esta fecha sí tiene datos
            os.remove(archivo_prueba)
            print(f"✅ Última fecha disponible confirmada: {fecha.strftime('%Y-%m-%d')}")
            return fecha
        except Exception as e:
            mensaje = str(e)
            if "None of the data you have requested is available yet" in mensaje:
                print(f"⚠️ {fecha.strftime('%Y-%m-%d')} aún no disponible, probando anterior...")
            else:
                print(f"⚠️ Error al probar {fecha.strftime('%Y-%m-%d')}: {e}")
    
    print("❌ No se encontró una fecha disponible en los últimos 10 días.")
    return None

# --- DESCARGAR DATOS DEL DÍA ---
def descargar_datos(fecha):
    año, mes, dia = fecha.year, fecha.month, fecha.day
    archivo = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.nc"

    if os.path.exists(archivo):
        print(f"ℹ️ Archivo ya existe: {archivo}, se omite descarga.")
        return archivo

    print(f"🌍 Descargando datos ERA5-Land para {año}-{mes:02d}-{dia:02d}...")
    c = cdsapi.Client()
    try:
        c.retrieve(
            'reanalysis-era5-land',
            {
                'format': 'netcdf',
                'variable': [
                    "2m_temperature",
                    "2m_dewpoint_temperature",
                    "surface_pressure",
                    "total_precipitation"
                ],
                'year': [str(año)],
                'month': [f"{mes:02d}"],
                'day': [f"{dia:02d}"],
                'time': ['00:00'],
                'area': [14, -90, 13, -89],
            },
            archivo
        )
        print(f"✅ Archivo descargado: {archivo}")
        return archivo
    except Exception as e:
        print(f"⚠️ Error descargando {archivo}: {e}")
        return None

# --- PROCESAR Y CARGAR A SUPABASE ---
def procesar_y_cargar(archivo):
    if not archivo or not os.path.exists(archivo):
        print("⚠️ No hay archivo válido para procesar.")
        return

    if os.path.getsize(archivo) < 1000:
        print(f"⚠️ Archivo inválido o vacío: {archivo}")
        return

    try:
        print(f"⚙️ Procesando {archivo}...")
        ds = xr.open_dataset(archivo, engine="netcdf4", decode_cf=True)

        if not ds.variables:
            print(f"⚠️ Archivo sin variables: {archivo}")
            return

        df = ds.to_dataframe().reset_index()
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df["fecha_actualizacion"] = datetime.now(pytz.UTC)

        engine = crear_engine()
        nombre_tabla = "reanalysis_era5_land"
        df.to_sql(nombre_tabla, engine, if_exists="append", index=False)
        print(f"✅ Datos cargados en Supabase: {archivo} ({len(df)} filas)")
    except Exception as e:
        print(f"❌ Error procesando {archivo}: {e}")

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL de la última actualización disponible ERA5-Land...")
    fecha_disponible = obtener_ultimo_dia_disponible()
    if fecha_disponible:
        archivo = descargar_datos(fecha_disponible)
        procesar_y_cargar(archivo)
    else:
        print("⚠️ No se pudo determinar una fecha con datos disponibles.")
    print("🎯 ETL completado con éxito.")
