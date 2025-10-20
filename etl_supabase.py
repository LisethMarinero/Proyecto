# etl_supabase_ultimo_dia.py
import os
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
import pytz
import gzip
import shutil
import zipfile

# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# --- CREAR .cdsapirc ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ['CDSAPI_URL']}\nkey: {os.environ['CDSAPI_KEY']}\n")

# --- LIMPIAR ARCHIVOS TEMPORALES .nc ---
for f in os.listdir("."):
    if f.endswith(".nc"):
        try:
            os.remove(f)
        except:
            pass

# --- CONEXIÓN A SUPABASE ---
def crear_engine():
    conexion = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(conexion, connect_args={'sslmode': 'require'})

# --- DETERMINAR ÚLTIMO DÍA DISPONIBLE ---
def obtener_ultimo_dia_disponible(max_dias=10):
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
                    'area': [14.0, -90.0, 13.5, -89.5],
                },
                archivo_prueba
            )
            os.remove(archivo_prueba)
            print(f"✅ Última fecha disponible confirmada: {fecha.strftime('%Y-%m-%d')}")
            return fecha
        except Exception as e:
            if "None of the data you have requested is available yet" in str(e):
                print(f"⚠️ {fecha.strftime('%Y-%m-%d')} aún no disponible, probando anterior...")
            else:
                print(f"⚠️ Error al probar {fecha.strftime('%Y-%m-%d')}: {e}")
    
    print("❌ No se encontró una fecha disponible en los últimos 10 días.")
    return None

# --- DESCARGAR DATOS Y CONVERTIR A CSV ---
def descargar_datos_csv(fecha):
    año, mes, dia = fecha.year, fecha.month, fecha.day
    archivo_nc = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.nc"
    archivo_csv = archivo_nc.replace(".nc", ".csv")

    if os.path.exists(archivo_csv):
        print(f"ℹ️ CSV ya existe: {archivo_csv}")
        return archivo_csv

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
            archivo_nc
        )
        print(f"✅ Archivo descargado: {archivo_nc}")

        # Si viene comprimido (ZIP o GZ), descomprimirlo
        if zipfile.is_zipfile(archivo_nc):
            print("🗜️ Descomprimiendo archivo ZIP...")
            with zipfile.ZipFile(archivo_nc, 'r') as zip_ref:
                zip_ref.extractall(".")
                contenido = zip_ref.namelist()[0]
            os.remove(archivo_nc)
            archivo_nc = contenido

        elif archivo_nc.endswith(".gz"):
            print("🗜️ Descomprimiendo archivo GZ...")
            archivo_descomprimido = archivo_nc.replace(".gz", "")
            with gzip.open(archivo_nc, 'rb') as f_in:
                with open(archivo_descomprimido, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(archivo_nc)
            archivo_nc = archivo_descomprimido

        # Convertir NetCDF → CSV
        print(f"⚙️ Convirtiendo {archivo_nc} a CSV...")
        ds = xr.open_dataset(archivo_nc, engine="netcdf4")
        df = ds.to_dataframe().reset_index()
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df["fecha_actualizacion"] = datetime.now(pytz.UTC)

        # Guardar CSV
        df.to_csv(archivo_csv, index=False)
        os.remove(archivo_nc)
        print(f"✅ CSV generado: {archivo_csv}")

        # 🔒 Cerrar el dataset antes de eliminar el archivo
        ds.close()

        # 🧹 Eliminar el archivo .nc una vez cerrado
        try:
            os.remove(archivo_nc)
            print(f"🧹 Archivo .nc eliminado.")
        except PermissionError:
            print(f"⚠️ No se pudo eliminar {archivo_nc}. Puede estar en uso.")

        return archivo_csv

    except Exception as e:
        print(f"❌ Error durante descarga/conversión: {e}")
        return None

# --- CARGAR CSV A SUPABASE ---
def cargar_a_supabase(archivo_csv):
    if not archivo_csv or not os.path.exists(archivo_csv):
        print("⚠️ No hay archivo CSV válido para cargar.")
        return

    try:
        print(f"📤 Cargando datos desde {archivo_csv} a Supabase...")
        df = pd.read_csv(archivo_csv)
        engine = crear_engine()
        nombre_tabla = "reanalysis_era5_land"
        df.to_sql(nombre_tabla, engine, if_exists="append", index=False)
        print(f"✅ Datos cargados a Supabase ({len(df)} filas).")
    except Exception as e:
        print(f"❌ Error al cargar a Supabase: {e}")

# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL ERA5-Land (descarga directa a CSV)...")
    fecha_disponible = obtener_ultimo_dia_disponible()
    if fecha_disponible:
        archivo_csv = descargar_datos_csv(fecha_disponible)
        cargar_a_supabase(archivo_csv)
    else:
        print("⚠️ No se encontró una fecha con datos disponibles.")
    print("🎯 ETL finalizado correctamente.")

