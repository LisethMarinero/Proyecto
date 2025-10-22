# etl_supabase_completo.py
import os
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine, text
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

# --- LIMPIAR ARCHIVOS TEMPORALES ---
for f in os.listdir("."):
    if f.endswith(".nc") or f.endswith(".csv"):
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

# --- CREAR TABLAS ---
def crear_tablas(engine):
    with engine.begin() as conn:
        # Tabla general
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS reanalysis_era5_land (
            id SERIAL PRIMARY KEY,
            valid_time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            t2m FLOAT,
            d2m FLOAT,
            stl1 FLOAT,
            stl2 FLOAT,
            stl3 FLOAT,
            stl4 FLOAT,
            swvl1 FLOAT,
            swvl2 FLOAT,
            swvl3 FLOAT,
            swvl4 FLOAT,
            u10 FLOAT,
            v10 FLOAT,
            skt FLOAT,
            nieve FLOAT,
            sp FLOAT,
            tp FLOAT,
            ssrd FLOAT,
            strd FLOAT,
            fecha_actualizacion TIMESTAMP
        );
        """))

        # Tablas específicas
        tablas = [
            "pressure-precipitationw8_rcxxb",
            "radiation-heatcpg03hs6",
            "skin-temperaturehke46ner",
            "snowhy9lgjol",
            "soil-waterlxqhzxz9",
            "temperatureedviyn5g",
            "temperaturepf7g_14p",
            "windeh_9u766"
        ]
        for tabla in tablas:
            conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{tabla}" (
                id SERIAL PRIMARY KEY,
                valid_time TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                t2m FLOAT,
                d2m FLOAT,
                stl1 FLOAT,
                stl2 FLOAT,
                stl3 FLOAT,
                stl4 FLOAT,
                swvl1 FLOAT,
                swvl2 FLOAT,
                swvl3 FLOAT,
                swvl4 FLOAT,
                u10 FLOAT,
                v10 FLOAT,
                skt FLOAT,
                nieve FLOAT,
                sp FLOAT,
                tp FLOAT,
                ssrd FLOAT,
                strd FLOAT,
                fecha_actualizacion TIMESTAMP
            );
            """))

# --- OBTENER ÚLTIMO DÍA DISPONIBLE ---
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
                    'area': [14, -90, 13, -89],
                },
                archivo_prueba
            )
            os.remove(archivo_prueba)
            print(f"✅ Última fecha disponible: {fecha.strftime('%Y-%m-%d')}")
            return fecha
        except Exception as e:
            if "None of the data you have requested is available yet" in str(e):
                print(f"⚠️ {fecha.strftime('%Y-%m-%d')} aún no disponible, probando anterior...")
            else:
                print(f"⚠️ Error al probar {fecha.strftime('%Y-%m-%d')}: {e}")
    print("❌ No se encontró una fecha disponible en los últimos 10 días.")
    return None

# --- DESCARGAR Y CONVERTIR A CSV ---
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
                    "2m_temperature","2m_dewpoint_temperature","surface_pressure",
                    "total_precipitation","surface_solar_radiation_downwards",
                    "surface_thermal_radiation_downwards","skin_temperature",
                    "snow_cover","volumetric_soil_water_layer_1",
                    "volumetric_soil_water_layer_2","volumetric_soil_water_layer_3",
                    "volumetric_soil_water_layer_4","soil_temperature_level_1",
                    "soil_temperature_level_2","soil_temperature_level_3",
                    "soil_temperature_level_4","10m_u_component_of_wind",
                    "10m_v_component_of_wind"
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

        # Descomprimir si es necesario
        if zipfile.is_zipfile(archivo_nc):
            with zipfile.ZipFile(archivo_nc, 'r') as zip_ref:
                zip_ref.extractall(".")
                archivo_nc = zip_ref.namelist()[0]
            os.remove(f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.nc")
        elif archivo_nc.endswith(".gz"):
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
        df.rename(columns={"skin_temperature": "skt", "snow_cover": "nieve"}, inplace=True)
        df["fecha_actualizacion"] = datetime.now(pytz.UTC)
        df.to_csv(archivo_csv, index=False)
        ds.close()
        os.remove(archivo_nc)
        print(f"✅ CSV generado: {archivo_csv}")
        return archivo_csv
    except Exception as e:
        print(f"❌ Error durante descarga/conversión: {e}")
        return None

# --- CARGAR A TABLA GENERAL ---
def cargar_tabla_general(engine, archivo_csv):
    df = pd.read_csv(archivo_csv)
    with engine.begin() as conn:
        df.to_sql("reanalysis_era5_land", conn, if_exists="append", index=False)
    print(f"✅ Datos guardados en tabla general reanalysis_era5_land ({len(df)} filas).")

# --- DISTRIBUIR A OTRAS TABLAS ---
def distribuir_datos(engine):
    print("🔁 Distribuyendo datos a tablas específicas...")
    tablas = {
        "pressure-precipitationw8_rcxxb": "valid_time, latitude, longitude, sp, tp",
        "radiation-heatcpg03hs6": "valid_time, latitude, longitude, ssrd, strd",
        "skin-temperaturehke46ner": "valid_time, latitude, longitude, skt",
        "snowhy9lgjol": "valid_time, latitude, longitude, nieve",
        "soil-waterlxqhzxz9": "valid_time, latitude, longitude, swvl1, swvl2, swvl3, swvl4",
        "temperatureedviyn5g": "valid_time, latitude, longitude, d2m, t2m",
        "temperaturepf7g_14p": "valid_time, latitude, longitude, stl1, stl2, stl3, stl4",
        "windeh_9u766": "valid_time, latitude, longitude, u10, v10"
    }
    with engine.begin() as conn:
        for tabla, columnas in tablas.items():
            conn.execute(text(f"""
                INSERT INTO "{tabla}" ({columnas}, fecha_actualizacion)
                SELECT {columnas}, fecha_actualizacion
                FROM reanalysis_era5_land;
            """))
            print(f"✅ Datos copiados en {tabla}.")

# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL completo ERA5-Land...")
    engine = crear_engine()
    crear_tablas(engine)
    fecha = obtener_ultimo_dia_disponible()
    if fecha:
        archivo_csv = descargar_datos_csv(fecha)
        if archivo_csv:
            cargar_tabla_general(engine, archivo_csv)
            distribuir_datos(engine)
    else:
        print("⚠️ No se encontró una fecha con datos disponibles.")
    print("🎯 ETL finalizado correctamente.")


