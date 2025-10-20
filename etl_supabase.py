# etl_supabase_ultimo_dia.py
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

# === CONFIGURACIÓN ===
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# === CREAR ARCHIVO .cdsapirc ===
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ['CDSAPI_URL']}\nkey: {os.environ['CDSAPI_KEY']}\n")

# === FUNCIÓN: Crear conexión a Supabase ===
def crear_engine():
    conexion = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(conexion, connect_args={'sslmode': 'require'})

# === FUNCIÓN: Obtener último día disponible ===
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

# === FUNCIÓN: Descargar datos y convertir a CSV ===
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
                    "2m_temperature", "2m_dewpoint_temperature",
                    "surface_pressure", "total_precipitation",
                    "10m_u_component_of_wind", "10m_v_component_of_wind",
                    "skin_temperature", "surface_solar_radiation_downwards",
                    "snow_depth", "volumetric_soil_water_layer_1",
                    "volumetric_soil_water_layer_2",
                    "volumetric_soil_water_layer_3",
                    "volumetric_soil_water_layer_4",
                    "soil_temperature_level_1",
                    "soil_temperature_level_2",
                    "soil_temperature_level_3",
                    "soil_temperature_level_4"
                ],
                'year': [str(año)],
                'month': [f"{mes:02d}"],
                'day': [f"{dia:02d}"],
                'time': ['00:00'],
                'area': [14, -90, 13, -89],
            },
            archivo_nc
        )

        ds = xr.open_dataset(archivo_nc)
        df = ds.to_dataframe().reset_index()
        ds.close()

        # Normalizar nombres de columnas
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df.rename(columns={
            "latitude": "latitud",
            "longitude": "longitud",
            "time": "tiempo_valido",
            "2m_temperature": "t2m",
            "2m_dewpoint_temperature": "d2m",
            "surface_pressure": "sp",
            "total_precipitation": "tp",
            "10m_u_component_of_wind": "u10",
            "10m_v_component_of_wind": "v10",
            "surface_solar_radiation_downwards": "ssd",
            "snow_depth": "nieve",
            "volumetric_soil_water_layer_1": "swvl1",
            "volumetric_soil_water_layer_2": "swvl2",
            "volumetric_soil_water_layer_3": "swvl3",
            "volumetric_soil_water_layer_4": "swvl4",
            "soil_temperature_level_1": "stl1",
            "soil_temperature_level_2": "stl2",
            "soil_temperature_level_3": "stl3",
            "soil_temperature_level_4": "stl4"
        }, inplace=True)

        df["fecha_actualizacion"] = datetime.now(pytz.UTC)
        ds.close()
        df.to_csv(archivo_csv, index=False)
        os.remove(archivo_nc)
        print(f"✅ CSV generado: {archivo_csv}")

        os.remove(archivo_nc)
        return archivo_csv

    except Exception as e:
        print(f"❌ Error durante la descarga/conversión: {e}")
        return None

# === FUNCIÓN: Subir datos y distribuir a tablas ===
def cargar_y_distribuir(archivo_csv):
    if not archivo_csv or not os.path.exists(archivo_csv):
        print("⚠️ No hay archivo CSV válido para cargar.")
        return

    engine = crear_engine()
    df = pd.read_csv(archivo_csv)
    tabla_general = "reanalysis_era5_land"

    try:
        print(f"📤 Cargando datos a tabla general '{tabla_general}'...")
        df.to_sql(tabla_general, engine, if_exists="append", index=False)
        print(f"✅ Datos insertados en {tabla_general} ({len(df)} filas).")

        # Diccionario de destino: tabla → columnas
        tablas_destino = {
            "radiación-calorcpg03hs6": ["tiempo_valido", "ssd", "latitud", "longitud"],
            "presión-precipitaciónw8_rcxxb": ["tiempo_valido", "tp", "sp", "latitud", "longitud"],
            "temperaturaedviyn5g": ["tiempo_valido", "t2m", "d2m", "latitud", "longitud"],
            "temperaturapf7g_14p": ["tiempo_valido", "stl1", "stl2", "stl3", "stl4", "latitud", "longitud"],
            "snowhy9lgjol": ["tiempo_valido", "nieve", "latitud", "longitud"],
            "suelo-agualxqhzxz9": ["tiempo_valido", "swvl1", "swvl2", "swvl3", "swvl4", "latitud", "longitud"],
            "windeh_9u766": ["tiempo_valido", "u10", "v10", "latitud", "longitud"]
        }

        with engine.begin() as conn:
            for tabla, columnas in tablas_destino.items():
                columnas_existentes = [col for col in columnas if col in df.columns]
                if not columnas_existentes:
                    print(f"⚠️ No se encontraron columnas válidas para {tabla}")
                    continue

                print(f"📦 Actualizando tabla {tabla}...")
                df[columnas_existentes].to_sql(tabla, conn, if_exists="append", index=False)
                print(f"✅ {tabla} actualizada con {len(df)} filas.")

    except Exception as e:
        print(f"❌ Error al cargar o distribuir datos: {e}")

# === MAIN ===
if __name__ == "__main__":
    print("🚀 Iniciando ETL ERA5-Land (modo tabla general + distribución)...")
    fecha_disponible = obtener_ultimo_dia_disponible()
    if fecha_disponible:
        archivo_csv = descargar_datos_csv(fecha_disponible)
        if archivo_csv:
            cargar_y_distribuir(archivo_csv)
    else:
        print("⚠️ No se encontró una fecha disponible.")
    print("🎯 ETL finalizado correctamente.")

