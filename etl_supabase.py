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

# --- CONFIGURACIÓN ERA5-LAND ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "TU_CDSAPI_KEY"

# --- CONFIGURACIÓN SUPABASE ---
os.environ["DB_USER"] = "TU_USUARIO"
os.environ["DB_PASSWORD"] = "TU_CONTRASEÑA"
os.environ["DB_HOST"] = "TU_HOST"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# --- CREAR .cdsapirc ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ['CDSAPI_URL']}\nkey: {os.environ['CDSAPI_KEY']}\n")

# --- LIMPIAR ARCHIVOS TEMPORALES ---
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

    # Forzar regeneración del CSV si existe
    if os.path.exists(archivo_csv):
        print(f"ℹ️ CSV existe, se volverá a generar: {archivo_csv}")
        os.remove(archivo_csv)

    print(f"🌍 Descargando datos ERA5-Land para {año}-{mes:02d}-{dia:02d}...")
    c = cdsapi.Client()

    c.retrieve(
        'reanalysis-era5-land',
        {
            'format': 'netcdf',
            'variable': [
                "2m_temperature", "2m_dewpoint_temperature",
                "surface_pressure", "total_precipitation",
                "surface_solar_radiation_downwards", "surface_thermal_radiation_downwards",
                "skin_temperature", "snow_cover",
                "volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2",
                "volumetric_soil_water_layer_3", "volumetric_soil_water_layer_4",
                "soil_temperature_level_1", "soil_temperature_level_2",
                "soil_temperature_level_3", "soil_temperature_level_4",
                "10m_u_component_of_wind", "10m_v_component_of_wind"
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

    # Descomprimir ZIP o GZ si aplica
    if zipfile.is_zipfile(archivo_nc):
        print("🗜️ Descomprimiendo archivo ZIP...")
        with zipfile.ZipFile(archivo_nc, 'r') as zip_ref:
            zip_ref.extractall(".")
            archivo_nc = zip_ref.namelist()[0]
        os.remove(archivo_nc)

    elif archivo_nc.endswith(".gz"):
        archivo_descomprimido = archivo_nc.replace(".gz", "")
        with gzip.open(archivo_nc, 'rb') as f_in, open(archivo_descomprimido, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(archivo_nc)
        archivo_nc = archivo_descomprimido

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

# --- CARGAR CSV CON UPSERT SOLO PARA EL DÍA DESCARGADO ---
def cargar_a_supabase(archivo_csv, fecha_disponible):
    if not archivo_csv or not os.path.exists(archivo_csv):
        print("⚠️ No hay archivo CSV válido para cargar.")
        return

    print(f"📤 Cargando datos desde {archivo_csv} a las tablas correspondientes...")
    df = pd.read_csv(archivo_csv)
    df['valid_time'] = pd.to_datetime(df['valid_time'])

    # Filtrar solo los datos del día descargado
    df = df[df['valid_time'].dt.date == fecha_disponible.date()]

    if df.empty:
        print("⚠️ No hay datos para el día descargado. Nada que cargar.")
        return

    engine = crear_engine()

    tablas = {
        "pressure-precipitationw8_rcxxb": ["valid_time", "sp", "tp", "latitude", "longitude"],
        "radiation-heatcpg03hs6": ["valid_time", "ssrd", "strd", "latitude", "longitude"],
        "skin-temperaturehke46ner": ["valid_time", "skt", "latitude", "longitude"],
        "snowhy9lgjol": ["valid_time", "nieve", "latitude", "longitude"],
        "soil-waterlxqhzxz9": ["valid_time", "swvl1", "swvl2", "swvl3", "swvl4", "latitude", "longitude"],
        "temperatureedviyn5g": ["valid_time", "d2m", "t2m", "latitude", "longitude"],
        "temperaturepf7g_14p": ["valid_time", "stl1", "stl2", "stl3", "stl4", "latitude", "longitude"],
        "windeh_9u766": ["valid_time", "u10", "v10", "latitude", "longitude"],
    }

    for tabla, columnas in tablas.items():
        columnas_validas = [col for col in columnas if col in df.columns]
        if columnas_validas:
            sub_df = df[columnas_validas].dropna(how="all")
            if not sub_df.empty:
                with engine.begin() as conn:
                    # Crear tabla temporal
                    sub_df.to_sql(f"{tabla}_temp", conn, if_exists="replace", index=False)
                    cols_insert = ", ".join(columnas_validas)
                    cols_update = ", ".join([f"{c} = EXCLUDED.{c}" for c in columnas_validas if c not in ["valid_time", "latitude", "longitude"]])
                    if cols_update:
                        upsert_sql = text(f"""
                            INSERT INTO "{tabla}" ({cols_insert})
                            SELECT {cols_insert} FROM "{tabla}_temp"
                            ON CONFLICT (valid_time, latitude, longitude)
                            DO UPDATE SET {cols_update};
                            DROP TABLE "{tabla}_temp";
                        """)
                    else:
                        upsert_sql = text(f"""
                            INSERT INTO "{tabla}" ({cols_insert})
                            SELECT {cols_insert} FROM "{tabla}_temp"
                            ON CONFLICT (valid_time, latitude, longitude)
                            DO NOTHING;
                            DROP TABLE "{tabla}_temp";
                        """)
                    conn.execute(upsert_sql)
                    print(f"✅ {tabla}: {len(sub_df)} filas insertadas o actualizadas.")
            else:
                print(f"⚠️ {tabla}: sin datos válidos para insertar.")
        else:
            print(f"⚠️ {tabla}: columnas no encontradas en el dataset.")


# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL ERA5-Land (descarga y carga en tablas)...")
    fecha_disponible = obtener_ultimo_dia_disponible()
    if fecha_disponible:
        archivo_csv = descargar_datos_csv(fecha_disponible)
        cargar_a_supabase(archivo_csv, fecha_disponible)  # ✅ CORRECTO
    else:
        print("⚠️ No se encontró una fecha con datos disponibles.")
    print("🎯 ETL finalizado correctamente.")

