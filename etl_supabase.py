# etl_supabase_v5.py
import os
import glob
import shutil
import gzip
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import pytz
import zipfile
import time

# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

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
        columnas_base = """
    id SERIAL PRIMARY KEY,
    valid_time TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    swvl1 DOUBLE PRECISION,
    swvl2 DOUBLE PRECISION,
    swvl3 DOUBLE PRECISION,
    swvl4 DOUBLE PRECISION,
    stl1 DOUBLE PRECISION,
    stl2 DOUBLE PRECISION,
    stl3 DOUBLE PRECISION,
    stl4 DOUBLE PRECISION,
    number BIGINT,
    expver BIGINT,
    sp DOUBLE PRECISION,
    u10 DOUBLE PRECISION,
    v10 DOUBLE PRECISION,
    t2m DOUBLE PRECISION,
    d2m DOUBLE PRECISION,
    ssrd DOUBLE PRECISION,
    strd DOUBLE PRECISION,
    tp DOUBLE PRECISION,
    skt DOUBLE PRECISION,
    nieve DOUBLE PRECISION,
    snowc DOUBLE PRECISION,
    fecha_actualizacion TEXT
    -- (Sin clave única)

"""

        # Crear la tabla principal
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS reanalysis_era5_land ({columnas_base});"))

        # Crear las tablas secundarias (subconjuntos de datos)
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
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS \"{tabla}\" ({columnas_base});"))

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
                    'area': [13, -89, 13, -89],
                },
                archivo_prueba
            )
            if os.path.getsize(archivo_prueba) < 1024:
                raise ValueError("Archivo NC muy pequeño, posiblemente no válido")
            os.remove(archivo_prueba)
            print(f"✅ Última fecha disponible: {fecha.strftime('%Y-%m-%d')}")
            return fecha
        except Exception as e:
            print(f"⚠️ {fecha.strftime('%Y-%m-%d')} no disponible: {e}")
    print("❌ No se encontró una fecha disponible en los últimos 10 días.")
    return None

# --- DESCARGAR Y CONVERTIR NETCDF A CSV ---
def descargar_datos_csv(fecha, max_reintentos=5):
    año, mes, dia = fecha.year, fecha.month, fecha.day
    archivo_csv = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.csv"

    if os.path.exists(archivo_csv):
        print(f"ℹ️ CSV ya existe: {archivo_csv}")
        return archivo_csv

    reintento = 0
    while reintento < max_reintentos:
        try:
            print(f"🌍 Descargando datos para {año}-{mes:02d}-{dia:02d} (Intento {reintento + 1})...")
            c = cdsapi.Client()

            # Nombre base para múltiples archivos
            archivo_base = f"data_{año}_{mes:02d}_{dia:02d}_"

            c.retrieve(
                'reanalysis-era5-land',
                {
                    'format': 'netcdf',
                    'variable': [
                        "2m_temperature","2m_dewpoint_temperature","surface_pressure",
                        "total_precipitation","surface_solar_radiation_downwards",
                        "surface_thermal_radiation_downwards","skin_temperature",
                        "snowc","volumetric_soil_water_layer_1",
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
                archivo_base + "0.nc"
            )

            # Esperar un momento para que se creen todos los archivos
            time.sleep(5)
            archivos_nc = sorted(glob.glob("data_*.nc"))
            if not archivos_nc:
                raise ValueError("❌ No se descargaron archivos NetCDF válidos")

            datasets = []
            for f in archivos_nc:
                # Descomprimir si es .zip o .gz
                if zipfile.is_zipfile(f):
                    with zipfile.ZipFile(f, 'r') as zip_ref:
                        zip_ref.extractall(".")
                        f = zip_ref.namelist()[0]
                elif f.endswith(".gz"):
                    archivo_descomprimido = f.replace(".gz", "")
                    with gzip.open(f, 'rb') as f_in:
                        with open(archivo_descomprimido, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    f = archivo_descomprimido

                if os.path.getsize(f) < 1024:
                    raise ValueError(f"Archivo NC muy pequeño o inválido: {f}")

                ds = xr.open_dataset(f, engine="netcdf4")
                datasets.append(ds)

            # Merge datasets (variables diferentes)
            ds_total = xr.merge(datasets)
            df = ds_total.to_dataframe().reset_index()

            # Renombrar columnas
            df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
            df.rename(columns={
                "skin_temperature": "skt", "snowc": "nieve",
                "2m_temperature": "t2m", "2m_dewpoint_temperature": "d2m",
                "surface_pressure": "sp",
                "total_precipitation": "tp",
                "surface_solar_radiation_downwards": "ssrd",
                "surface_thermal_radiation_downwards": "strd",
                "volumetric_soil_water_layer_1": "swvl1",
                "volumetric_soil_water_layer_2": "swvl2",
                "volumetric_soil_water_layer_3": "swvl3",
                "volumetric_soil_water_layer_4": "swvl4",
                "soil_temperature_level_1": "stl1",
                "soil_temperature_level_2": "stl2",
                "soil_temperature_level_3": "stl3",
                "soil_temperature_level_4": "stl4",
                "10m_u_component_of_wind": "u10",
                "10m_v_component_of_wind": "v10"
            }, inplace=True)

            df["fecha_actualizacion"] = datetime.now(pytz.UTC)
            df.to_csv(archivo_csv, index=False)
            print(f"✅ CSV generado: {archivo_csv}")

            # Cerrar datasets y limpiar NetCDF
            for ds, f in zip(datasets, archivos_nc):
                ds.close()
                if os.path.exists(f):
                    os.remove(f)

            return archivo_csv

        except Exception as e:
            print(f"❌ Error durante descarga/conversión: {e}")
            for f in glob.glob("data_*.nc"):
                os.remove(f)
            reintento += 1
            if reintento < max_reintentos:
                print(f"🔁 Reintentando descarga... ({reintento}/{max_reintentos})")
                time.sleep(10)
            else:
                print("❌ Se alcanzó el máximo de reintentos. No se pudo generar CSV.")
                return None

# --- CARGAR Y DISTRIBUIR DATOS ---
def cargar_tabla_general(engine, archivo_csv):
    # Leer CSV
    df = pd.read_csv(archivo_csv)
    
    # Columnas esperadas en la tabla
    columnas_esperadas = [
        'valid_time','latitude','longitude','t2m','d2m',
        'stl1','stl2','stl3','stl4',
        'swvl1','swvl2','swvl3','swvl4',
        'u10','v10','skt','nieve','snowc','sp','tp','ssrd','strd',
        'fecha_actualizacion'
    ]
    
    # Crear columnas faltantes y llenarlas con NaN
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = pd.NA  # O np.nan
    
    # Convertir columnas numéricas a float
    columnas_float = [c for c in columnas_esperadas if c not in ['valid_time', 'fecha_actualizacion']]
    for col in columnas_float:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Cargar a la tabla temporal
    df.to_sql('reanalysis_era5_land_temp', engine, if_exists='replace', index=False)
    
    # Insertar en la tabla final con ON CONFLICT
    # Insertar todos los datos (sin conflicto)
    with engine.begin() as conn:
        conn.execute(text(f"""
           INSERT INTO reanalysis_era5_land ({', '.join(columnas_esperadas)})
           SELECT {', '.join(columnas_esperadas)}
           FROM reanalysis_era5_land_temp;
        """))


    print("✅ Datos cargados correctamente.")

def distribuir_datos(engine):
    print("📤 Distribuyendo datos a tablas secundarias...")

    tablas = {
        "pressure-precipitationw8_rcxxb": ["valid_time", "sp", "tp", "latitude", "longitude"],
        "radiation-heatcpg03hs6": ["valid_time", "ssrd", "strd", "latitude", "longitude"],
        "skin-temperaturehke46ner": ["valid_time", "skt", "latitude", "longitude"],
        "snowhy9lgjol": ["valid_time", "snowc", "nieve", "latitude", "longitude"],
        "soil-waterlxqhzxz9": ["valid_time", "swvl1", "swvl2", "swvl3", "swvl4", "latitude", "longitude"],
        "temperatureedviyn5g": ["valid_time", "d2m", "t2m", "latitude", "longitude"],
        "temperaturepf7g_14p": ["valid_time", "stl1", "stl2", "stl3", "stl4", "latitude", "longitude"],
        "windeh_9u766": ["valid_time", "u10", "v10", "latitude", "longitude"]
    }

    with engine.begin() as conn:
        for tabla, cols in tablas.items():
            # Columnas que se insertarán
            insert_cols = cols + ["fecha_actualizacion"]
            select_cols = ", ".join(insert_cols)

            query = f"""
                INSERT INTO "{tabla}" ({', '.join(insert_cols)})
                SELECT {select_cols}
                FROM reanalysis_era5_land
                WHERE {cols[1]} IS NOT NULL;
            """

            try:
                conn.execute(text(query))
                print(f"✅ Datos copiados correctamente en {tabla}.")
            except Exception as e:
                print(f"⚠️ Error copiando datos en {tabla}: {e}")

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



