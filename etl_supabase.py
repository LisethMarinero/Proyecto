# etl_supabase_v6.py
import os
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import pytz

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
def descargar_datos_csv(fecha):
    año, mes, dia = fecha.year, fecha.month, fecha.day
    archivo_csv = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.csv"
    if os.path.exists(archivo_csv):
        print(f"ℹ️ CSV ya existe: {archivo_csv}")
        return archivo_csv

    c = cdsapi.Client()
    archivo_nc = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.nc"

    print(f"🌍 Descargando datos para {año}-{mes:02d}-{dia:02d}...")
    c.retrieve(
        'reanalysis-era5-land',
        {
            'format': 'netcdf',
            'variable': [
                "2m_temperature","2m_dewpoint_temperature","surface_pressure",
                "total_precipitation","surface_solar_radiation_downwards",
                "surface_thermal_radiation_downwards","skin_temperature",
                "snowc","volumetric_soil_water_layer_1","volumetric_soil_water_layer_2",
                "volumetric_soil_water_layer_3","volumetric_soil_water_layer_4",
                "soil_temperature_level_1","soil_temperature_level_2",
                "soil_temperature_level_3","soil_temperature_level_4",
                "10m_u_component_of_wind","10m_v_component_of_wind"
            ],
            'year': [str(año)],
            'month': [f"{mes:02d}"],
            'day': [f"{dia:02d}"],
            'time': ['00:00'],
            'area': [13, -89, 13, -89],
        },
        archivo_nc
    )

    ds = xr.open_dataset(archivo_nc, engine="netcdf4")
    df = ds.to_dataframe().reset_index()
    ds.close()
    os.remove(archivo_nc)

    # Renombrar columnas
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    df.rename(columns={
        "skin_temperature": "skt",
        "snowc": "nieve",
        "2m_temperature": "t2m",
        "2m_dewpoint_temperature": "d2m",
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
    return archivo_csv

# --- CARGAR TABLA GENERAL ---
def cargar_tabla_general(engine, archivo_csv):
    df = pd.read_csv(archivo_csv)
    df.to_sql('reanalysis_era5_land', engine, if_exists='append', index=False, chunksize=5000)
    print("✅ Datos cargados correctamente en la tabla principal.")

# --- DISTRIBUIR DATOS ---
def distribuir_datos(engine):
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
            insert_cols = cols + ["fecha_actualizacion"]
            select_cols = ", ".join(insert_cols)
            query = f"""
                INSERT INTO "{tabla}" ({', '.join(insert_cols)})
                SELECT {select_cols}
                FROM reanalysis_era5_land
                WHERE {cols[1]} IS NOT NULL;
            """
            conn.execute(text(query))
            print(f"✅ Datos distribuidos en {tabla}.")

# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL optimizado ERA5-Land...")
    engine = crear_engine()
    fecha = obtener_ultimo_dia_disponible()
    if fecha:
        archivo_csv = descargar_datos_csv(fecha)
        if archivo_csv:
            cargar_tabla_general(engine, archivo_csv)
            distribuir_datos(engine)
    print("🎯 ETL finalizado correctamente.")
