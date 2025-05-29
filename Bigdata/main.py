import os
import pandas as pd
import logging
from datetime import datetime
from prefect import flow, task

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Rutas de archivos (en entorno local, se ignoran en Prefect Cloud)
RAW_EVALS = "data/raw/evaluaciones.csv"
RAW_ESTUD = "data/reference/estudiantes.csv"
OUTPUT_DIR = "data/processed/resumenes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@task(retries=2, retry_delay_seconds=5)
def cargar_evaluaciones():
    logging.info("📥 Cargando archivo de evaluaciones...")
    return pd.read_csv(RAW_EVALS, encoding="utf-8")

@task(retries=2, retry_delay_seconds=5)
def cargar_estudiantes():
    logging.info("📥 Cargando archivo de estudiantes...")
    return pd.read_csv(RAW_ESTUD, encoding="utf-8")

@task
def procesar_datos(evaluaciones, estudiantes):
    logging.info("🔄 Combinando datasets...")
    df = pd.merge(evaluaciones, estudiantes, on="id_estudiante", how="inner")
    df["duracion_minutos"] = df["duracion_segundos"] / 60
    df["estado"] = df["promedio_final"].apply(lambda x: "Aprobado" if x >= 13 else "Desaprobado")
    logging.info(f"✅ Procesados {len(df)} estudiantes.")
    logging.info(f"\n📊 Vista previa del resultado:\n{df[['id_estudiante', 'promedio_final', 'estado']].head().to_string(index=False)}")
    return df

@task
def exportar(df):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = f"{OUTPUT_DIR}/reporte_estudiantes_{timestamp}.csv"
    
    # Puedes comentar esta línea si solo estás usando Prefect Cloud
    # df.to_csv(out_path, index=False)

    logging.info(f"📤 Simulando exportación del archivo: {out_path}")
    return out_path

@flow(name="EvaluacionEstudiantil")
def pipeline_estudiantes():
    logging.info("🚀 Iniciando flujo de evaluación estudiantil...")
    ev = cargar_evaluaciones()
    es = cargar_estudiantes()
    df = procesar_datos(ev, es)
    exportar(df)
    logging.info("🎯 Flujo completado exitosamente.")

# Solo se ejecuta localmente si corres el script directamente
if __name__ == "__main__":
    pipeline_estudiantes()
