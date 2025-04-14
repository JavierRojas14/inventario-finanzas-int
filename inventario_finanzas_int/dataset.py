from pathlib import Path

import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

import inventario_finanzas_int.funciones_auxiliares as fa
from inventario_finanzas_int.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()


def procesar_mobiliarios(ruta_mobiliario):
    # Lee todas las hojas de mobiliarios
    df_mobiliario = pd.read_excel(ruta_mobiliario, sheet_name=None)

    # Procede a leer todas las hojas del Excel
    dfs = []
    for unidad, mobiliario in df_mobiliario.items():
        if unidad == "Esterilizacion":
            mobiliario.columns = mobiliario.iloc[3]
            mobiliario = mobiliario[4:]

        # Limpia el nombre de las columnas
        df_unidad_mobiliario_limpio = fa.clean_column_names(mobiliario)
        df_unidad_mobiliario_limpio["unidad"] = unidad

        # Cambia de nombre las columnas
        df_unidad_mobiliario_limpio = df_unidad_mobiliario_limpio.rename(
            columns={
                "correlativo_asignado": "correlativo_antiguo",
                "observaciones": "observacion",
                "correlativo_2025": "correlativo_antiguo",
            }
        )

        # Deja solamente las columnas de interes
        columnas_interes = [
            "correlativo_antiguo",
            "bien",
            "marca",
            "modelo",
            "serie",
            "tipo",
            "unidadservicio_clinico",
            "ubicacion_unidad",
            "propiedad",
            "observacion",
            "unidad",
            "piso",
        ]
        df_unidad_mobiliario_limpio = df_unidad_mobiliario_limpio[columnas_interes]

        dfs.append(df_unidad_mobiliario_limpio)

    # Une todas las unidades
    df_mobiliario_limpio = pd.concat(dfs)

    # Llena los NaNs
    df_mobiliario_limpio = df_mobiliario_limpio.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df_mobiliario_limpio.drop(columns="piso").columns
    df_mobiliario_limpio[columnas_texto] = df_mobiliario_limpio[columnas_texto].apply(
        fa.limpiar_columna_texto
    )

    # Filtra solamente los bienes que son del hospital
    mask_propiedades_validas = df_mobiliario_limpio["propiedad"].isin(
        [
            "DONACION",
            "FUNCIONARIO(IMPRIMIR ETIQUETA)",
            "INT",
            "INT(IMPRIMIR ETIQUETA)",
            "COMODATO",
        ]
    )
    df_mobiliario_limpio = df_mobiliario_limpio[mask_propiedades_validas].copy()

    # Indica el tipo de bien
    df_mobiliario_limpio["tipo_bien"] = "MOBILIARIO"

    return df_mobiliario_limpio


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR,
    # ----------------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset...")

    # Procesa los mobiliarios
    ruta_mobiliarios = (
        input_path
        / "MOBILIARIO (ARIEL)/PLANILLA REGISTRO DE INVENTARIO BIENES DE USO 2025 06022025.xlsx"
    )
    output_mobiliarios = output_path / "df_procesada_mobiliarios.csv"

    # Lee y exporta mobiliarios
    df_mobiliario = procesar_mobiliarios(ruta_mobiliarios)
    df_mobiliario.to_csv(output_mobiliarios, index=False)

    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
