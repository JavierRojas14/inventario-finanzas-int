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
        df_unidad_mobiliario_limpio["unidad_hoja"] = unidad

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
            "unidad_hoja",
            "piso",
        ]
        df_unidad_mobiliario_limpio = df_unidad_mobiliario_limpio[columnas_interes]

        dfs.append(df_unidad_mobiliario_limpio)

    # Une todas las unidades
    df_mobiliario_limpio = pd.concat(dfs)

    # Elimina los registros que NO tengan un nombre del bien
    df_mobiliario_limpio = df_mobiliario_limpio.dropna(subset="bien")

    # Llena los NaNs
    df_mobiliario_limpio = df_mobiliario_limpio.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df_mobiliario_limpio.drop(columns="piso").columns
    df_mobiliario_limpio[columnas_texto] = df_mobiliario_limpio[columnas_texto].apply(
        fa.limpiar_columna_texto
    )

    # # Filtra solamente los bienes que son del hospital
    # mask_propiedades_validas = df_mobiliario_limpio["propiedad"].isin(
    #     [
    #         "DONACION",
    #         "FUNCIONARIO(IMPRIMIR ETIQUETA)",
    #         "INT",
    #         "INT(IMPRIMIR ETIQUETA)",
    #         "COMODATO",
    #     ]
    # )
    # df_mobiliario_limpio = df_mobiliario_limpio[mask_propiedades_validas].copy()

    # Indica el tipo de bien
    df_mobiliario_limpio["tipo_bien"] = "MOBILIARIO"

    return df_mobiliario_limpio


def procesar_equipos_medicos(ruta_equipos):
    df_equipos_medicos = pd.read_excel(ruta_equipos, header=1, sheet_name=None)

    dfs = []
    for unidad_equipo, df_equipo_unidad in df_equipos_medicos.items():
        df_equipo_unidad = fa.clean_column_names(df_equipo_unidad)
        df_equipo_unidad["unidad_hoja"] = unidad_equipo

        dfs.append(df_equipo_unidad)

    # Une todas las unidades
    df_final = pd.concat(dfs).dropna(subset="nombre_equipo")
    df_final = df_final.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df_final.drop(columns="piso").columns
    df_final[columnas_texto] = df_final[columnas_texto].apply(fa.limpiar_columna_texto)

    # Agrega el tipo de bien
    df_final["tipo_bien"] = "EQUIPO MEDICO"

    # Renombra las columnas para que sean las mismas que las otras planillas
    df_final = df_final.rename(
        columns={
            "recinto": "ubicacion_unidad",
            "servicio_clinico": "unidadservicio_clinico",
            "nombre_equipo": "bien",
        }
    )

    return df_final


def procesar_equipos_industriales(ruta_industriales):
    df_industriales = pd.read_excel(ruta_industriales)

    # Limpia el nombre de las columnas
    df_industriales_limpio = fa.clean_column_names(df_industriales)
    df_industriales_limpio = df_industriales_limpio.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df_industriales_limpio.drop(columns="piso").columns
    df_industriales_limpio[columnas_texto] = df_industriales_limpio[columnas_texto].apply(
        fa.limpiar_columna_texto
    )

    return df_industriales_limpio


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR,
    # ----------------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset...")

    # Define rutas input
    ruta_mobiliarios = (
        input_path
        / "MOBILIARIO (ARIEL)/PLANILLA REGISTRO DE INVENTARIO BIENES DE USO 2025 06022025.xlsx"
    )
    ruta_equipos = (
        input_path / "EQUIPOS MEDICOS (RODRIGO)/CONSOLIDADO EQ. MEDICOS REVISADO POR RODRIGO.xlsx"
    )
    ruta_industriales = (
        input_path
        / "EQUIPOS INDUSTRIALES Y DE OFICINA (ALEJANDRO)/CONSOLIDADO BIENES INDUSTRIALES Y DE OFICINA.xlsx"
    )

    # Define rutas output
    output_mobiliarios = output_path / "df_procesada_mobiliarios.csv"
    output_equipos = output_path / "df_procesada_equipos_medicos.csv"
    output_industriales = output_path / "df_procesada_industriales.csv"

    # Lee y exporta diversos bienes
    df_mobiliario = procesar_mobiliarios(ruta_mobiliarios)
    df_equipos = procesar_equipos_medicos(ruta_equipos)
    df_industriales = procesar_equipos_industriales(ruta_industriales)

    df_mobiliario.to_csv(output_mobiliarios, index=False)
    df_equipos.to_csv(output_equipos, index=False)
    df_industriales.to_csv(output_industriales, index=False)
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
