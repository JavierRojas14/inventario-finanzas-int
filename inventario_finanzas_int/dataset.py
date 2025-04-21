from pathlib import Path

import numpy as np
import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

import inventario_finanzas_int.funciones_auxiliares as fa
from inventario_finanzas_int.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

CAMBIOS_UNIDAD_MOBILIARIOS = {
    "MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "LAVORATORIO": "LABORATORIO",
    "ALIVIO DEL DOLOR- CUIDADOR PALIATIVOS": "CUIDADOS PALIATIVOS",
    "FARMACIA HOPITALIZADOS": "FARMACIA HOSPITALIZADO",
    "UCI 5 NORTE": "UPC 5 NORTE",
    "UPC": "RESIDENCIA 7 PISO",
    "UTI NORTE": "UTI 4 NORTE",
    "UTI SUR": "UTI 4 SUR",
}

CAMBIOS_UNIDAD_EQUIPOS_MEDICOS = {
    "UPC 5TO NORTE": "UPC 5 NORTE",
    "UTI 4TO SUR": "UTI 4 SUR",
    "MEDICO QUIRURGICO 3RO SUR": "MQ 3 SUR",
    "MEDICO QUIRURGICO 3RO NORTE": "MQ 3 NORTE",
    "UTI 3RO NORTE": "UTI 3 NORTE",
    "UTI 4TO NORTE": "UTI 4 NORTE",
    "CIRUGIA TORAX": "CIRUGIA DE TORAX",
}

CAMBIOS_UNIDAD_EQUIPOS_INDUSTRIALES = {
    "SMQ SUR": "MQ 3 SUR",
    "SMQ NORTE": "MQ 3 NORTE",
    "UCI UTI 5 NORTE": "UPC 5 NORTE",
    "KINESIOLOGIA": "MEDICINA FISICA Y REHABILITACION",
    "4 MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "CONGENITO": "CONGENITOS",
}


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

    # Indica el tipo de bien
    df_mobiliario_limpio["tipo_bien"] = "MOBILIARIO"

    # Limpia las unidades
    df_mobiliario_limpio["unidadservicio_clinico"] = df_mobiliario_limpio[
        "unidadservicio_clinico"
    ].replace(CAMBIOS_UNIDAD_MOBILIARIOS)

    return df_mobiliario_limpio


def procesar_equipos_medicos(ruta_equipos):
    df_equipos_medicos = pd.read_excel(ruta_equipos, header=1, sheet_name=None)

    dfs = []
    for unidad_equipo, df_equipo_unidad in df_equipos_medicos.items():
        df_equipo_unidad = fa.clean_column_names(df_equipo_unidad)
        df_equipo_unidad["unidad_hoja"] = unidad_equipo

        # Cambia la hoja donde le ponen "Ubicacion"
        df_equipo_unidad = df_equipo_unidad.rename(columns={"ubicacion": "recinto"})

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
            "n_inventario": "correlativo_antiguo",
            "propio_arriendo_comodato": "propiedad",
        }
    )

    # Cambia glosas del nombre de la unidad
    df_final["unidadservicio_clinico"] = df_final["unidadservicio_clinico"].replace(
        CAMBIOS_UNIDAD_EQUIPOS_MEDICOS
    )

    # Elimina columnas innecesarias
    df_final = df_final.drop(columns=["n_inventario_2025"])

    return df_final


def procesar_equipos_industriales(ruta_industriales):
    df = pd.read_excel(ruta_industriales)

    # Limpia el nombre de las columnas
    df = fa.clean_column_names(df)
    df = df.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df.drop(columns="piso").columns
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Elimina columna innecesaria
    df = df.drop(columns=["correlativo_asignado", "ano_egreso"])

    # Renombra columnas
    df = df.rename(
        columns={
            "n_inventario_definido_2025": "correlativo_antiguo",
            "tipo": "propiedad",
            "observaciones": "observacion",
        }
    )

    # Agrega el tipo de bien
    df["tipo_bien"] = "EQUIPO INDUSTRIAL"

    # Renombra glosas del servicio
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(
        CAMBIOS_UNIDAD_EQUIPOS_INDUSTRIALES
    )

    return df


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
