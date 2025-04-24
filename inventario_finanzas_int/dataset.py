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
    "UTIM NORTE": "UTIM 4 NORTE",
}

CAMBIO_PROPIEDAD_MOBILIARIOS = {
    "FUNCIONARIOS": "FUNCIONARIO",
    "U DE CHILE": "U. DE CHILE",
    "EMPRESE EXTERNA": "EMPRESA EXTERNA",
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

CAMBIOS_PROPIEDAD_EQUIPOS_MEDICOS = {
    "PROPIO": "INT",
}

CAMBIOS_UNIDAD_EQUIPOS_INDUSTRIALES = {
    "SMQ SUR": "MQ 3 SUR",
    "SMQ NORTE": "MQ 3 NORTE",
    "UCI UTI 5 NORTE": "UPC 5 NORTE",
    "KINESIOLOGIA": "MEDICINA FISICA Y REHABILITACION",
    "4 MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "CONGENITO": "CONGENITOS",
    "ECORCADIOGRAFIA": "ECOCARDIOGRAFIA",
    "RESIDENCIA /GIMNACIO": "RESIDENCIA/GIMNASIO",
}

CAMBIO_PROPIEDAD_INDUSTRIALES = {
    "U CHILE": "U. DE CHILE",
}

CAMBIO_PROPIEDAD_INDUSTRIALES_NUEVOS = {
    "FUNCIONARIOS": "FUNCIONARIO",
    "FUNCIOINARIOS": "FUNCIONARIO",
    "FUNCIONARIOS EXTERNOS": "FUNCIONARIO EXTERNO",
}

CAMBIOS_UNIDAD_EQUIPOS_INFORMATICOS = {
    "ALIVIO DEL DOLOR Y CUIDADOS PALIATIVOS": "CUIDADOS PALIATIVOS",
    "CUIDADOS PALEATIVOS": "CUIDADOS PALIATIVOS",
    "ANATOMIA P": "ANATOMIA PATOLOGICA",
    "ANATOMIA PATOLOGIA": "ANATOMIA PATOLOGICA",
    "ARCHIVOS": "ARCHIVO",
    "CARDIOCRUGIA": "CARDIOCIRUGIA",
    "CARDIOPATIAS CONGENITAS": "CONGENITOS",
    "CARDIOCIRUGIA": "MQ 4 CARDIOVASCULAR",
    "CIRUGIA CARDIACA": "MQ 4 CARDIOVASCULAR",
    "CIRUGIA CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "SMQCV": "MQ 4 CARDIOVASCULAR",
    "MQ": "MQ 4 CARDIOVASCULAR",
    "MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "CONSULTORIO -ADMISION": "ADMISION",
    "DEPTO. REHABILITACION": "MEDICINA FISICA Y REHABILITACION",
    "ECOCARDIOGRAMA": "ECOCARDIOGRAFIA",
    "ESTERILIZACION AREA LIMPIA": "ESTERILIZACION",
    "FARMACIA AMBULATORIO": "FARMACIA AMBULATORIA",
    "FARMACIA HOSPITALIZADOS": "FARMACIA HOSPITALIZADO",
    "HEMODINMIA": "HEMODINAMIA",
    "HISTOTECNIA": "ANATOMIA PATOLOGICA",
    "JEFATURA FARMACIA": "FARMACIA AMBULATORIA",
    "CAMAS MEDIAS 3RO NORTE": "CAMAS MEDIAS 3 NORTE",
    "CAMAS MEDIAS 3RO SUR": "CAMAS MEDIAS 3 SUR",
    "SMQR": "MQ 3 SUR",
    "UTI 4TO SUR": "UTI 4 SUR",
    "UCI-UPC": "UPC 5 CENTRO",
    "CAMAS MEDIAS 4TO NORTE": "CAMAS MEDIAS 4 NORTE",
    "LABORATORIO TUBERCULOCIS": "LABORATORIO TUBERCULOSIS",
    "UTI 3ERO NORTE": "UTI 3 NORTE",
    "UTI 4TO NORTE": "UTI 4 NORTE",
    "CONGENITO": "CONGENITOS",
    "KINESIOLOGIA": "MEDICINA FISICA Y REHABILITACION",
}


def procesar_mobiliarios(ruta_mobiliario):
    # Lee todas las hojas de mobiliarios
    df = pd.read_excel(ruta_mobiliario, sheet_name=None)

    # Procede a leer todas las hojas del Excel
    dfs = []
    for unidad, mobiliario in df.items():
        if unidad == "Esterilizacion":
            mobiliario.columns = mobiliario.iloc[3]
            mobiliario = mobiliario[4:]

        # Limpia el nombre de las columnas
        df_unidad = fa.clean_column_names(mobiliario)
        df_unidad["unidad_hoja"] = unidad

        # Cambia de nombre las columnas
        df_unidad = df_unidad.rename(
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
        df_unidad = df_unidad[columnas_interes]

        # Elimina registros sin bien
        df_unidad = df_unidad.dropna(subset=["bien"])

        # Extiende los registros del servicio, unidad y piso
        df_unidad["unidadservicio_clinico"] = df_unidad["unidadservicio_clinico"].ffill()
        df_unidad["ubicacion_unidad"] = df_unidad["ubicacion_unidad"].ffill()
        df_unidad["piso"] = df_unidad["piso"].ffill()

        dfs.append(df_unidad)

    # Une todas las unidades
    df = pd.concat(dfs)

    # Llena los NaNs
    df = df.fillna("")

    # Limpia las columnas de texto
    columnas_texto = df.drop(columns="piso").columns
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Indica el tipo de bien
    df["tipo_bien"] = "MOBILIARIO"

    # Limpia las unidades
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(CAMBIOS_UNIDAD_MOBILIARIOS)

    # Limpia la propiedad
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_MOBILIARIOS)

    # Elimina columnas innecesarias
    df = df.drop(columns="tipo")

    return df


def procesar_mobiliario_consolidado(ruta):
    # Lee archivo
    df = pd.read_excel(ruta)

    # Limpia el nombre de las columnas
    df = fa.clean_column_names(df)

    # Limpia columnas de texto
    columnas_texto = df.drop(columns="piso").columns


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

    # Cambia propiedades
    df_final["propiedad"] = df_final["propiedad"].replace(CAMBIOS_PROPIEDAD_EQUIPOS_MEDICOS)

    # Elimina columnas innecesarias
    df_final = df_final.drop(columns=["n_inventario_2025"])

    # Ordena los registros
    df_final = df_final.sort_values(["piso", "unidadservicio_clinico", "ubicacion_unidad"])

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

    # Cambia la propiedad
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_INDUSTRIALES)

    # Ordena los registros
    df = df.sort_values(["piso", "unidadservicio_clinico", "ubicacion_unidad"])

    return df


def procesar_equipos_industriales_nuevos(ruta):
    # Lee el archivo
    df = pd.read_excel(ruta)

    # Limpia las columnas
    df = fa.clean_column_names(df)

    # Limpia columnas de texto
    columnas_texto = [
        "bien",
        "marca",
        "tipo",
        "unidadservicio_clinico",
        "ubicacion_unidad",
        "observaciones",
    ]
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Elimina columna innecesaria
    df = df.drop(columns=["correlativo_asignado", "ano_egreso", "vida_util"])

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

    # Cambia el tipo de propiedad
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_INDUSTRIALES_NUEVOS)

    # Cambio la unidad
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(
        CAMBIOS_UNIDAD_EQUIPOS_INDUSTRIALES
    )

    return df


def procesar_equipos_informaticos(ruta_archivo):
    # Lee el archivo
    df = pd.read_excel(ruta_archivo, sheet_name=None, header=4)

    dfs = {}
    for nombre_hoja, df_tipo_equipo in df.items():
        # Si el equipo NO se tiene que dar de baja
        if nombre_hoja != "EQUIPOS SIN DATOS":

            # Limpia los nombres de columnas
            df_unidad = fa.clean_column_names(df_tipo_equipo)

            # Renombra columnas
            df_unidad = df_unidad.rename(
                columns={
                    # Cambio de las columnas codigo correlativos
                    "cod_int": "correlativo_antiguo",
                    "cod_institucional": "correlativo_antiguo",
                    "codint": "correlativo_antiguo",
                    "codinstitucional": "correlativo_antiguo",
                    # Cambios de la columna Serie
                    "n_de_serie": "serie",
                    "n_serie_enero": "serie",
                    "nserie": "serie",
                    # Cambios del servicio
                    "departamento": "unidadservicio_clinico",
                    "unidad": "unidadservicio_clinico",
                    "servicio": "unidadservicio_clinico",
                    # Cambios de la columna Unidad
                    "ubicacion": "ubicacion_unidad",
                    # Cambios de la columna propiedad
                    "estado_de_propiedad": "propiedad",
                }
            )
            # Agrega el nombre del bien
            df_unidad["bien"] = nombre_hoja

            # Agrega DataFrame de unidad a todos
            dfs[nombre_hoja] = df_unidad

    # Concatena todos los bienes
    df = pd.concat(dfs.values())

    # Solamente deja las columnas utiles
    df = df[
        [
            "correlativo_antiguo",
            "bien",
            "marca",
            "modelo",
            "serie",
            "unidadservicio_clinico",
            "ubicacion_unidad",
            "propiedad",
            "piso",
        ]
    ]

    # Agrega el tipo de bien
    df["tipo_bien"] = "INFORMATICO"

    # Limpia columnas de texto
    columnas_texto = [
        "correlativo_antiguo",
        "bien",
        "marca",
        "modelo",
        "unidadservicio_clinico",
        "ubicacion_unidad",
        "propiedad",
    ]
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Elimina registros sin diversas columnas
    df = df.dropna(
        subset=[
            "correlativo_antiguo",
            "marca",
            "modelo",
            "serie",
            "unidadservicio_clinico",
            "ubicacion_unidad",
            "propiedad",
            "piso",
        ],
        how="all",
    )

    # Cambia glosas del servicio
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(
        CAMBIOS_UNIDAD_EQUIPOS_INFORMATICOS
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
    ruta_industriales_nuevo = (
        input_path
        / "EQUIPOS INDUSTRIALES Y DE OFICINA (ALEJANDRO)/PLANILLA REGISTRO DE INVENTARIO BIENES DE USO 2025.xlsx"
    )
    ruta_informaticos = (
        input_path
        / "EQUIPOS INFORMATICOS (PAOLA-ANDRES)/CONSOLIDADO INVENTARIO INFORMATICA 17032025 .xlsx"
    )

    # Define rutas output
    output_mobiliarios = output_path / "df_procesada_mobiliarios.csv"
    output_equipos = output_path / "df_procesada_equipos_medicos.csv"
    output_industriales = output_path / "df_procesada_industriales.csv"
    output_industriales_nuevos = output_path / "df_procesada_industriales.csv"
    output_informaticos = output_path / "df_procesada_informaticos.csv"

    # Lee y exporta diversos bienes
    df_mobiliario = procesar_mobiliarios(ruta_mobiliarios)
    df_equipos = procesar_equipos_medicos(ruta_equipos)
    # df_industriales = procesar_equipos_industriales(ruta_industriales)
    df_industriales_nuevos = procesar_equipos_industriales_nuevos(ruta_industriales_nuevo)
    df_informaticos = procesar_equipos_informaticos(ruta_informaticos)

    df_mobiliario.to_csv(output_mobiliarios, index=False)
    df_equipos.to_csv(output_equipos, index=False)
    # df_industriales.to_csv(output_industriales, index=False)
    df_industriales_nuevos.to_csv(output_industriales_nuevos, index=False)
    df_informaticos.to_csv(output_informaticos, index=False)
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
