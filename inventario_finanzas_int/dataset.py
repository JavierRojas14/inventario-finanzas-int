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
    "ALIVIO DEL DOLOR-PALIATIVOS": "CUIDADOS PALIATIVOS",
    "UTI NORTE": "UTI 4 NORTE",
    "FISIOPATOLOGIA": "FUNCION PULMONAR",
    "KINESIOLOGIA": "MEDICINA FISICA Y REHABILITACION",
    "MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "NUTRICION": "NUTRICION/ALIMENTACION",
    "PASILLO Y ENTRE ACSENSORES": "PASILLO Y ENTRE ASCENSORES",
    "REHABILITACION CARDIOPULMONAR": "MEDICINA FISICA Y REHABILITACION",
    # "FARMACIA HOPITALIZADOS": "FARMACIA HOSPITALIZADO",
    # "UCI 5 NORTE": "UPC 5 NORTE",
    # "UPC": "RESIDENCIA 7 PISO",
    # "UTI NORTE": "UTI 4 NORTE",
    # "UTI SUR": "UTI 4 SUR",
    # "UTIM NORTE": "UTIM 4 NORTE",
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
    "ECORCADIOGRAFIA": "ECOCARDIOGRAFIA",
    "RESIDENCIA /GIMNACIO": "RESIDENCIA/GIMNASIO",
}

CAMBIO_UNIDAD_INFORMATICO = {
    "UTIM 3ERO NORTE": "UTIM 3 NORTE",
    "UTIM 3RO NORTE": "UTIM 3 NORTE",
    "UTIM NORTE": "UTIM 3 NORTE",
    "UTI-1 NORTE": "UTI 4 NORTE",
    "ECOCARDIOGRAMA": "ECOCARDIOGRAFIA",
    "BOX 1 POLICLINICO": "CONSULTORIO EXTERNO",
    "CONGENITO": "CONGENITOS",
    "INMUNOLOGIA": "LABORATORIO",
    "MICROBIOLOGIA": "LABORATORIO",
    "MQ 4TO NORTE": "MQ 4 NORTE",
    "MQ CARDIOVASCULAR": "MQ 4 CARDIOVASCULAR",
    "FARMACIA": "UNIDAD DE APOYO FARMACIA",
    "UCI": "UCI 5",
    "UCI NORTE": "UCI 5 NORTE",
    "UNIDAD PACIENTE CRITICO": "UPC 3",
    "UTI": "UTI 4",
}

CAMBIO_PROPIEDAD_MOBILIARIOS = {
    "FUNCIONARIOS": "PROPIEDAD DEL FUNCIONARIO",
    "DONACION": "INT (DONACION)",
    "UNIVERSIDAD DE CHILE": "PROPIEDAD DE U. DE CHILE",
    "EMPRESA EXTERNA": "PROPIEDAD DE EMPRESA EXTERNA",
    "U. ANDRES BELLO": "PROPIEDAD U. ANDRES BELLO",
}


CAMBIOS_PROPIEDAD_EQUIPOS_MEDICOS = {
    "PROPIO": "INT",
    "SSMO": "INT (SSMO)",
    "MINSAL": "INT (MINSAL)",
    "DRA. LINACRE": "PROPIEDAD DRA. LINACRE",
}


CAMBIO_PROPIEDAD_INDUSTRIALES = {
    "FUNCIONARIOS": "PROPIEDAD DEL FUNCIONARIO",
    "FUNCIOINARIOS": "PROPIEDAD DEL FUNCIONARIO",
    "FUNCIONARIO": "PROPIEDAD DEL FUNCIONARIO",
    "BECADOS": "PROPIEDAD DE BECADOS",
    "U CHILE": "PROPIEDAD DE U. DE CHILE",
    "FUNCIONARIOS EXTERNOS": "PROPIEDAD FUNCIONARIO EXTERNO",
    "PROVEEDOR GOLDEN": "PROPIEDAD DE PROVEEDOR GOLDEN",
    "MEL": "INT (MEL)",
    "DONACION": "INT (DONACION)",
}

CAMBIO_PROPIEDAD_INFORMATICOS = {
    "SSMO": "INT (SSMO)",
    "DONACION": "INT (DONACION)",
    "UCHILE": "PROPIEDAD DE U. DE CHILE",
    "EQUIPOS MEDICOS": "INT",
}

CAMBIO_TIPO_DE_BIEN_MOBILIARIOS = {"MAQUINARIA Y EQUIPO PARA LA PRODUCCION": "EQUIPO MEDICO"}


def procesar_mobiliario(ruta):
    # Define los archivos en la carpeta
    archivos = ruta.glob("*.xlsx")

    dfs = []
    for ruta_unidad in archivos:
        df = pd.read_excel(ruta_unidad)
        dfs.append(df)

    # Concatena todas las unidades
    df = pd.concat(dfs)

    # Limpia el nombre de las columnas
    df = fa.clean_column_names(df)

    # Elimina columnas innecesarias
    df = df.drop(columns=["ano_egreso", "unnamed_13", "unnamed_14", "unnamed_12"])

    # Elimina registros sin bien
    df = df.dropna(subset=["bien"])

    # Renombra columnas
    df = df.rename(
        columns={
            "correlativo_asignado": "correlativo_antiguo",
            "n_inventario_definido_2025": "n_inventario_2025",
            "tipo": "tipo_bien",
        }
    )

    # Agrega el int del inventario asignado
    df["numero_inventario"] = (
        df["n_inventario_2025"].str.split("-").str[-1].str.lstrip("0").astype("Int32")
    )

    # Ordena la base segun el numero del inventario
    df = df.sort_values("numero_inventario")

    # Elimina la columna del numero de inventario asignado
    df = df.drop(columns=["numero_inventario"])

    # Limpia columnas de texto
    columnas_texto = [
        "bien",
        "marca",
        "tipo_bien",
        "unidadservicio_clinico",
        "ubicacion_unidad",
        "propiedad",
        "observaciones",
    ]
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Renombra propiedades
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_MOBILIARIOS)

    # Renombra unidades
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(CAMBIOS_UNIDAD_MOBILIARIOS)

    # Renombra tipo de bien
    df["tipo_bien"] = df["tipo_bien"].replace(CAMBIO_TIPO_DE_BIEN_MOBILIARIOS)

    return df


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
        }
    )

    # Agrega el tipo de bien
    df["tipo_bien"] = "EQUIPO INDUSTRIAL"

    # Cambia el tipo de propiedad
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_INDUSTRIALES)

    # Cambio la unidad
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(
        CAMBIOS_UNIDAD_EQUIPOS_INDUSTRIALES
    )

    return df


def procesar_equipos_informaticos_nuevos(ruta):
    # Lee los archivos
    df = pd.read_excel(ruta)

    # Limpia los nombres de las columnas
    df = fa.clean_column_names(df)

    # Elimina registros sin bien
    df = df.dropna(subset=["bien"])

    # Limpia columnas de texto
    columnas_texto = [
        "bien",
        "marca",
        "unidadservicio_clinico",
        "ubicacion_unidad",
        "observaciones",
        "propiedad",
    ]
    df[columnas_texto] = df[columnas_texto].apply(fa.limpiar_columna_texto)

    # Renombra columnas
    df = df.rename(
        columns={
            "cod_int": "correlativo_antiguo",
        }
    )

    # Agrega el tipo de bien
    df["tipo_bien"] = "INFORMATICO"

    # Cambia unidad de servicio
    df["unidadservicio_clinico"] = df["unidadservicio_clinico"].replace(CAMBIO_UNIDAD_INFORMATICO)

    # Cambia propiedad
    df["propiedad"] = df["propiedad"].replace(CAMBIO_PROPIEDAD_INFORMATICOS)

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
    ruta_mobiliarios = input_path / "MOBILIARIO (ARIEL)/BASE DE ETIQUETAS/"
    ruta_equipos = (
        input_path / "EQUIPOS MEDICOS (RODRIGO)/CONSOLIDADO EQ. MEDICOS REVISADO POR RODRIGO.xlsx"
    )
    ruta_industriales_nuevo = (
        input_path
        / "EQUIPOS INDUSTRIALES Y DE OFICINA (ALEJANDRO)/PLANILLA REGISTRO DE INVENTARIO BIENES DE USO 2025.xlsx"
    )
    ruta_informaticos_nuevo = (
        input_path / "EQUIPOS INFORMATICOS FORMATO NUEVO/Consolidado Informatica nuevo formato.xlsx"
    )

    # Define rutas output
    output_mobiliarios = output_path / "df_procesada_mobiliarios.csv"
    output_equipos = output_path / "df_procesada_equipos_medicos.csv"
    output_industriales_nuevos = output_path / "df_procesada_industriales.csv"
    output_informaticos_nuevo = output_path / "df_procesada_informaticos.csv"

    # Lee y exporta diversos bienes
    df_mobiliario = procesar_mobiliario(ruta_mobiliarios)
    df_equipos = procesar_equipos_medicos(ruta_equipos)
    df_industriales_nuevos = procesar_equipos_industriales_nuevos(ruta_industriales_nuevo)
    df_informaticos_nuevos = procesar_equipos_informaticos_nuevos(ruta_informaticos_nuevo)

    df_mobiliario.to_csv(output_mobiliarios, index=False)
    df_equipos.to_csv(output_equipos, index=False)
    df_industriales_nuevos.to_csv(output_industriales_nuevos, index=False)
    df_informaticos_nuevos.to_csv(output_informaticos_nuevo, index=False)
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
