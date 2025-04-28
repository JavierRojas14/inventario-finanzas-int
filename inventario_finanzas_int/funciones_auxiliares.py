import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo


def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by converting to lowercase, replacing spaces with
    underscores, ensuring only a single underscore between words, and removing miscellaneous symbols.

    :param df: The input DataFrame.
    :type df: pandas DataFrame

    :return: The DataFrame with cleaned column names.
    :rtype: pandas DataFrame
    """
    tmp = df.copy()

    # Clean and transform the column names
    cleaned_columns = (
        df.columns.str.lower()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
        .str.replace(
            r"[^\w\s]", "", regex=True
        )  # Remove all non-alphanumeric characters except spaces
        .str.replace(r"\s+", "_", regex=True)  # Replace spaces with underscores
        .str.replace(r"_+", "_", regex=True)  # Ensure only a single underscore between words
        .str.strip("_")
    )

    # Assign the cleaned column names back to the DataFrame
    tmp.columns = cleaned_columns

    return tmp


def limpiar_columna_texto(serie):
    return (
        serie.str.upper()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
    )


def guardar_dataframe_como_tabla_excel(
    df: pd.DataFrame,
    ruta_archivo: str,
    nombre_tabla: str = "Tabla1",
    estilo_tabla: str = "TableStyleMedium9",
) -> None:
    """
    Guarda un DataFrame de pandas en un archivo Excel como una tabla formal (no solo como
    rango de celdas).

    Parámetros:
        df (pd.DataFrame): DataFrame que quieres exportar.
        ruta_archivo (str): Ruta donde se guardará el archivo Excel.
        nombre_tabla (str): Nombre de la tabla en Excel (debe ser único y sin espacios).
        estilo_tabla (str): Estilo visual de la tabla en Excel. Por defecto "TableStyleMedium9".
    """
    # Crear un nuevo libro y seleccionar la hoja activa
    wb = Workbook()
    ws = wb.active

    # Escribir el DataFrame en la hoja
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    # Calcular el rango de la tabla
    max_col = get_column_letter(df.shape[1])
    max_row = df.shape[0] + 1  # +1 para la cabecera
    rango_tabla = f"A1:{max_col}{max_row}"

    # Crear la tabla
    tabla = Table(displayName=nombre_tabla, ref=rango_tabla)

    # Aplicar estilo
    estilo = TableStyleInfo(
        name=estilo_tabla,
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    tabla.tableStyleInfo = estilo

    # Agregar la tabla a la hoja
    ws.add_table(tabla)

    # Guardar el archivo
    wb.save(ruta_archivo)

    print(f"✅ Archivo guardado exitosamente en: {ruta_archivo}")
