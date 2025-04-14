import pandas as pd


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
