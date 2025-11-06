from pandas import DataFrame, read_csv, errors as pd_errors
from os import getenv
from dotenv import load_dotenv

# Чувствительные данные скрываем
load_dotenv()
CSV_FILE_PATH = getenv("CSV_FILE_PATH")


def get_data() -> DataFrame:
    """
    Загружает данные о фильмах из CSV-файла.
    :return: DataFrame
    """
    if not CSV_FILE_PATH:
        print("Cannot get file path")
        return None

    try:
        netflix_data = read_csv(CSV_FILE_PATH)
        return netflix_data
    except FileNotFoundError:
        print(f"Ошибка: файл не найден по пути {CSV_FILE_PATH}")
    except pd_errors.EmptyDataError:
        print(f"Ошибка: файл пустой")

    return None


try:
    extracted_data = get_data()
except FileNotFoundError:
    print("Ошибка: файл не найден по пути")
except pd_errors.EmptyDataError:
    print("Ошибка: файл пустой")