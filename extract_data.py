from get_data_from_csv import get_data
from pandas import errors as pd_errors

try:
    extracted_data = get_data()
except FileNotFoundError:
    print("Ошибка: файл не найден по пути")
except pd_errors.EmptyDataError:
    print("Ошибка: файл пустой")