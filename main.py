from etl.extract import extracted_data
from etl.transform import transform_data
from etl.load import load_data


def main():
    # EXTRACT - Получаем данные датасета
    df = extracted_data

    # TRANSFORM - Трансформируем данные
    df = transform_data(df)

    # LOAD - Загружаем данные
    load_data(df)


if __name__ == '__main__':
    main()