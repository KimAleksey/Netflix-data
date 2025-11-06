from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data


def main():
    try:
        # EXTRACT - Получаем данные датасета
        df = extract_data()
        # TRANSFORM - Трансформируем данные
        df = transform_data(df)
        # LOAD - Загружаем данные
        load_data(df)
        print("ETL pipeline completed successfully.")
    except Exception as e:
        print(f"Error in ETL pipeline: {e}")

if __name__ == '__main__':
    main()