from etl.extract.extract import extracted_data
from etl.transform.transform import fill_na_with_unknown, convert_date, convert_duration, replace_semi_column, drop_key_duplicates
from etl.load.load import create_table, load_data


def main():
    # EXTRACT - Получаем данные датасета
    df = extracted_data

    # TRANSFORM - Трансформируем данные
    df = fill_na_with_unknown(df)
    df['date_added'] = convert_date(df['date_added'])
    df['duration_seasons'] = convert_duration(df['duration'], duration='seasons')
    df['duration_min'] = convert_duration(df['duration'], duration='min')
    df = df.drop(columns='duration')
    df = replace_semi_column(df)
    df = drop_key_duplicates(df)

    # LOAD - Загружаем данные
    create_table()
    load_data(df)


if __name__ == '__main__':
    main()