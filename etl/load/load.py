from psycopg2 import connect
from pandas import DataFrame, read_csv, errors as pd_errors
from os import getenv
from dotenv import load_dotenv
from io import StringIO
from csv import QUOTE_NONNUMERIC

# Данные подключения
load_dotenv()

DATABASE_NAME = getenv('DATABASE_NAME')
USER_NAME = getenv('USER_NAME')
PASSWORD = getenv('PASSWORD')
HOST = getenv('HOST')
PORT = getenv('PORT')


def create_table():
    # параметры подключения
    conn = connect(
        dbname=f'{DATABASE_NAME}',
        user=f'{USER_NAME}',
        password=f'{PASSWORD}',
        host=f'{HOST}',  # или IP сервера
        port=f'{PORT}'
    )

    # открываем курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # SQL для создания таблицы
    create_table_query = """
    CREATE TABLE IF NOT EXISTS myschema.netflix_data (
        show_id varchar(10000) PRIMARY KEY,
        type TEXT,
        title TEXT,
        director TEXT,
        "cast" TEXT,
        country TEXT,
        date_added DATE,
        release_year INTEGER,
        rating TEXT,
        listed_in TEXT,
        description TEXT,
        duration_seasons INTEGER,
        duration_min INTEGER
    );
    """

    # выполняем команду
    cur.execute(create_table_query)

    # сохраняем изменения
    conn.commit()

    # закрываем соединение
    cur.close()
    conn.close()

    return True


def load_data_to_postgres(df: DataFrame):
    # параметры подключения
    conn = connect(
        dbname=f'{DATABASE_NAME}',
        user=f'{USER_NAME}',
        password=f'{PASSWORD}',
        host=f'{HOST}',  # или IP сервера
        port=f'{PORT}'
    )

    # открываем курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # 1. Конвертируем DataFrame в CSV-строку (в оперативной памяти)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=';', quotechar='"', quoting=QUOTE_NONNUMERIC)
    buffer.seek(0)

    # 2. Экранируем названия колонок
    columns = ', '.join([f'"{col}"' for col in df.columns])

    # 3. Создаём временную таблицу с той же структурой
    cur.execute("DROP TABLE IF EXISTS tmp_netflix;")
    cur.execute("CREATE TEMP TABLE tmp_netflix AS TABLE netflix_data WITH NO DATA;")

    # 4. Копируем данные во временную таблицу
    copy_sql = f"""
         COPY tmp_netflix ({columns})
         FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '');
     """
    cur.copy_expert(sql=copy_sql, file=buffer)

    # 5. Обновляем существующие строки + добавляем новые
    upsert_sql = f"""
         INSERT INTO netflix_data ({columns})
         SELECT {columns} FROM tmp_netflix
         ON CONFLICT (show_id) DO UPDATE SET
             {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != 'show_id'])};
     """
    cur.execute(upsert_sql)

    # 6. Сохраняем изменения
    conn.commit()

    # 7. Закрываем соединение
    cur.close()
    conn.close()

    print("Данные успешно загружены в PostgreSQL!")


def load_data(df: DataFrame):
    create_table()
    load_data_to_postgres(df)