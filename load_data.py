import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Данные подключения
load_dotenv()
DATABASE_NAME = os.getenv('DATABASE_NAME')
USER_NAME = os.getenv('USER_NAME')
PASSWORD = os.getenv('PASSWORD')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')


def create_table():
    # параметры подключения
    conn = psycopg2.connect(
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
        show_id TEXT PRIMARY KEY,
        type TEXT,
        title TEXT,
        director TEXT,
        cast TEXT,
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