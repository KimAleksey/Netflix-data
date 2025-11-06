from pandas import DataFrame, Series
from pendulum import from_format, parse

def fill_na_with_unknown(df: DataFrame) -> DataFrame:
    """
    Все NaN заменяем на Unknown, для текстовых колонок 'director', 'cast', 'country', 'rating'
    Все NaN в датах заменяем на January 1, 1900
    Все NaN в duration заменяем на 0 Seasons
    """
    fill_values = {
        'director': 'Unknown',
        'cast': 'Unknown',
        'country': 'Unknown',
        'rating': 'Unknown',
        'date_added': 'January 1, 1900',
        'duration': '0 Seasons'
    }

    df = df.fillna(value=fill_values)
    return df


def convert_date(s: Series) -> Series:
    """
    Конвертируем дату в формате September 25, 2021
    в формат 2021-09-25
    """
    def safe_parse(x):
        # Пропускаем пустые и нестроковые значения
        if not isinstance(x, str) or x is None or x == '':
            return None

        try:
            # формат с одной или двумя цифрами дня
            return from_format(x.strip(), "MMMM D, YYYY").to_date_string()
        except Exception:
            try:
                # если формат немного отличается — автоопределение
                return parse(x.strip()).to_date_string()
            except Exception:
                return None


    s = s.apply(func=safe_parse)
    return s


def convert_duration(s: Series, duration='seasons') -> Series:
    """
    Конвертируем:
        - 12 Seasons -> 12, иначе 0 (duration='seasons')
        - 120 min -> 120, иначе 0 (duration='min')
    """
    def parse(x):
        if not isinstance(x, str) or not x.strip():
            return 0
        if duration == 'min':
            if 'min' in x:
                return int(x.replace('min', '').strip())
        else:
            if 'Season' in x or 'Seasons' in x:
                return int(x.replace('Seasons', '').replace('Season', '').strip())
        return 0


    s = s.apply(func=parse)
    return s


def replace_semi_column(df: DataFrame) -> DataFrame:
    """
    Убираем символ ;, чтобы в дальнейшем корретно парсить строку на столбцы,
    используя в качестве разделителя ;
    """
    if df.empty:
        return df
    return df.replace(';', ',', regex=True)


def drop_key_duplicates(df: DataFrame) -> DataFrame:
    """
    Удаление дубликатов по ключу show_id,
    чтобы не возникало ошибок при вставке
    """
    if df.empty:
        return df
    return df.drop_duplicates(subset=['show_id'])


def transform_data(df: DataFrame) -> DataFrame:
    df = fill_na_with_unknown(df)
    df['date_added'] = convert_date(df['date_added'])
    df['duration_seasons'] = convert_duration(df['duration'], duration='seasons')
    df['duration_min'] = convert_duration(df['duration'], duration='min')
    df = df.drop(columns='duration')
    df = replace_semi_column(df)
    df = drop_key_duplicates(df)
    return df