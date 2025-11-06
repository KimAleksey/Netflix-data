from extract_data import extracted_data
from transform_data import fill_na_with_unknown, convert_date, convert_duration, replace_semi_column, drop_key_duplicates
from load_data import create_table, load_data

# EXTRACT - Получаем данные датасета
df = extracted_data

# TRANSFORM - Трансформируем данные
df = fill_na_with_unknown(df)
df['date_added'] = convert_date(df['date_added'])
# print(df.loc[df['show_id'] == 's6067', ['director', 'cast', 'date_added']].head(15))

df['duration_seasons'] = convert_duration(df['duration'], duration='seasons')
df['duration_min'] = convert_duration(df['duration'], duration='min')
df = df.drop(columns='duration')
df = replace_semi_column(df)
df = drop_key_duplicates(df)
# print(df[['show_id', 'duration_seasons', 'duration_min']].head(15))

# print(df.columns)
# print(df.dtypes)

# LOAD - Загружаем данные
create_table()
load_data(df)