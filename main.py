from datetime import datetime
import time
import pandas as pd
import csv
from typing import Any, Union
from dateutil import parser
import locale


locale.setlocale(locale.LC_TIME, 'ru-RU.UTF-8')


def runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        delta = end_time - start_time
        print(f'Время выполнения функции: {delta}')
        return result
    return wrapper


def parse_date(date_string: str) -> datetime:
    date_string = date_string.replace('января', '01').replace('февраля', '02').replace('марта', '03') \
                             .replace('апреля', '04').replace('мая', '05').replace('июня', '06') \
                             .replace('июля', '07').replace('августа', '08').replace('сентября', '09') \
                             .replace('октября', '10').replace('ноября', '11').replace('декабря', '12')
    return pd.to_datetime(date_string, format='%d %m %Y г. %H:%M:%S.%f мсек')


@runtime
def filter_data(start_time: datetime, end_time: datetime, aperture: float,
                input_csv: str, output_csv: str) -> None:
    # Чтение данных из CSV файла
    df = pd.read_csv(input_csv, delimiter=';', encoding='cp1251')

    # Преобразование столбца времени в формат datetime
    df['Дата и время записи'] = df['Дата и время записи'].apply(parse_date)

    # Фильтрация данных по заданному временному диапазону
    filtered_df = df[(df['Дата и время записи'] >= start_time) & (df['Дата и время записи'] <= end_time)]

    # Пустой список для хранения отфильтрованных строк
    result_rows = []

    # Проход по строкам и проверка условия апертуры
    for index, row in filtered_df.iterrows():
        if index > 0:
            # Проверка, существует ли индекс в DataFrame
            if index - 1 not in filtered_df.index:
                continue

            # Получение разницы во времени
            time_difference = row['Дата и время записи'] - filtered_df.at[index - 1, 'Дата и время записи']

            # Проверка апертуры для каждого параметра
            if any(
                abs(time_difference.total_seconds()) > aperture
                for param in df.columns[1:]
            ):
                result_rows.append(row)

    # Создание нового DataFrame из отфильтрованных строк
    result_df = pd.DataFrame(result_rows)

    # Запись результата в CSV файл
    result_df.to_csv(output_csv, index=False, encoding='cp1251', quoting=csv.QUOTE_NONNUMERIC, sep=';')


# Пример использования
start_time = datetime(2022, 8, 18, 7, 0, 0)
end_time = datetime(2022, 8, 18, 8, 0, 0)
aperture_value = 0.5
input_csv_file = 'input_data.csv'
output_csv_file = 'filtered_data.csv'


filter_data(start_time, end_time, aperture_value, input_csv_file, output_csv_file)
