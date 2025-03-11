import psycopg2
import time
import os
import chardet
from db_config import user, password, host, port, database, SCHEMA_NAME

# Константы для проверок
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 МБ
MIN_FILE_SIZE = 1  # 1 байт
MAX_LINE_LENGTH = 10000  # Максимальная длина строки
MIN_LINE_LENGTH = 10  # Минимальная длина строки
EXPECTED_FIELDS = 7  # Ожидаемое число полей в строке
ALLOWED_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,.-_()\"\'\t\n/"  # Латинские буквы, цифры, спецсимволы
    "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"  # Кириллица
)

def check_file_encoding(file_path):
    """Проверяет кодировку файла."""
    start_time = time.time()  # Начало замера времени
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        end_time = time.time()  # Конец замера времени
        print(f"Определена кодировка файла: {encoding} (уверенность: {confidence * 100:.2f}%). Время выполнения: {end_time - start_time:.2f} секунд.")
        return encoding

def check_file_size(file_path):
    """Проверяет размер файла."""
    start_time = time.time()  # Начало замера времени
    file_size = os.path.getsize(file_path)
    if file_size > MAX_FILE_SIZE:
        raise ValueError(f"Размер файла превышает максимально допустимый ({MAX_FILE_SIZE} байт).")
    if file_size < MIN_FILE_SIZE:
        raise ValueError(f"Размер файла меньше минимально допустимого ({MIN_FILE_SIZE} байт).")
    end_time = time.time()  # Конец замера времени
    print(f"Размер файла в допустимых пределах: {file_size} байт. Время выполнения: {end_time - start_time:.2f} секунд.")

def check_line_lengths(file_path, encoding):
    """Проверяет длину строк в файле."""
    start_time = time.time()  # Начало замера времени
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            line_length = len(line.strip())
            if line_length > MAX_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} превышает максимальную длину ({MAX_LINE_LENGTH} символов).")
            if line_length < MIN_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} короче минимальной длины ({MIN_LINE_LENGTH} символов).")
    end_time = time.time()  # Конец замера времени
    print(f"Длина всех строк в допустимых пределах. Время выполнения: {end_time - start_time:.2f} секунд.")

def check_field_count(file_path, encoding, delimiter='\t'):
    """Проверяет число полей в каждой строке."""
    start_time = time.time()  # Начало замера времени
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            fields = line.strip().split(delimiter)
            if len(fields) != EXPECTED_FIELDS:
                raise ValueError(f"Строка {line_num} содержит {len(fields)} полей, ожидается {EXPECTED_FIELDS}.")
    end_time = time.time()  # Конец замера времени
    print(f"Все строки содержат корректное число полей ({EXPECTED_FIELDS}). Время выполнения: {end_time - start_time:.2f} секунд.")

def check_allowed_chars(file_path, encoding):
    """Проверяет допустимые символы в файле."""
    start_time = time.time()  # Начало замера времени
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            for char in line:
                if char not in ALLOWED_CHARS:
                    raise ValueError(f"Недопустимый символ '{char}' в строке {line_num}.")
    end_time = time.time()  # Конец замера времени
    print(f"Все символы в файле допустимы. Время выполнения: {end_time - start_time:.2f} секунд.")

def validate_file(file_path):
    """Выполняет все проверки файла перед загрузкой."""
    print(f"Проверка файла: {file_path}")
    start_time = time.time()  # Начало общего замера времени

    # Проверка кодировки
    encoding = check_file_encoding(file_path)

    # Проверка размера файла
    check_file_size(file_path)

    # Проверка длины строк
    check_line_lengths(file_path, encoding)

    # Проверка числа полей
    check_field_count(file_path, encoding)

    # Проверка допустимых символов
    check_allowed_chars(file_path, encoding)

    end_time = time.time()  # Конец общего замера времени
    print(f"Все проверки пройдены успешно. Общее время проверки: {end_time - start_time:.2f} секунд.\n")

def create_table(conn, table_name):
    """Создает основную таблицу в указанной схеме PostgreSQL."""
    start_time = time.time()  # Начало замера времени
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE SEQUENCE IF NOT EXISTS test.s_{table_name}_id
                INCREMENT 1
                START 1
                MINVALUE 1
                MAXVALUE 9223372036854775807
                CACHE 1;

            CREATE TABLE IF NOT EXISTS test.t_{table_name} (
                id bigint NOT NULL DEFAULT nextval('test.s_{table_name}_id'::regclass),
                status bigint NOT NULL DEFAULT 0,
                date_insert timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP,
                account_number character varying NOT NULL,
                full_name character varying NOT NULL,
                address character varying NOT NULL,
                period_year character varying NOT NULL,
                period_month character varying NOT NULL,
                meter_reading character varying NOT NULL,
                debt bigint NOT NULL,
                CONSTRAINT data_pkey PRIMARY KEY (id)
            );

            ALTER SEQUENCE test.s_{table_name}_id OWNED BY test.t_{table_name}.id;
            ALTER TABLE test.t_{table_name} OWNER TO postgres;
        """)
    conn.commit()
    end_time = time.time()  # Конец замера времени
    print(f"Таблица test.t_{table_name} создана или уже существует. Время выполнения: {end_time - start_time:.2f} секунд.")

def load_data_with_copy(conn, table_name, file_path, delimiter='\t', null_value='\\N'):
    """Загружает данные из CSV/TSV файла в таблицу с использованием команды COPY."""
    start_time = time.time()  # Начало замера времени

    # Переводим таблицу в режим UNLOGGED для ускорения вставки
    with conn.cursor() as cursor:
        cursor.execute(f"ALTER TABLE test.t_{table_name} SET UNLOGGED;")
        conn.commit()

    # Формируем команду COPY
    copy_command = f"""
        COPY test.t_{table_name} (account_number, full_name, address, period_year, period_month, meter_reading, debt)
        FROM stdin
        WITH (FORMAT csv, DELIMITER '{delimiter}', NULL '{null_value}', HEADER);
    """

    # Открываем файл и загружаем данные
    with open(file_path, 'r', encoding='utf-8') as file:
        with conn.cursor() as cursor:
            cursor.copy_expert(copy_command, file)

    # Возвращаем таблицу в режим LOGGED
    with conn.cursor() as cursor:
        cursor.execute(f"ALTER TABLE test.t_{table_name} SET LOGGED;")
        conn.commit()

    end_time = time.time()  # Конец замера времени
    print(f"Данные из файла {file_path} загружены в таблицу test.t_{table_name}. Время выполнения: {end_time - start_time:.2f} секунд.")

def main(file_path):
    """Основная функция для загрузки данных."""
    total_start_time = time.time()  # Начало общего замера времени

    # Проверка файла перед загрузкой
    validate_file(file_path)

    # Подключение к PostgreSQL
    conn = psycopg2.connect(user=user,
                            password=password,
                            host=host,
                            port=port,
                            database=database)

    # Имя таблицы соответствует имени файла (без расширения)
    table_name = file_path.split(".")[0]

    try:
        # Создание основной таблицы в схеме test
        create_table(conn, table_name)

        # Загрузка данных из CSV/TSV файла
        load_data_with_copy(conn, table_name, file_path)
    finally:
        # Закрытие соединения с базой данных
        conn.close()

    total_end_time = time.time()  # Конец общего замера времени
    print(f"Общее время выполнения скрипта: {total_end_time - total_start_time:.2f} секунд.")

# Пример использования
if __name__ == "__main__":
    file_path = "data.tsv"  # Путь к .tsv файлу
    main(file_path)