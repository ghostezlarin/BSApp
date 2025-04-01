import psycopg2
import time
import os
import chardet
import platform
import socket
import getpass
from datetime import datetime
from psycopg2 import sql
from db_config import user, password, host, port, database, SCHEMA_NAME

# Константы для проверок
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 МБ
MIN_FILE_SIZE = 33  # 33 байт размер мин строки
MAX_LINE_LENGTH = 10000  # Максимальная длина строки
MIN_LINE_LENGTH = 33  # Минимальная длина строки
EXPECTED_FIELDS = 7  # Ожидаемое число полей в строке
ALLOWED_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,.-_()\"\'\t\n/"  # Латинские буквы, цифры, спецсимволы
    "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"  # Кириллица
)


def print_system_info():
    """Выводит информацию о системе и среде развертывания."""
    print("\n=== СИСТЕМНАЯ ИНФОРМАЦИЯ ===")
    print(f"Дата и время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"ОС: {platform.system()} {platform.release()} ({platform.version()})")
    print(f"Архитектура: {platform.machine()}")
    print(f"Имя компьютера: {socket.gethostname()}")
    print(f"Имя пользователя: {getpass.getuser()}")
    print(f"Рабочая директория: {os.getcwd()}")
    print(f"Python версия: {platform.python_version()}")
    print(f"Psycopg2 версия: {psycopg2.__version__}")
    print(f"Процессор: {platform.processor()}")
    print(f"Логические ядер CPU: {os.cpu_count()}")

def check_file_encoding(file_path):
    """Проверяет кодировку файла."""
    start_time = time.time()
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        end_time = time.time()
        print(
            f"Определена кодировка файла: {encoding} (уверенность: {confidence * 100:.2f}%). Время выполнения: {end_time - start_time:.2f} секунд.")
        return encoding


def check_file_size(file_path):
    """Проверяет размер файла."""
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    if file_size > MAX_FILE_SIZE:
        raise ValueError(f"Размер файла превышает максимально допустимый ({MAX_FILE_SIZE} байт).")
    if file_size < MIN_FILE_SIZE:
        raise ValueError(f"Размер файла меньше минимально допустимого ({MIN_FILE_SIZE} байт).")
    end_time = time.time()
    print(
        f"Размер файла в допустимых пределах: {file_size} байт. Время выполнения: {end_time - start_time:.2f} секунд.")


def check_line_lengths(file_path, encoding):
    """Проверяет длину строк в файле."""
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            line_length = len(line.strip())
            if line_length > MAX_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} превышает максимальную длину ({MAX_LINE_LENGTH} символов).")
            if line_length < MIN_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} короче минимальной длины ({MIN_LINE_LENGTH} символов).")
    end_time = time.time()
    print(f"Длина всех строк в допустимых пределах. Время выполнения: {end_time - start_time:.2f} секунд.")


def check_field_count(file_path, encoding, delimiter='\t'):
    """Проверяет число полей в каждой строке."""
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            fields = line.strip().split(delimiter)
            if len(fields) != EXPECTED_FIELDS:
                raise ValueError(f"Строка {line_num} содержит {len(fields)} полей, ожидается {EXPECTED_FIELDS}.")
    end_time = time.time()
    print(
        f"Все строки содержат корректное число полей ({EXPECTED_FIELDS}). Время выполнения: {end_time - start_time:.2f} секунд.")


def check_allowed_chars(file_path, encoding):
    """Проверяет допустимые символы в файле."""
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            for char in line:
                if char not in ALLOWED_CHARS:
                    raise ValueError(f"Недопустимый символ '{char}' в строке {line_num}.")
    end_time = time.time()
    print(f"Все символы в файле допустимы. Время выполнения: {end_time - start_time:.2f} секунд.")


def validate_file(file_path):
    """Выполняет все проверки файла перед загрузкой."""
    print(f"\nПроверка файла: {file_path}")
    start_time = time.time()

    encoding = check_file_encoding(file_path)
    check_file_size(file_path)
    check_line_lengths(file_path, encoding)
    check_field_count(file_path, encoding)
    check_allowed_chars(file_path, encoding)

    end_time = time.time()
    print(f"Все проверки пройдены успешно. Общее время проверки: {end_time - start_time:.2f} секунд.\n")


def create_table(conn, table_name):
    """Создает основную таблицу в указанной схеме PostgreSQL с использованием psycopg2.sql."""
    start_time = time.time()
    with conn.cursor() as cursor:
        # Проверка существования последовательности
        seq_name = f"s_{table_name}_id"
        query = sql.SQL("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.sequences 
                WHERE sequence_schema = %s AND sequence_name = %s
            )
        """)
        cursor.execute(query, (SCHEMA_NAME, seq_name))
        sequence_exists = cursor.fetchone()[0]

        if not sequence_exists:
            create_seq = sql.SQL("""
                CREATE SEQUENCE {schema}.{seq_name}
                    INCREMENT 1
                    START 1
                    MINVALUE 1
                    MAXVALUE 9223372036854775807
                    CACHE 1
            """).format(
                schema=sql.Identifier(SCHEMA_NAME),
                seq_name=sql.Identifier(seq_name)
            )
            cursor.execute(create_seq)

        # Проверка существования таблицы
        tbl_name = f"t_{table_name}"
        query = sql.SQL("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """)
        cursor.execute(query, (SCHEMA_NAME, tbl_name))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Формируем строку с именем последовательности для DEFAULT
            seq_default = sql.SQL("nextval('{schema}.{seq_name}'::regclass)").format(
                schema=sql.Identifier(SCHEMA_NAME),
                seq_name=sql.Identifier(seq_name)
            )

            create_tbl = sql.SQL("""
                CREATE TABLE {schema}.{tbl_name} (
                    id bigint NOT NULL DEFAULT {seq_default},
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
                )
            """).format(
                schema=sql.Identifier(SCHEMA_NAME),
                tbl_name=sql.Identifier(tbl_name),
                seq_default=seq_default
            )
            cursor.execute(create_tbl)

            # Настройка владельца последовательности
            alter_seq = sql.SQL("""
                ALTER SEQUENCE {schema}.{seq_name} OWNED BY {schema}.{tbl_name}.id
            """).format(
                schema=sql.Identifier(SCHEMA_NAME),
                seq_name=sql.Identifier(seq_name),
                tbl_name=sql.Identifier(tbl_name)
            )
            cursor.execute(alter_seq)

            # Настройка владельца таблицы
            alter_tbl = sql.SQL("""
                ALTER TABLE {schema}.{tbl_name} OWNER TO postgres
            """).format(
                schema=sql.Identifier(SCHEMA_NAME),
                tbl_name=sql.Identifier(tbl_name)
            )
            cursor.execute(alter_tbl)

    conn.commit()
    end_time = time.time()
    print(
        f"Таблица {SCHEMA_NAME}.{tbl_name} создана или уже существует. Время выполнения: {end_time - start_time:.2f} секунд.")

def load_data_with_copy(conn, table_name, file_path, delimiter='\t', null_value='\\N'):
    """Загружает данные из CSV/TSV файла в таблицу с использованием команды COPY."""
    start_time = time.time()

    copy_query = sql.SQL("""
        COPY {schema}.{table} (
            account_number, full_name, address, 
            period_year, period_month, meter_reading, debt
        ) FROM stdin WITH (FORMAT csv, DELIMITER %s, NULL %s, HEADER)
    """).format(
        schema=sql.Identifier(SCHEMA_NAME),
        table=sql.Identifier(f"t_{table_name}")
    )

    with open(file_path, 'r', encoding='utf-8') as file:
        with conn.cursor() as cursor:
            cursor.copy_expert(cursor.mogrify(copy_query, (delimiter, null_value)), file)

    conn.commit()
    end_time = time.time()
    print(
        f"Данные из файла {file_path} загружены в таблицу {SCHEMA_NAME}.t_{table_name}. Время выполнения: {end_time - start_time:.2f} секунд.")


def get_db_connection_info(conn):
    """Возвращает информацию о подключении к БД"""
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("SELECT version()"))
        db_version = cursor.fetchone()[0]

        cursor.execute(sql.SQL("""
            SELECT current_database(), current_user, inet_client_addr(), inet_client_port()
        """))
        db_info = cursor.fetchone()

        cursor.execute(sql.SQL("""
            SELECT pg_size_pretty(pg_database_size(current_database()))
        """))
        db_size = cursor.fetchone()[0]

    return {
        "DB Version": db_version,
        "Database": db_info[0],
        "User": db_info[1],
        "Client IP": db_info[2],
        "Client Port": db_info[3],
        "DB Size": db_size
    }


def print_db_info(conn):
    """Выводит информацию о подключении к БД"""
    db_info = get_db_connection_info(conn)
    print("\n=== ИНФОРМАЦИЯ О ПОДКЛЮЧЕНИИ К БД ===")
    for key, value in db_info.items():
        print(f"{key}: {value}")
    print("===================================\n")


def main(file_path):
    """Основная функция для загрузки данных."""
    total_start_time = time.time()

    print_system_info()
    validate_file(file_path)

    conn = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )

    try:
        print_db_info(conn)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        create_table(conn, table_name)
        load_data_with_copy(conn, table_name, file_path)
    finally:
        conn.close()

    total_end_time = time.time()
    print(f"\nОбщее время выполнения скрипта: {total_end_time - total_start_time:.2f} секунд.")


if __name__ == "__main__":
    file_path = "GAZ.tsv"
    main(file_path)