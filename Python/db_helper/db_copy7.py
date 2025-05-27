import psycopg2
import time
import os
import chardet
import platform
import socket
import getpass
from datetime import datetime
from psycopg2 import sql
from db_config import user, password, host, port, database
from functools import wraps


def with_transaction(func):
    @wraps(func)
    def wrapper(conn, *args, **kwargs):
        try:
            result = func(conn, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise e

    return wrapper


# Константы для проверок
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 МБ
MIN_FILE_SIZE = 33  # 33 байт размер мин строки
MAX_LINE_LENGTH = 10000  # Максимальная длина строки
MIN_LINE_LENGTH = 33  # Минимальная длина строки
EXPECTED_FIELDS = 7  # Ожидаемое число полей в строке
ALLOWED_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,.-_()\"\'\t\n/"
    "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
)

# Константы для этапов загрузки (битовые флаги)
LOAD_STAGES = {
    'create_table': 1,  # 2^0
    'copy_data': 2,  # 2^1
    'finalize': 4  # 2^2
}


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
    print(f"\nПроверка кодировки файла {file_path}...")
    start_time = time.time()
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        end_time = time.time()
        print(
            f"Определена кодировка: {encoding} (уверенность: {confidence * 100:.2f}%). Время: {end_time - start_time:.2f} сек.")
        return encoding


def ensure_log_tables_exist(conn, schema_name):
    """Гарантирует существование таблиц для логирования"""
    print("\nПроверка таблиц логов...")
    with conn.cursor() as cursor:
        try:
            # Таблица для логов этапов загрузки
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.t_load_stages (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    stage_bitmap INTEGER NOT NULL DEFAULT 0,
                    status_code SMALLINT NOT NULL DEFAULT 0,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    error_message TEXT
                )
            """).format(schema=sql.Identifier(schema_name)))

            conn.commit()
            print("Таблица логов успешно проверена/создана")
        except Exception as e:
            conn.rollback()
            print(f"Ошибка при создании таблицы логов: {str(e)}")
            raise


def create_load_stage_log(conn, schema_name, table_name):
    """Создает запись в логе этапов загрузки"""
    print(f"\nСоздание лога этапов для таблицы {table_name}...")
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                INSERT INTO {schema}.t_load_stages 
                (table_name, stage_bitmap, status_code)
                VALUES (%s, %s, 0)
                RETURNING id
            """).format(schema=sql.Identifier(schema_name)),
                           (table_name, 0))  # Изначально все этапы = 0
            log_id = cursor.fetchone()[0]
            print(f"Лог этапов создан, ID: {log_id}")
            return log_id
    except Exception as e:
        print(f"Ошибка создания лога этапов: {str(e)}")
        raise


def update_stage_status(conn, schema_name, stage_log_id, stage_name, success=True, error_message=None):
    """Обновляет статус этапа в битовой карте"""
    stage_flag = LOAD_STAGES[stage_name]
    status_text = "успешно" if success else "с ошибкой"
    print(f"\nОбновление статуса этапа '{stage_name}': {status_text}...")

    try:
        with conn.cursor() as cursor:
            # Получаем текущую битовую карту
            cursor.execute(sql.SQL("""
                SELECT stage_bitmap FROM {schema}.t_load_stages WHERE id = %s FOR UPDATE
            """).format(schema=sql.Identifier(schema_name)), (stage_log_id,))

            current_bitmap = cursor.fetchone()[0]

            # Устанавливаем или сбрасываем бит
            if success:
                new_bitmap = current_bitmap | stage_flag  # Устанавливаем бит (OR)
            else:
                new_bitmap = current_bitmap & ~stage_flag  # Сбрасываем бит (AND NOT)

            # Формируем запрос обновления
            update_data = {
                'stage_bitmap': new_bitmap,
                'stage_log_id': stage_log_id
            }

            update_query = sql.SQL("""
                UPDATE {schema}.t_load_stages
                SET stage_bitmap = %(stage_bitmap)s
            """).format(schema=sql.Identifier(schema_name))

            # Добавляем статус и сообщение об ошибке при необходимости
            if not success:
                update_query += sql.SQL(", status_code = 2, error_message = %(error_message)s")
                update_data['error_message'] = error_message
            elif new_bitmap == sum(LOAD_STAGES.values()):  # Все этапы выполнены (1+2+4=7)
                update_query += sql.SQL(", status_code = 1, end_time = CURRENT_TIMESTAMP")

            update_query += sql.SQL(" WHERE id = %(stage_log_id)s")

            cursor.execute(update_query, update_data)

        print(f"Статус этапа '{stage_name}' обновлен")
    except Exception as e:
        print(f"Ошибка обновления статуса этапа: {str(e)}")
        raise


def check_file_size(file_path):
    """Проверяет размер файла."""
    print(f"\nПроверка размера файла {file_path}...")
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    if file_size > MAX_FILE_SIZE:
        raise ValueError(f"Размер файла превышает максимально допустимый ({MAX_FILE_SIZE} байт).")
    if file_size < MIN_FILE_SIZE:
        raise ValueError(f"Размер файла меньше минимально допустимого ({MIN_FILE_SIZE} байт).")
    end_time = time.time()
    print(f"Размер файла в допустимых пределах: {file_size} байт. Время: {end_time - start_time:.2f} сек.")


def check_line_lengths(file_path, encoding):
    """Проверяет длину строк в файле."""
    print(f"\nПроверка длины строк в файле {file_path}...")
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            line_length = len(line.strip())
            if line_length > MAX_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} превышает максимальную длину ({MAX_LINE_LENGTH} символов).")
            if line_length < MIN_LINE_LENGTH:
                raise ValueError(f"Строка {line_num} короче минимальной длины ({MIN_LINE_LENGTH} символов).")
    end_time = time.time()
    print(f"Длина всех строк в допустимых пределах. Время: {end_time - start_time:.2f} сек.")


def check_field_count(file_path, encoding, delimiter='\t'):
    """Проверяет число полей в каждой строке."""
    print(f"\nПроверка количества полей в файле {file_path}...")
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            fields = line.strip().split(delimiter)
            if len(fields) != EXPECTED_FIELDS:
                raise ValueError(f"Строка {line_num} содержит {len(fields)} полей, ожидается {EXPECTED_FIELDS}.")
    end_time = time.time()
    print(f"Все строки содержат корректное число полей ({EXPECTED_FIELDS}). Время: {end_time - start_time:.2f} сек.")


def check_allowed_chars(file_path, encoding):
    """Проверяет допустимые символы в файле."""
    print(f"\nПроверка допустимых символов в файле {file_path}...")
    start_time = time.time()
    with open(file_path, 'r', encoding=encoding) as f:
        for line_num, line in enumerate(f, 1):
            for char in line:
                if char not in ALLOWED_CHARS:
                    raise ValueError(f"Недопустимый символ '{char}' в строке {line_num}.")
    end_time = time.time()
    print(f"Все символы в файле допустимы. Время: {end_time - start_time:.2f} сек.")


def validate_file(file_path):
    """Выполняет все проверки файла перед загрузкой."""
    print(f"\n=== ВАЛИДАЦИЯ ФАЙЛА {file_path} ===")
    start_time = time.time()

    encoding = check_file_encoding(file_path)
    check_file_size(file_path)
    check_line_lengths(file_path, encoding)
    check_field_count(file_path, encoding)
    check_allowed_chars(file_path, encoding)

    end_time = time.time()
    print(f"\nВсе проверки пройдены успешно. Общее время проверки: {end_time - start_time:.2f} сек.")


def get_next_table_number(conn, schema_name, base_table_name):
    """Безопасное определение следующего номера таблицы с проверкой результата"""
    with conn.cursor() as cursor:
        try:
            cursor.execute("""
                SELECT COALESCE(MAX(
                    NULLIF(
                        REGEXP_REPLACE(table_name, %s, %s), ''
                    )::INT
                ), 0) + 1
                FROM information_schema.tables
                WHERE table_schema = %s 
                AND table_name ~ %s
            """, (
                f'^{base_table_name}_([0-9]+)$',
                r'\1',
                schema_name,
                f'^{base_table_name}_[0-9]+$'
            ))
            result = cursor.fetchone()
            return result[0] if result else 1
        except Exception as e:
            print(f"Ошибка при определении номера таблицы: {str(e)}")
            conn.rollback()
            return 1


@with_transaction
def create_new_table(conn, schema_name, base_table_name):
    """Создаёт новую таблицу с проверкой ошибок"""
    try:
        table_number = get_next_table_number(conn, schema_name, base_table_name)
        table_name = f"{base_table_name}_{table_number}"
        seq_name = f"s_{table_name}_id"

        with conn.cursor() as cursor:
            # Проверяем, не существует ли таблица
            cursor.execute(sql.SQL("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """), (schema_name, table_name))
            if cursor.fetchone():
                raise ValueError(f"Таблица {schema_name}.{table_name} уже существует")

            # Создаем последовательность
            cursor.execute(sql.SQL("""
                CREATE SEQUENCE IF NOT EXISTS {schema}.{seq_name}
            """).format(
                schema=sql.Identifier(schema_name),
                seq_name=sql.Identifier(seq_name)
            ))

            # Создаем таблицу
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    id BIGINT NOT NULL DEFAULT nextval('{schema}.{seq_name}'::regclass),
                    status BIGINT NOT NULL DEFAULT 0,
                    date_insert TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    account_number VARCHAR NOT NULL,
                    full_name VARCHAR NOT NULL,
                    address VARCHAR NOT NULL,
                    period_year VARCHAR NOT NULL,
                    period_month VARCHAR NOT NULL,
                    meter_reading VARCHAR NOT NULL,
                    debt BIGINT NOT NULL,
                    CONSTRAINT {pk_name} PRIMARY KEY (id)
                )
            """).format(
                schema=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name),
                seq_name=sql.Identifier(seq_name),
                pk_name=sql.Identifier(f"pk_{table_name}")
            ))

            print(f"Успешно создана таблица: {schema_name}.{table_name}")
            return table_name

    except Exception as e:
        print(f"Ошибка при создании таблицы: {str(e)}")
        raise


def get_db_connection_info(conn):
    """Возвращает информацию о подключении к БД"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT version()")
        db_version = cursor.fetchone()[0]

        cursor.execute("""
            SELECT current_database(), current_user, inet_client_addr(), inet_client_port()
        """)
        db_info = cursor.fetchone()

        cursor.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database()))
        """)
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
    print("===================================")


def get_load_status(conn, schema_name, table_name):
    """Возвращает статус загрузки таблицы"""
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("""
            SELECT 
                stage_bitmap,
                status_code,
                start_time,
                end_time,
                error_message
            FROM {schema}.t_load_stages
            WHERE table_name = %s
        """).format(schema=sql.Identifier(schema_name)),
                       (table_name,))

        result = cursor.fetchone()
        if not result:
            return None

        bitmap, status, start, end, error = result

        # Проверяем какие этапы выполнены (биты установлены)
        stages_status = {
            stage: bool(bitmap & flag)
            for stage, flag in LOAD_STAGES.items()
        }

        return {
            "stages": stages_status,
            "status": status,
            "start_time": start,
            "end_time": end,
            "error_message": error,
            "duration": (end - start).total_seconds() if end else None
        }


def load_data_to_new_table(conn, file_path, schema_name, table_name, encoding='utf-8'):
    """Загружает данные с проверкой существования таблицы"""
    print("\n=== НАЧАЛО ЗАГРУЗКИ ДАННЫХ ===")

    # Извлекаем имя таблицы без схемы (если оно было передано с схемой)
    table_name_only = table_name.split('.')[-1]
    full_table_name = f"{schema_name}.{table_name_only}"

    try:
        # 1. Проверка существования таблицы
        print(f"Проверка существования таблицы {full_table_name}...")
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """), (schema_name, table_name_only))

            if not cur.fetchone()[0]:
                raise ValueError(f"Таблица {full_table_name} не существует")

        # 2. Подсчет строк в файле
        with open(file_path, 'r', encoding=encoding) as f:
            total_rows = sum(1 for _ in f) - 1  # исключаем заголовок
        print(f"Всего строк для загрузки: {total_rows}")

        # 3. Создание временного файла без заголовка
        temp_file_path = f"{file_path}.temp"
        try:
            with open(file_path, 'r', encoding=encoding) as src, \
                    open(temp_file_path, 'w', encoding=encoding) as dst:
                next(src)  # Пропускаем заголовок
                for line in src:
                    dst.write(line)

            # 4. Загрузка данных
            print("Начало загрузки данных...")
            with open(temp_file_path, 'r', encoding=encoding) as f:
                with conn.cursor() as cursor:
                    cursor.copy_expert(
                        sql.SQL("""
                            COPY {schema}.{table} (
                                account_number, full_name, address, 
                                period_year, period_month, meter_reading, debt
                            ) FROM STDIN WITH (FORMAT csv, DELIMITER '\t')
                        """).format(
                            schema=sql.Identifier(schema_name),
                            table=sql.Identifier(table_name_only)
                        ),
                        f
                    )
            print("Данные успешно загружены")

            # 5. Проверка количества загруженных строк
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    SELECT COUNT(*) FROM {schema}.{table}
                """).format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name_only))
                )
                loaded_rows = cursor.fetchone()[0]
                print(f"Загружено строк: {loaded_rows}")

                if loaded_rows != total_rows:
                    raise ValueError(
                        f"Несоответствие количества строк (ожидалось: {total_rows}, загружено: {loaded_rows})")

            return True

        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    except Exception as e:
        print(f"Ошибка при загрузке данных: {str(e)}")
        raise


def main(file_path, schema_name):
    """Основная функция для загрузки данных из файла в новую таблицу.

    Args:
        file_path (str): Путь к файлу с данными.
        schema_name (str): Имя схемы в БД.

    Returns:
        int: 0 при успешном выполнении, 1 при ошибке.
    """
    total_start_time = time.time()
    print_system_info()
    validate_file(file_path)

    conn = None
    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        conn.autocommit = False

        ensure_log_tables_exist(conn, schema_name)
        print_db_info(conn)
        base_table_name = "t_GAZ"

        try:
            # 1. Создание таблицы
            print("\n=== 1. СОЗДАНИЕ ТАБЛИЦЫ ===")
            table_name = create_new_table(conn, schema_name, base_table_name)
            stage_log_id = create_load_stage_log(conn, schema_name, table_name)
            update_stage_status(conn, schema_name, stage_log_id, 'create_table')
            conn.commit()

            # 2. Загрузка данных
            print("\n=== 2. ЗАГРУЗКА ДАННЫХ ===")
            success = load_data_to_new_table(conn, file_path, schema_name, table_name)
            update_stage_status(conn, schema_name, stage_log_id, 'copy_data', success)
            conn.commit()

            # 3. Финализация
            print("\n=== 3. ФИНАЛИЗАЦИЯ ===")
            if success:
                update_stage_status(conn, schema_name, stage_log_id, 'finalize', success)
                conn.commit()
            print("Все этапы завершены успешно")

        except Exception as e:
            print(f"\nОШИБКА: {str(e)}")
            if 'stage_log_id' in locals():
                try:
                    update_stage_status(conn, schema_name, stage_log_id, 'finalize', False, str(e))
                    conn.commit()
                except Exception as log_error:
                    print(f"Ошибка при записи лога: {str(log_error)}")
            if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
            return 1

    except Exception as e:
        print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        return 1
    finally:
        if conn:
            conn.close()
        print(f"\nОбщее время выполнения: {time.time() - total_start_time:.2f} сек.")
    return 0


if __name__ == "__main__":
    file_path = "GAZ.tsv"
    schema_name = "GAZ"
    main(file_path, schema_name)