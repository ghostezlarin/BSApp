import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
from db_config import user, password, host, port, database, SCHEMA_NAME


class SchemaCreator:
    def __init__(self, db_params):
        """
        Инициализация подключения к базе данных
        :param db_params: параметры подключения к БД (словарь)
        """
        self.db_params = db_params
        self.connection = None
        self.cursor = None

    def connect(self):
        """Установка соединения с базой данных"""
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()
            print("Подключение к базе данных успешно установлено.")
        except Exception as e:
            print(f"Ошибка при подключении к базе данных: {e}")
            raise

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Соединение с базой данных закрыто.")

    def is_valid_mnemonic(self, mnemonic):
        """
        Проверка валидности мнемокода
        :param mnemonic: мнемокод (строка)
        :return: True если валидный, False если нет
        """
        # Проверяем длину (8 символов) и допустимые символы (буквы, цифры, подчеркивание)
        return bool(re.match(r'^[a-zA-Z0-9_]{1,8}$', mnemonic))

    def mnemonic_exists(self, mnemonic):
        """
        Проверка существования мнемокода в таблице t_organizations
        :param mnemonic: мнемокод (строка)
        :return: True если существует, False если нет
        """
        try:
            query = sql.SQL("SELECT 1 FROM main.t_organizations WHERE code_mnemonic = %s LIMIT 1")
            self.cursor.execute(query, (mnemonic,))
            return bool(self.cursor.fetchone())
        except Exception as e:
            print(f"Ошибка при проверке мнемокода: {e}")
            raise

    def create_schema(self, mnemonic):
        """
        Создание схемы с именем мнемокода
        :param mnemonic: мнемокод (строка)
        :return: True если успешно, False если нет
        """
        try:
            # Проверяем валидность мнемокода
            if not self.is_valid_mnemonic(mnemonic):
                print("Ошибка: мнемокод должен состоять из 8 символов (буквы, цифры, подчеркивание)")
                return False

            # Проверяем существование мнемокода в таблице
            if not self.mnemonic_exists(mnemonic):
                print("Ошибка: мнемокод не найден в таблице t_organizations")
                return False

            # Проверяем, существует ли уже схема с таким именем
            check_schema_query = sql.SQL("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s")
            self.cursor.execute(check_schema_query, (mnemonic.lower(),))
            if self.cursor.fetchone():
                print(f"Схема '{mnemonic}' уже существует")
                return True

            # Создаем новую схему
            create_schema_query = sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(mnemonic))
            self.cursor.execute(create_schema_query)
            print(f"Схема '{mnemonic}' успешно создана")
            return True
        except Exception as e:
            print(f"Ошибка при создании схемы: {e}")
            return False


def main():
    # Параметры подключения к базе данных (замените на свои)
    db_params = {
        'host': host,
        'database': database,
        'user': user,
        'password': password,
        'port': port
    }

    # Создаем экземпляр класса
    schema_creator = SchemaCreator(db_params)

    try:
        # Устанавливаем соединение
        schema_creator.connect()

        # Запрашиваем мнемокод у пользователя
        mnemonic = input("Введите мнемокод (8 символов): ").strip()

        # Пытаемся создать схему
        schema_creator.create_schema(mnemonic)

    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        # Закрываем соединение в любом случае
        schema_creator.close()


if __name__ == "__main__":
    main()