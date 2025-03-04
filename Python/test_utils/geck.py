import random
from faker import Faker

# Инициализация Faker для генерации имен и адресов
fake = Faker('ru_RU')  # 'ru_RU' для русских имен и адресов

def generate_unique_account_number(existing_numbers):
    while True:
        number = ''.join([str(random.randint(0, 9)) for _ in range(9)])
        if number not in existing_numbers:
            existing_numbers.add(number)
            return number

def generate_full_name():
    return fake.name()

def generate_address():
    return fake.address().replace("\n", ", ")

def generate_period_year():
    return str(random.randint(2000, 2023))

def generate_period_month():
    return f"{random.randint(1, 12):02d}"

def generate_meter_reading():
    return ''.join([str(random.randint(0, 9)) for _ in range(5)])

def generate_debt_in_kopecks():
    return int(round(random.uniform(0, 10000) * 100))  # Сумма в копейках

def generate_data(num_records):
    data = []
    existing_account_numbers = set()  # Множество для хранения уникальных номеров счетов
    for _ in range(num_records):
        record = {
            "Счет": generate_unique_account_number(existing_account_numbers),
            "ФИО": generate_full_name(),
            "Адрес": generate_address(),
            "Период год": generate_period_year(),
            "Период месяц": generate_period_month(),
            "Показание счетчика": generate_meter_reading(),
            "Задолженность": generate_debt_in_kopecks()
        }
        data.append(record)
    return data

# Генерация 10 000 записей
data = generate_data(100000)

# Сохранение данных в файл с разделителем TAB
filename = "data_100000.tsv"
with open(filename, mode="w", encoding="utf-8", newline="") as file:
    for record in data:
        line = (
            f'"{record["Счет"]}"\t'
            f'"{record["ФИО"]}"\t'
            f'"{record["Адрес"]}"\t'
            f'"{record["Период год"]}"\t'
            f'"{record["Период месяц"]}"\t'
            f'"{record["Показание счетчика"]}"\t'
            f'"{record["Задолженность"]}"\n'
        )
        file.write(line)

print(f"Данные сохранены в файл {filename}")