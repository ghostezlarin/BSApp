import random


def generate_new_period_year():
    return str(random.randint(2023, 2025))


def generate_new_period_month():
    return f"{random.randint(1, 3):02d}"


def generate_new_meter_reading():
    return ''.join([str(random.randint(0, 9)) for _ in range(7)])


def generate_new_debt_in_kopecks():
    return int(round(random.uniform(0, 10000) * 100))  # Сумма в копейках


def modify_data(input_file):
    # Чтение данных из файла
    with open(input_file, mode="r", encoding="utf-8") as infile:
        lines = infile.readlines()  # Читаем все строки

    # Изменение данных
    modified_lines = []
    for line in lines:
        # Разделяем строку по TAB
        parts = line.strip().split("\t")

        # Оставляем первые три поля (счет, ФИО, адрес) без изменений
        account_number = parts[0]
        full_name = parts[1]
        address = parts[2]

        # Генерируем новые значения для оставшихся полей
        new_period_year = generate_new_period_year()
        new_period_month = generate_new_period_month()
        new_meter_reading = generate_new_meter_reading()
        new_debt = generate_new_debt_in_kopecks()

        # Формируем новую строку с кавычками вокруг каждого поля
        new_line = (
            f'"{account_number}"\t'
            f'"{full_name}"\t'
            f'"{address}"\t'
            f'"{new_period_year}"\t'
            f'"{new_period_month}"\t'
            f'"{new_meter_reading}"\t'
            f'"{new_debt}"\n'
        )
        modified_lines.append(new_line)

    # Перезапись файла новыми данными
    with open(input_file, mode="w", encoding="utf-8") as outfile:
        outfile.writelines(modified_lines)

    print(f"Данные изменены и сохранены в файл {input_file}")


# Пример использования
input_file = "data.tsv"  # Исходный файл
modify_data(input_file)