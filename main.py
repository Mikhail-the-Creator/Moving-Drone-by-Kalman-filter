import os
import glob
import requests
from zipfile import ZipFile
import pandas as pd
import yaml
import re
from pathlib import Path
from typing import Final
import tarfile
from collections import Counter


def remove_after_test(path):
    # Найти индекс строки 'test' и обрезать после нее
    index = path.find("test")
    if index != -1:
        return path[:index + len("test")]
    return path  # Если 'test' не найден, возвращаем исходную строку


# Функция загрузки файла по URL
def download_dataset(url, save_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            f.write(response.content)
        print(f"Dataset downloaded and saved to: {save_path}")
    else:
        print(f"Failed to download dataset: {response.status_code}")
        raise Exception(f"Failed to download file from {url}")


# Функция распаковки скачанного zip-файла
def unpack_zip(path_from, path_to):
    with ZipFile(path_from, 'r') as zip:
        # Покажет метаданные о содержимом zip-файла
        zip.printdir()
        # Распакует архив в указанную папку
        print('Extracting all the files now...')
        zip.extractall(path=path_to)
        print('Done!')


def unpack_tar_gz(path_from, path_to):
    with tarfile.open(path_from) as tar:
        print('Extracting all the files now...')
        tar.extractall(path=path_to, filter='data')
        print('Done!')


def process_log_file(file_path):
    unique_lines = set()  # Для хранения уникальных строк
    total_count = 0  # Для подсчета общего числа строк

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            total_count += 1  # Увеличиваем счетчик для каждой строки

            # Удаляем метки дат и времени с помощью регулярного выражения
            processed_line = re.sub(
                r'nova-api\.log\.\d+\.\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}', '', line
            )  # Убираем первую метку даты-времени
            processed_line = re.sub(
                r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}', '', processed_line
            )  # Убираем вторую метку даты-времени

            # Удаление лишних пробелов
            processed_line = processed_line.strip()

            # Добавляем обработанную строку в множество
            unique_lines.add(processed_line)

    # Возвращаем общее число строк и число уникальных строк
    return total_count, len(unique_lines)


def analyze_log_files_in_folder(folder_path):
    # Словарь для хранения результатов
    results = {}

    # Перебираем все .log файлы в папке
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".log"):  # Проверяем расширение файла
            file_path = os.path.join(folder_path, file_name)

            # Анализируем текущий лог-файл
            total_count, unique_count = process_log_file(file_path)

            # Сохраняем результат для текущего файла
            results[file_name] = (total_count, unique_count)

    return results


import re


def count_lines(file_path):
    date_time_pattern = re.compile(r"\b\d{4}[.-]\d{2}[.-]\d{2}\b|\b\d{2}[.:]\d{2}[.:]\d{2}\b")
    unique_lines = set()
    total_lines = 0

    with open(file_path, "r") as file:
        for line in file:
            total_lines += 1  # Считаем каждую строку
            # Удаляем дату и время из строки
            cleaned_line = date_time_pattern.sub("", line).strip()
            unique_lines.add(cleaned_line)

    return total_lines, len(unique_lines)


import os
import re


def process_logs_bgl(directory):
    total_lines = 0
    unique_lines = set()

    # Регулярное выражение для удаления метки даты и времени
    datetime_pattern = r'\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+'

    # Перебираем все файлы в директории
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.log'):  # Работать только с .log файлами
                file_path = os.path.join(root, file)
                print(f"Обработка файла: {file}")  # Добавить подпись обрабатываемого файла
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        total_lines += 1
                        # Удаляем метку даты и времени
                        cleaned_line = re.sub(datetime_pattern, '', line).strip()
                        unique_lines.add(cleaned_line)
    return total_lines, len(unique_lines)


import os
from collections import Counter


def analyze_log_files_HDFS(dir_path):
    """
    Анализирует .log файлы в указанной директории и выводит результаты анализа
    (общее количество строк и количество уникальных строк) для каждого файла.

    :param dir_path: Путь к директории с лог-файлами.
    """
    try:
        # Проверяем, что директория существует
        if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
            print(f"Путь '{dir_path}' не существует или не является директорией.")
            return

        # Для каждого файла с расширением .log
        for filename in os.listdir(dir_path):
            if filename.endswith(".log"):
                file_path = os.path.join(dir_path, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        # Считаем строки с помощью Counter
                        counter = Counter(file)
                        total_lines = sum(counter.values())  # Общее количество строк
                        unique_lines = len(counter.keys())  # Число уникальных строк

                        # Выводим результаты анализа прямо из функции

                except Exception as e:
                    print(f"Ошибка при чтении файла '{file_path}': {e}")

        print("Анализ завершен.")
    except Exception as e:
        print(f"Ошибка при обработке директории: {e}")

    return total_lines, unique_lines


URLs = {'HDFS': "https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1",
        'OpenStack': "https://zenodo.org/records/8196385/files/OpenStack.tar.gz?download=1",
        'Hadoop': "https://zenodo.org/records/8196385/files/Hadoop.zip?download=1",
        'BGL': "https://zenodo.org/records/8196385/files/BGL.zip?download=1"}

home_path_1 = "E:\\Common\\Desktop\\test\\HDFS_v1.zip"
home_path_2 = "E:\\Common\\Desktop\\test\\OpenStack.tar.gz"
home_path_3 = "E:\\Common\\Desktop\\test\\Hadoop.zip"
home_path_4 = "E:\\Common\\Desktop\\test\\BGL.zip"

print(f"Какой датасет нужно скачать: 1-HDFS, 2-OpenStack, 3-Hadoop, 4-BGL")
num_of_dataset = int(input("Введи цифру нужного датасета: "))
match num_of_dataset:
    case 1:
        url = URLs['HDFS']
        home_path = home_path_1
        home_path_without = remove_after_test(home_path)
    case 2:
        url = URLs['OpenStack']
        home_path = home_path_2
        home_path_without = remove_after_test(home_path)
    case 3:
        url = URLs['Hadoop']
        home_path = home_path_3
        home_path_without = remove_after_test(home_path)
    case 4:
        url = URLs['BGL']
        home_path = home_path_4
        home_path_without = remove_after_test(home_path)
    case _:
        print("Неизвестное число")

download_dataset(url, home_path)
match num_of_dataset:
    case 1:
        unpack_zip(home_path, home_path_without)
        a1, a2 = analyze_log_files_HDFS(home_path_without)
        hdfs_df = pd.DataFrame({"Файл": "HDFS.log",
                                "Всего строк": [a1],
                                "Уникальных строк": [a2]})
        print(hdfs_df)
    case 2:
        unpack_tar_gz(home_path, home_path_without)
        log_file_path = "E:\\Common\\Desktop\\test"
        results = analyze_log_files_in_folder(log_file_path)
        for file_name, (total_count, unique_count) in results.items():
            a1, a2 = total_count, unique_count
            openstack_df = pd.DataFrame({"Файл": file_name,
                                    "Всего строк": [a1],
                                    "Уникальных строк": [a2]})
            print(openstack_df)


    case 3:
        unpack_zip(home_path, home_path_without)

    case 4:
        unpack_zip(home_path, home_path_without)
        a1, a2 = process_logs_bgl(home_path_without)
        bgl_df = pd.DataFrame({"Файл": "BGL.log",
                                "Всего строк": [a1],
                                "Уникальных строк": [a2]})
        print(bgl_df)




