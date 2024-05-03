import os
from typing import Union, List, Dict

import dotenv
import psycopg2


class PostgresDb:
    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            dotenv.load_dotenv()
            cls._instance = super().__new__(cls)
            cls._connection = psycopg2.connect(
                dbname=os.getenv('DATABASE_NAME'),
                host=os.getenv('DATABASE_HOST'),
                user=os.getenv('DATABASE_USERNAME'),
                password=os.getenv('DATABASE_PASSWORD'),
                port=os.getenv('DATABASE_PORT'),
            )
            cls._connection.autocommit = True
        return cls._instance

    def select(self, table_name: str, columns=None, where=None) -> Union[List, Dict]:
        '''
        Выбирает данные из указанной таблицы по заданным столбцам и условиям.

        Параметры:
            table_name (str): Название таблицы, из которой производится выборка.
            columns (list or tuple, optional): Список столбцов для выборки. Если не указан, выбираются все столбцы.
            where (str, optional): Условие для фильтрации строк. Если не указано, применяется к каждой строке.
        Возвращает:
            Union[List, Dict]: В случае успешного выполнения - List, иначе - словарь с информацией об ошибке.
        '''
        if not isinstance(table_name, str):
            return {'status': 'Error', 'message': 'Argument table_name (name of table) need to be type string'}
        if columns is not None and not isinstance(columns, list) and not isinstance(columns, tuple):
            return {'status': 'Error', 'message': 'Argument columns (list of columns) need to be types list or tuple'}
        if where is not None and not isinstance(where, str):
            return {'status': 'Error', 'message': 'Argument where (condition for filtering) need to be type string'}

        cursor = self._connection.cursor()
        # Вывод всех столбцов таблицы
        if columns is None:
            query = f'SELECT * FROM {table_name}'
            if where is not None:
                query += f' WHERE {where}'
                cursor.execute(query)
                return cursor.fetchall()
            cursor.execute(query)
            return cursor.fetchall()
        # Вывод определенных столбцов таблицы
        columns = ', '.join(columns)
        query = f'SELECT {columns} FROM {table_name}'
        if where is not None:
            query += f' WHERE {where}'
            cursor.execute(query)
            return cursor.fetchall()
        cursor.execute(query)
        return cursor.fetchall()

    def insert(self, table_name: str, columns_and_values: dict) -> Union[None, Dict]:
        '''
        Вставляет новую запись в указанную таблицу с указанными значениями для столбцов.

        Параметры:
            table_name (str): Название таблицы, в которую вставляются данные.
            columns_and_values (dict): Словарь, где ключи - названия столбцов, а значения - соответствующие значения для вставки.
        Возвращает:
            Union[None, Dict]: В случае успешного выполнения - None, иначе - словарь с информацией об ошибке.
        '''
        if not isinstance(table_name, str):
            return {'status': 'Error', 'message': 'Argument table_name (name of table) need to be type string'}
        if not isinstance(columns_and_values, dict):
            return {'status': 'Error', 'message': 'Argument columns_and_values (columns and it\'s values) need to be type dict'}

        cursor = self._connection.cursor()
        # Собираем все ключи для строки с названием столбцов
        columns = columns_and_values.keys()
        for column in columns:
            if not isinstance(column, str):
                return {'status': 'Error', 'message': 'All keys of columns_and_values (columns and it\'s values) need to be type string'}
        columns = ', '.join(columns)
        # Собираем все значения для строки со значениями
        values = list()
        for value in list(columns_and_values.values()):
            if not isinstance(value, str):
                values.append(str(value))
                continue
            values.append(f'"{value}"')
        values = ', '.join(values)
        # Собираем запрос
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'
        cursor.execute(query)

    def update(self, table_name: str, columns_and_values: dict, where=None) -> Union[None, Dict]:
        '''
        Обновляет данные в указанной таблице согласно заданным значениям и условиям.

        Параметры:
            table_name (str): Название таблицы, которая обновляется.
            columns_and_values (dict): Словарь, где ключи - названия столбцов, а значения - соответствующие значения для обновления.
            where (str, optional): Условие для фильтрации строк, которые нужно обновить. Если не указано, обновление происходит для всех строк.
        Возвращает:
            Union[None, Dict]: В случае успешного выполнения - None, иначе - словарь с информацией об ошибке.
        '''
        if not isinstance(table_name, str):
            return {'status': 'Error', 'message': 'Argument table_name (name of table) need to be type string'}
        if not isinstance(columns_and_values, dict):
            return {'status': 'Error', 'message': 'Argument columns_and_values (columns and it\'s values) need to be type dict'}
        if where is not None and not isinstance(where, str):
            return {'status': 'Error', 'message': 'Argument where (condition for filtering) need to be type string'}

        cursor = self._connection.cursor()
        # Собираем все ключи для строки с названием столбцов
        columns = columns_and_values.keys()
        for column in columns:
            if not isinstance(column, str):
                return {'status': 'Error', 'message': 'All keys of columns_and_values (columns and it\'s values) need to be type string'}
        # Собираем все значения для строки со значениями
        values = list()
        for value in list(columns_and_values.values()):
            if not isinstance(value, str):
                values.append(str(value))
                continue
            values.append(f'"{value}"')
        # Собираем запрос
        columns_and_values = ', '.join(f"{column}={value}" for column, value in zip(columns, values))
        query = f'UPDATE {table_name} SET {columns_and_values}'
        if where is not None:
            query += f' WHERE {where}'
        cursor.execute(query)

    def delete(self, table_name: str, where: str) -> Union[None, Dict]:
        '''
        Удаляет строки из указанной таблицы в соответствии с заданным условием.

        Параметры:
            table_name (str): Название таблицы, из которой удаляются данные.
            where (str): Условие для фильтрации строк, которые нужно удалить.
        Возвращает:
            Union[None, Dict]: В случае успешного выполнения - None, иначе - словарь с информацией об ошибке.
        '''
        if not isinstance(table_name, str):
            return {'status': 'Error', 'message': 'Argument table_name (name of table) need to be type string'}
        if not isinstance(where, str):
            return {'status': 'Error', 'message': 'Argument where (condition for filtering) need to be type string'}
        
        cursor = self._connection.cursor()
        # Собираем запрос
        query = f'DELETE FROM {table_name} WHERE {where}'
        cursor.execute(query)
    
    def clear_table(self, table_name: str) -> Union[None, Dict]:
        '''
        Очищает указанную таблицу, удаляя все записи из неё.

        Параметры:
            table_name (str): Название таблицы, которую необходимо очистить.
        Возвращает:
            Union[None, Dict]: В случае успешного выполнения - None, иначе - словарь с информацией об ошибке.
        '''
        if not isinstance(table_name, str):
            return {'status': 'Error', 'message': 'Argument table_name (name of table) need to be type string'}

        cursor = self._connection.cursor()
        # Собираем запрос
        query = f'DELETE FROM {table_name}'
        cursor.execute(query)
