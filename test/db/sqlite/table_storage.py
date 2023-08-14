import os
import unittest
import threading
from pathlib import Path
from cetino.db.sqlite.type import SQLiteDataType
from cetino.db.sqlite.table_storage import SQLiteTableStorage

THREAD_COUNT = 100
RECORD_COUNT = 500

DB_FILE = 'test.db'
TABLE_NAME = 'test_table'


class TestStorage(SQLiteTableStorage):
    fields = {
        'name': SQLiteDataType.TEXT,
        'age': SQLiteDataType.INTEGER
    }
    table_name = TABLE_NAME


def threaded_insert(thread_id, db_file):
    storage_instance = TestStorage(db_file)
    data = [{'name': f'name_{thread_id}_{i}', 'age': i} for i in range(RECORD_COUNT)]

    with storage_instance:
        storage_instance.insert_many(data)


def threaded_query(db_file, expected_count=None):
    storage_instance = TestStorage(db_file)
    with storage_instance:
        n_records = len(storage_instance.query())
        if expected_count is not None:
            assert n_records == expected_count, f'expected {expected_count} records, got {n_records}'


class TestSQLiteTableStorage(unittest.TestCase):
    class TestStorage(SQLiteTableStorage):
        fields = {
            'name': SQLiteDataType.TEXT,
            'age': SQLiteDataType.INTEGER
        }
        table_name = TABLE_NAME

    def setUp(self):
        self.storage: SQLiteTableStorage = self.TestStorage(DB_FILE)
        """the test storage object"""
        with self.storage:
            self.storage.create()

    def tearDown(self):
        os.remove(DB_FILE)
        if Path('test.csv').exists():
            os.remove('test.csv')

    def test_validate_table_name(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = ""

            InvalidStorage(DB_FILE)

    def test_validate_fields(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {}
                table_name = 'invalid_table'

            with InvalidStorage(DB_FILE):
                pass

    def test_validate_unique_fields(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = 'invalid_table'
                unique_tuple = ('invalid_field',)

            InvalidStorage(DB_FILE)

    def test_validate_primary_key(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = 'invalid_table'
                primary_key_tuple = ('invalid_pk',)

            InvalidStorage(DB_FILE)

    def test_insert_and_query(self):
        with self.storage:
            self.storage.insert({'name': 'Alice', 'age': 25})
            result = self.storage.query(use_pandas=False)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')
        self.assertEqual(result[0]['age'], 25)

    def test_insert_many_and_query(self):
        with self.storage:
            data = [{'name': 'Alice', 'age': 25}, {'name': 'Bob', 'age': 30}]
            self.storage.insert_many(data)
            result = self.storage.query(use_pandas=False)
        self.assertEqual(len(result), 2)

    def test_query_dump(self):
        with self.storage:
            data = [{'name': 'Alice', 'age': 25}]
            self.storage.insert_many(data)
            self.storage.query_dump('test.csv')
        self.assertTrue(Path('test.csv').exists())


class MultiThreadedSQLiteTest(unittest.TestCase):

    def setUp(self):
        self.storage: SQLiteTableStorage = TestStorage(DB_FILE)
        with self.storage:
            self.storage.create()

    def tearDown(self):
        os.remove(DB_FILE)

    def insert_data(self):
        with self.storage:
            for _ in range(RECORD_COUNT):
                self.storage.insert({'name': 'Alice', 'age': 25})

    def query_data(self):
        with self.storage:
            self.storage.query()

    def test_multi_threaded_insert(self):
        db_file = DB_FILE

        threads = []

        # 创建多个线程，每个线程使用自己的SQLiteTableStorage实例
        for i in range(THREAD_COUNT):
            t = threading.Thread(target=threaded_insert, args=(i, db_file))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()  # 等待所有线程完成

        # 使用主线程的storage实例检查结果
        with self.storage:
            result = self.storage.query(use_pandas=False)

        self.assertEqual(len(result), THREAD_COUNT * RECORD_COUNT)

    def test_multi_threaded_query(self):
        # First, insert some data
        with self.storage:
            for _ in range(RECORD_COUNT):
                self.storage.insert({'name': 'Alice', 'age': 25})

        # Second, launching multi-threading read
        threads = []
        for _ in range(THREAD_COUNT):
            t = threading.Thread(target=threaded_query, args=(DB_FILE, RECORD_COUNT))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
