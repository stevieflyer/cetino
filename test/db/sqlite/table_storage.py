import os
import unittest
from pathlib import Path
from cetino.db.sqlite.type import SQLiteDataType
from cetino.db.sqlite.table_storage import SQLiteTableStorage


class TestSQLiteTableStorage(unittest.TestCase):
    class TestStorage(SQLiteTableStorage):
        fields = {
            'name': SQLiteDataType.TEXT,
            'age': SQLiteDataType.INTEGER
        }
        table_name = 'test_table'

    def setUp(self):
        self.storage = self.TestStorage('test.db')
        with self.storage:
            self.storage.create()

    def tearDown(self):
        os.remove('test.db')
        if Path('test.csv').exists():
            os.remove('test.csv')

    def test_validate_table_name(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = ''

            InvalidStorage('test.db')

    def test_validate_fields(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {}
                table_name = 'invalid_table'

            with InvalidStorage('test.db'):
                pass

    def test_validate_unique_fields(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = 'invalid_table'
                unique_tuple = ('invalid_field',)

            InvalidStorage('test.db')

    def test_validate_primary_key(self):
        with self.assertRaises(ValueError):
            class InvalidStorage(SQLiteTableStorage):
                fields = {'name': SQLiteDataType.TEXT}
                table_name = 'invalid_table'
                primary_key_tuple = ('invalid_pk',)

            InvalidStorage('test.db')

    def test_insert_and_query(self):
        self.storage.insert({'name': 'Alice', 'age': 25})
        result = self.storage.query(use_pandas=False)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')
        self.assertEqual(result[0]['age'], 25)

    def test_insert_many_and_query(self):
        data = [{'name': 'Alice', 'age': 25}, {'name': 'Bob', 'age': 30}]
        self.storage.insert_many(data)
        result = self.storage.query(use_pandas=False)
        self.assertEqual(len(result), 2)

    def test_query_dump(self):
        data = [{'name': 'Alice', 'age': 25}]
        self.storage.insert_many(data)
        self.storage.query_dump('test.csv')
        self.assertTrue(Path('test.csv').exists())


if __name__ == '__main__':
    unittest.main()
