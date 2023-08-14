import unittest
from enum import Enum

from cetino.db.sqlite.sql_builder import BaseSQLBuilder, SQLiteSQLBuilder


class FieldType(Enum):
    TEXT = "TEXT"
    INTEGER = "INTEGER"


class TestBaseSQLBuilder(unittest.TestCase):

    def test_primary_key_sql(self):
        result = BaseSQLBuilder.primary_key_sql(("id", "name"))
        print(f"[test_primary_key_sql] result: {result}, expected: PRIMARY KEY (id, name)")
        self.assertEqual(result, "PRIMARY KEY (id, name)")

    def test_fields_declare_sql(self):
        fields = {"name": FieldType.TEXT, "age": FieldType.INTEGER, "id": FieldType.INTEGER}
        result = BaseSQLBuilder.fields_declare_sql(fields)
        print(f"[test_fields_declare_sql] result: {result}, expected: name TEXT,\nage INTEGER,\nid INTEGER")
        self.assertEqual(result, "name TEXT,\nage INTEGER,\nid INTEGER")

    def test_unique_sql(self):
        result = BaseSQLBuilder.unique_sql(("name",))
        print(f"[test_unique_sql] result: {result}, expected: UNIQUE (name)")
        self.assertEqual(result, "UNIQUE (name)")

    def test_where_clause(self):
        conditions = ['age > 20', 'name = "John"']
        result = BaseSQLBuilder.where_clause(conditions)
        print(f"[test_where_clause] result: {result}, expected: WHERE age > 20 AND name = \"John\"")
        self.assertEqual(result, 'WHERE age > 20 AND name = "John"')


class TestSQLiteSQLBuilder(unittest.TestCase):

    def test_query_sql(self):
        table_name = "users"
        fields = ["name", "age"]
        conditions = ['age > 20']
        order_by = {"age": "DESC"}
        limit = 10
        result = SQLiteSQLBuilder.query_sql(table_name, fields, conditions, order_by, limit)
        print(f"[test_query_sql] result: {result}, expected: SELECT name, age\nFROM users\nWHERE age > 20\nORDER BY age DESC\nLIMIT 10;")
        expected_sql = 'SELECT name, age\nFROM users\nWHERE age > 20\nORDER BY age DESC\nLIMIT 10;'
        self.assertEqual(result, expected_sql)

    def test_insert_sql(self):
        table_name = "users"
        data = [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]
        result = SQLiteSQLBuilder.insert_sql(table_name, data)
        print(f"[test_insert_sql] result: {result}, expected: INSERT INTO users (name, age) VALUES (\"John\", 25), (\"Jane\", 30);")
        expected_sql = 'INSERT INTO users (name, age) VALUES ("John", 25), ("Jane", 30);'
        self.assertEqual(result, expected_sql)

    def test_create_table_sql(self):
        table_name = "users"
        fields = {"name": FieldType.TEXT, "age": FieldType.INTEGER}
        primary_key = ("id",)
        unique = ("name",)
        result = SQLiteSQLBuilder.create_table_sql(table_name, fields, primary_key, unique)
        print(f"[test_create_table_sql] result:\n{result}, expected:\nCREATE TABLE IF NOT EXISTS users (\nname TEXT,\nage INTEGER,\nPRIMARY KEY (id),\nUNIQUE (name)\n);")
        expected_sql = '''CREATE TABLE IF NOT EXISTS users (
name TEXT,
age INTEGER,
PRIMARY KEY (id),
UNIQUE (name)
);'''
        self.assertEqual(result, expected_sql)


if __name__ == "__main__":
    unittest.main()
