import abc
import logging
import pandas as pd

from ._decorator import connect
from .storage import SQLiteStorage
from .sql_builder import SQLiteSQLBuilder
from cetino.fs.csv.storage import CSVTableStorage
from .type import SQLiteDataType

default_primary_key_tuple = ('_id',)


class SQLiteTableStorage(SQLiteStorage, abc.ABC):
    """
    SQLite Table Manipulation, DO NOT instantiate this class directly.

    You have to override the following properties to define the table metadata:
    - fields: (dict) list of fields, e.g. {'name': SQLiteDataType.TEXT, 'age': SQLiteDataType.INTEGER}
    - table_name: (str) table name, e.g. 'employees'
    - (optional) primary_key_tuple: (tuple) primary key, e.g. ('id'), if not override, return default primary key
    - (optional) unique_tuple: (tuple) unique fields, e.g. ('name', 'age')
    """
    primary_key_tuple = default_primary_key_tuple
    unique_tuple = ()

    @property
    @abc.abstractmethod
    def fields(self) -> dict:
        """list of fields, e.g. {'name': SQLiteDataType.TEXT, 'age': SQLiteDataType.INTEGER}"""
        pass

    @property
    @abc.abstractmethod
    def table_name(self) -> str:
        """table name, e.g. 'employees'"""
        pass

    @property
    def field_names_list(self):
        return list(self.fields.keys())

    def __init__(self, data_path, log_path=None):
        """
        :param data_path: (str | pathlib.Path) database file path
        :param log_path: (str | pathlib.Path | None) log file path, if None, only print to console
        """
        super().__init__(data_path, log_path)
        self.ddl = None
        self._validate_table_metadata()

    @connect()
    def create(self, allow_exist: bool = False):
        """
        Create table.
        :return: None
        """
        full_fields = self.fields.copy()
        if self.primary_key_tuple == default_primary_key_tuple:
            for pk_item in self.primary_key_tuple:
                full_fields[pk_item] = SQLiteDataType.INTEGER
        self.ddl = SQLiteSQLBuilder.create_table_sql(table_name=self.table_name, fields_dict=full_fields,
                                                     primary_key_tuple=self.primary_key_tuple,
                                                     unique_tuple=self.unique_tuple,
                                                     allow_exist=allow_exist)
        self.execute(self.ddl)
        self._log(f'Table {self.table_name} created', level=logging.INFO)

    @connect()
    def drop(self, allow_not_exist=True):
        """
        Drop table.
        :return: None
        """
        sql = SQLiteSQLBuilder.drop_table_sql(self.table_name, allow_not_exist=allow_not_exist)
        self.execute(sql)
        self._log(f'Table {self.table_name} dropped', level=logging.INFO)

    @connect()
    def query(self, limit: int = None, offset: int = None, use_pandas: bool = False):
        """
        Get records with full fields from table.

        :param limit: (int)
        :param offset: (int)
        :param use_pandas: (bool) if True, return pandas.DataFrame, else return list of dict
        :return: (list<dict> | pd.DataFrame)
        """
        sql = SQLiteSQLBuilder.query_sql(table_name=self.table_name, fields_list=self.field_names_list,
                                         limit=limit, offset=offset)
        records = self.execute(sql)
        records = [dict(zip([field[0] for field in records.description], record)) for record in records]
        if use_pandas:
            records = pd.DataFrame(records)
            records.set_index(self.primary_key_tuple, inplace=True)
        return records

    @connect()
    def query_dump(self, save_path, limit: int = None, offset: int = None):
        """
        Dump records with full fields from table to csv file.

        :param save_path: (str | pathlib.Path) csv file path
        :param limit: (int)
        :param offset: (int)
        :return: None
        """
        records = self.query(limit=limit, offset=offset, use_pandas=False)
        csv_storage = self.get_csv_storage()
        self._log(f'Dump {len(records)} records to {save_path}...', level=logging.INFO)
        csv_storage.write(save_path, records)
        self._log(f'Dumping finished', level=logging.INFO)

    @connect(commit=True)
    def insert(self, record: dict):
        insert_sql = SQLiteSQLBuilder.insert_sql(table_name=self.table_name, data_dict_list=[record])
        cursor = self.execute(insert_sql)
        return cursor.rowcount

    @connect(commit=True)
    def insert_many(self, record_list: list):
        insert_sql = SQLiteSQLBuilder.insert_sql(table_name=self.table_name, data_dict_list=record_list)
        cursor = self.execute(insert_sql)
        return cursor.rowcount

    @connect(commit=True)
    def empty(self):
        delete_sql = SQLiteSQLBuilder.delete_sql(table_name=self.table_name)
        print(f"delete_sql: {delete_sql}")
        cursor = self.execute(delete_sql)
        return cursor.rowcount

    def get_csv_storage(self):
        return CSVTableStorage(fields=list(self.fields.keys()), index_col=self.primary_key_tuple)

    def _validate_table_metadata(self):
        self._validate_table_name()
        self._validate_fields()
        self._validate_unique_fields()
        self._validate_primary_key()

    def _validate_table_name(self):
        if not self.table_name:
            msg = 'Table name is not defined.'
            self._log(msg, level=logging.ERROR)
            raise ValueError(msg)

    def _validate_fields(self):
        if not self.fields or not isinstance(self.fields, dict) or len(self.fields) == 0:
            msg = 'Fields must be defined as a non-empty dictionary.'
            self._log(msg, level=logging.ERROR)
            raise ValueError(msg)

    def _validate_unique_fields(self):
        if self.unique_tuple:
            if not isinstance(self.unique_tuple, (list, tuple)):
                msg = 'The "unique_tuple" attribute must be a list or tuple.'
                self._log(msg, level=logging.ERROR)
                raise ValueError(msg)
            for field in self.unique_tuple:
                if field not in self.fields:
                    msg = f'Unique field "{field}" is not defined in the fields.'
                    self._log(msg, level=logging.ERROR)
                    raise ValueError(msg)

    def _validate_primary_key(self):
        # 1. primary key tuple cannot be empty
        if not self.primary_key_tuple or not isinstance(self.primary_key_tuple, (list, tuple)):
            msg = '`primary_key_tuple` must be a non-empty list or tuple.'
            self._log(msg, level=logging.ERROR)
            raise ValueError(msg)
        if self.primary_key_tuple != default_primary_key_tuple:
            # 2. user specified primary key must be defined in the fields
            for pk_item in self.primary_key_tuple:
                if pk_item not in self.fields:
                    msg = f'User specified primary key "{pk_item}" is not defined in the fields.'
                    self._log(msg, level=logging.ERROR)
                    raise ValueError(msg)


__all__ = ['SQLiteTableStorage']
