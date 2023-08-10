import abc
import uuid
import logging
import sqlite3
import pandas as pd
from sqlite3 import Error

from .data_type import SQLiteDataType
from .sql_builder import SQLiteSQLBuilder
from cetino.fs.csv import CSVTableStorage
from cetino.utils.io_utils import ensure_pathlib_path
from cetino.utils.log_utils import get_console_only_logger, get_logger


def connect(commit=False):
    """
    Decorator for managing connection with SQLite Database
    """

    def decorator(func):
        def wrapper(self, *args, **kwargs):
            # Before calling the decorated function, connect to database
            if self.is_connect is False:
                self._connect()
            result = func(self, *args, **kwargs)
            # After calling the decorated function, commit and disconnect from database if commit is True
            if commit:
                self._commit()
            self._disconnect()
            return result

        return wrapper

    return decorator


class SQLiteStorage:
    def __init__(self, data_path, log_path=None):
        """
        Initialization.
        - Create or attach to a sqlite database file.
        - Create a single connection for reuse.(SQLite only support single-thread operation)

        :param data_path: (str | pathlib.Path) database file path
        :param log_path: (str | pathlib.Path | None) log file path, if None, only print to console
        :return None
        """
        self._uuid = str(uuid.uuid4())
        self._logger = self.__configure_logger(log_path)
        self._db_file = ensure_pathlib_path(data_path)
        self._conn = None
        """Single connection for reuse."""
        self.is_connect = False
        """State variable to indicate whether the connection is established."""
        self._connect()

    def execute(self, sql: str):
        """Execute SQL statement."""
        if self._conn is None:
            raise ConnectionError(
                f"Connection with {self._db_file} is not established, You have to call `_connect()` first before calling `execute()`.")

        c = self._conn
        try:
            result = c.execute(sql)
            self._log(f'Execute SQL: {sql}', level=logging.INFO)
            return result
        except Error as e:
            self._log(f'Execute SQL: {sql} failed', level=logging.ERROR)
            self._log(str(e), level=logging.ERROR)
            raise RuntimeError(f'Execute SQL: {sql} failed')

    @property
    def db_file(self):
        return self._db_file

    def _connect(self):
        self._conn = sqlite3.connect(self._db_file)
        self.is_connect = True
        self._log(f'Connection with {self._db_file} established', level=logging.INFO)

    def _commit(self):
        self._conn.commit()
        self._log(f'Commit changes to {self._db_file}', level=logging.INFO)

    def _disconnect(self):
        self._conn.close()
        self.is_connect = False
        self._log(f'Connection with {self._db_file} closed', level=logging.INFO)

    def _log(self, message: str, level=logging.INFO, **kwargs):
        # log message at the specified level
        self._logger.log(level, message, **kwargs)

    def __configure_logger(self, log_path):
        if log_path is None:
            logger = get_console_only_logger(self._logger_name)
        else:
            logger = get_logger(self._logger_name, log_path=log_path, console=False)
        return logger

    @property
    def _logger_name(self):
        return f"{self.__class__.__name__}_{self._uuid}_logger"

    def __str__(self):
        return f"{self.__class__.__name__}({self._db_file})"

    def __repr__(self):
        return self.__str__()


class SQLiteTableStorage(SQLiteStorage, abc.ABC):
    """
    SQLite Table Manipulation, DO NOT instantiate this class directly.

    You have to override the following properties to define the table metadata:
    - fields: (dict) list of fields, e.g. {'name': SQLiteDataType.TEXT, 'age': SQLiteDataType.INTEGER}
    - table_name: (str) table name, e.g. 'employees'
    - (optional) primary_key_tuple: (tuple) primary key, e.g. ('id'), if not override, return default primary key
    - (optional) unique_tuple: (tuple) unique fields, e.g. ('name', 'age')
    """
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
    def primary_key_tuple(self) -> tuple:
        """primary key, e.g. ('id'), if not override, return default primary key"""
        return f'_{self.table_name}_pt',

    @property
    def unique_tuple(self) -> tuple:
        """unique fields, e.g. ('name', 'age')"""
        return ()

    @property
    def field_names_list(self):
        return list(self.fields.keys())

    def __init__(self, data_path, log_path=None):
        """
        :param data_path: (str | pathlib.Path) database file path
        :param log_path: (str | pathlib.Path | None) log file path, if None, only print to console
        """
        super().__init__(data_path, log_path)
        self._validate_table_metadata()

    @connect()
    def create(self):
        """
        Create table.
        :return: None
        """
        sql = SQLiteSQLBuilder.create_table_sql(table_name=self.table_name, fields_dict=self.fields,
                                                primary_key_tuple=self.primary_key_tuple, unique_tuple=self.unique_tuple)
        self.execute(sql)
        self._log(f'Table {self.table_name} created', level=logging.INFO)

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
        sql = SQLiteSQLBuilder.query_sql(table_name=self.table_name, fields_list=self.field_names_list,
                                         limit=limit, offset=offset)
        records = self.execute(sql)
        records = [dict(zip([field[0] for field in records.description], record)) for record in records]
        csv_storage = self.get_csv_storage()
        self._log(f'Dump {len(records)} records to {save_path}...', level=logging.INFO)
        csv_storage.write(save_path, records)
        self._log(f'Dumping finished', level=logging.INFO)

    @connect(commit=True)
    def insert(self, data_dict: dict):
        """
        Insert data into table.

        :param data_dict: (dict) data to insert, e.g. {'name': 'John', 'age': 25}
        :return: (int)　number of inserted rows
        """
        insert_sql = SQLiteSQLBuilder.insert_sql(table_name=self.table_name, data_dict_list=[data_dict])
        return self.execute(insert_sql)

    @connect(commit=True)
    def insert_many(self, data_list: list):
        insert_sql = SQLiteSQLBuilder.insert_sql(table_name=self.table_name, data_dict_list=data_list)
        return self.execute(insert_sql)

    def get_csv_storage(self):
        """
        Get CSV storage for this table.

        :return: (CSVTableStorage)
        """
        return CSVTableStorage(fields=list(self.fields.keys()), index_col=self.primary_key_tuple)

    def _validate_table_metadata(self):
        """
        Validate table metadata.
        """

        # Validate table_name: Ensure table name is defined.
        if not self.table_name:
            raise ValueError('Table name must be defined.')

        # Validate fields: Ensure fields are defined and structured correctly.
        if not self.fields or not isinstance(self.fields, dict) or len(self.fields) == 0:
            raise ValueError('Fields must be defined as a non-empty dictionary.')

        # Validate unique fields: Ensure they are in the fields and are structured correctly.
        if self.unique_tuple:
            if not isinstance(self.unique_tuple, (list, tuple)):
                raise ValueError('The "unique_tuple" attribute must be a list or tuple.')

            for field in self.unique_tuple:
                if field not in self.fields:
                    raise ValueError(f'Unique field "{field}" is not defined in the fields.')

        # Validate primary key: Ensure it's in the fields and has the correct type.
        if not self.primary_key_tuple:
            raise ValueError("Primary key is not defined.")

        if self.primary_key_tuple not in self.fields:
            raise ValueError(f'Primary key "{self.primary_key_tuple}" is not defined in the fields.')

        primary_key_data_type = self.fields[self.primary_key_tuple].value
        if primary_key_data_type != SQLiteDataType.INTEGER.value:
            raise ValueError(
                f'Primary key "{self.primary_key_tuple}" must be of type SQLiteDataType.INTEGER. Found: {primary_key_data_type}.')
