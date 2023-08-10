import enum
import uuid
import logging
import sqlite3
import pandas as pd
from sqlite3 import Error

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


class SQLiteDataType(enum.Enum):
    INTEGER = 'INTEGER'
    REAL = 'REAL'
    TEXT = 'TEXT'
    BLOB = 'BLOB'


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
        self.is_connect = False
        """State variable to indicate whether the connection is established."""
        """Single connection for reuse."""
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


class SQLiteTableStorage(SQLiteStorage):
    """
    SQLite Table Manipulation
    """

    fields = None
    """list of fields, e.g. {'name': SQLiteDataType.TEXT, 'age': SQLiteDataType.INTEGER}"""
    primary_key = None
    """primary key, e.g. 'id'"""
    unique = None
    """unique fields, e.g. ['name']"""
    table_name = None
    """table name, e.g. 'employees'"""

    def __init__(self, data_path, log_path=None):
        """
        :param data_path: (str | pathlib.Path) database file path
        :param log_path: (str | pathlib.Path | None) log file path, if None, only print to console
        """
        super().__init__(data_path, log_path)
        self._validate()

    @connect()
    def create(self):
        """
        Create table.
        :return: None
        """
        self.execute(self._create_table_sql)
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
        sql = self._query_sql(limit=limit, offset=offset)
        records = self.execute(sql)
        records = [dict(zip([field[0] for field in records.description], record)) for record in records]
        if use_pandas:
            records = pd.DataFrame(records)
            records.set_index(self.primary_key, inplace=True)
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
        sql = self._query_sql(limit=limit, offset=offset)
        records = self.execute(sql)
        records = [dict(zip([field[0] for field in records.description], record)) for record in records]
        csv_storage = self.get_csv_storage()
        self._log(f'Dump {len(records)} records to {save_path}...', level=logging.INFO)
        csv_storage.write(save_path, records)
        self._log(f'Dumping finished', level=logging.INFO)

    def get_csv_storage(self):
        """
        Get CSV storage for this table.

        :return: (CSVTableStorage)
        """
        return CSVTableStorage(fields=list(self.fields.keys()), index_col=self.primary_key)

    @connect(commit=True)
    def insert(self, data_dict: dict):
        """
        Insert data into table.

        :param data_dict: (dict) data to insert, e.g. {'name': 'John', 'age': 25}
        :return: (int)　number of inserted rows
        """
        insert_sql = self._insert_sql([data_dict])
        return self.execute(insert_sql)

    @connect(commit=True)
    def insert_many(self, data_list: list):
        insert_sql = self._insert_sql(data_list)
        return self.execute(insert_sql)

    def _query_sql(self, fields=None, where=None, order_by=None, limit=None, offset=None):
        """
        Generate query sql.

        :param fields: (list) fields to query, e.g. ['name', 'age']
        :param where: (str) where clause, e.g. 'age > 20'
        :param order_by: (str) order by clause, e.g. 'age DESC'
        :param limit: (int) limit clause, e.g. 10
        :return: (str) query sql
        sql =
        """
        if not fields:
            fields = list(self.fields.keys())
        where_clause = "" if not where else f"WHERE {where}\n"
        order_by_clause = "" if not order_by else f"ORDER BY {order_by}\n"
        limit_clause = "" if not limit else f"LIMIT {limit}\n"
        offset_clause = "" if not offset else f"OFFSET {offset}\n"
        sql = f"""
SELECT {', '.join(fields)}
FROM {self.table_name}
{where_clause}{order_by_clause}{limit_clause}{offset_clause};"""
        return sql.strip()

    def _insert_sql(self, data_dict_list: list):
        """
        Generate insert many sql.

        :param data_dict_list: (list<dict>) data to insert, e.g. [{'name': 'John', 'age': 25}, {'name': 'Mary', 'age': 30}]
        :return: (str) insert sql, e.g. insert into employee (name, age) values (
        """

        def _fmt_value(value):
            if isinstance(value, str):
                return f'"{value}"'
            else:
                return str(value)

        values_list = [[_fmt_value(value) for value in data_dict.values()] for data_dict in data_dict_list]
        values_str = ', '.join([f'({", ".join(values)})' for values in values_list])
        insert_sql = f"""INSERT INTO {self.table_name} ({', '.join(data_dict_list[0].keys())}) VALUES {values_str};"""
        return insert_sql.strip()

    @property
    def _create_table_sql(self):
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.table_name} (
{self._fields_sql}{self._primary_key_sql}{self._unique_sql}
);"""
        return sql.strip()

    @property
    def _fields_sql(self):
        fields_str = ', '.join([f'{field} {(self.fields[field]).value}' for field in self.fields])
        return f"{fields_str}"

    @property
    def _primary_key_sql(self):
        return f',\nPRIMARY KEY ({self.primary_key})'

    @property
    def _unique_sql(self):
        if not self.unique and len(self.unique) > 0:
            return f',\nUNIQUE ({", ".join(self.unique)})'
        else:
            return ''

    def _validate(self):
        """
        Validate table metadata.

        :return:
        """
        # validate table_name
        if not self.table_name:
            raise ValueError('Table name is not defined')
        # validate fields
        if not self.fields:
            raise ValueError('Fields are not defined')
        if not isinstance(self.fields, dict) or len(self.fields) == 0:
            raise ValueError('Fields must be a non-empty dict')
        # validate unique
        if self.unique:
            if not isinstance(self.unique, list) and not isinstance(self.unique, tuple):
                raise ValueError('Unique must be a list or tuple')
            for field in self.unique:
                if field not in self.fields:
                    raise ValueError(f'Unique field {field} is not in fields')
        else:
            self.unique = []
        # validate primary key
        if self.primary_key:
            if self.primary_key not in self.fields:
                raise ValueError(f'Primary key {self.primary_key} is not in fields {self.fields}')
            if self.fields[self.primary_key].value != SQLiteDataType.INTEGER.value:
                raise ValueError(
                    f'Primary key {self.primary_key} must be SQLiteDataType.INTEGER, but {self.fields[self.primary_key]}')
        else:
            self.primary_key = f'_{self.table_name}_pt'
        assert self.primary_key is not None, "Primary key is not defined"
