import uuid
import logging
import sqlite3
from sqlite3 import Error

from ._decorator import connect
from cetino.utils.io_utils import ensure_pathlib_path
from cetino.utils.log_utils import get_console_only_logger, get_logger


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

    @property
    def db_file(self):
        return self._db_file

    @connect()
    def execute(self, sql: str):
        """Execute SQL statement."""
        if self._conn is None:
            raise ConnectionError(
                f"Connection with {self._db_file} is not established. You should use 'with' statement or manually manage the connection.")

        c = self._conn
        try:
            result = c.execute(sql)
            self._log(f'Execute SQL: {sql}', level=logging.INFO)
            return result
        except Error as e:
            self._log(f'Execute SQL: {sql} failed', level=logging.ERROR)
            self._log(str(e), level=logging.ERROR)
            raise e  # 直接抛出原始的 error

    def _connect(self):
        if self.is_connect is False:
            self._log(f'Establishing connection with {self._db_file}...')
            self._conn = sqlite3.connect(self._db_file)
            self.is_connect = True
            self._log(f'Connection with {self._db_file} established', level=logging.INFO)
        else:
            self._log(f'Connection with {self._db_file} already established', level=logging.INFO)

    def _commit(self):
        self._conn.commit()
        self._log(f'Commit changes to {self._db_file}', level=logging.INFO)

    def _disconnect(self):
        if self.is_connect is True:
            self._log(f'Closing connection with {self._db_file}...')
            self._conn.close()
            self.is_connect = False
            self._log(f'Connection with {self._db_file} closed', level=logging.INFO)
        else:
            self._log(f'Connection with {self._db_file} already closed', level=logging.INFO)

    def _log(self, message: str, level=logging.INFO, **kwargs):
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

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._disconnect()


__all__ = ["SQLiteStorage"]
