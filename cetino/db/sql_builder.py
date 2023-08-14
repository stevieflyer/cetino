from cetino.db.dialect import SQLDialect
from .sqlite.sql_builder import SQLiteSQLBuilder
from .mysql.sql_builder import MySQLSQLBuilder


_builder_dict = {
    SQLDialect.SQLITE: SQLiteSQLBuilder,
    SQLDialect.MYSQL: MySQLSQLBuilder
}


class SQLBuilder:
    supported_dialects = [SQLDialect.SQLITE, SQLDialect.MYSQL]

    def __init__(self, dialect: SQLDialect = SQLDialect.SQLITE):
        self.dialect = dialect
        self._validate()
        self._builder = self._get_builder()

    def _get_builder(self):
        if self.dialect in self.supported_dialects:
            return _builder_dict[self.dialect]()
        else:
            raise NotImplementedError(f"Dialect {self.dialect} is not supported yet.")

    def _validate(self):
        self._validate_dialect()

    def _validate_dialect(self):
        if self.dialect not in SQLDialect:
            raise ValueError(f"Dialect {self.dialect} is not supported.")


__all__ = ["SQLBuilder"]
