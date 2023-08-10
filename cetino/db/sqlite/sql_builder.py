from ..dialect import SQLDialect


class BaseSQLBuilder:
    _clause_seperator = "\n"
    _field_declare_seperator = ",\n"

    @staticmethod
    def primary_key_sql(primary_key_tuple):
        return f'PRIMARY KEY ({", ".join(primary_key_tuple)})'

    @classmethod
    def fields_declare_sql(cls, fields_dict):
        if not isinstance(fields_dict, dict):
            raise TypeError(f"fields_dict must be a dict({{[FIELD_NAME]:[FIELD_TYPE]}}), got {type(fields_dict)}")
        fields_str = cls._field_declare_seperator.join([f'{field} {(fields_dict[field]).value}' for field in fields_dict])
        return fields_str

    @staticmethod
    def unique_sql(unique_tuple):
        if not unique_tuple or len(unique_tuple) == 0:
            return ''
        else:
            return f'UNIQUE ({", ".join(unique_tuple)})'

    @staticmethod
    def where_clause(cond_list: list):
        """
        Generate where clause.

        :param cond_list: (list) where clause, e.g. ['age > 20', 'name = "John"']
        :return: (str) where clause, e.g. 'WHERE age > 20 AND name = "John"'
        """
        if not cond_list or len(cond_list) == 0:
            return ''
        else:
            return f'WHERE {" AND ".join(cond_list)}'

    @staticmethod
    def order_by_clause(order_by_dict: dict):
        if not order_by_dict or len(order_by_dict) == 0:
            return ''
        else:
            return f'ORDER BY {", ".join([f"{field} {order_by_dict[field]}" for field in order_by_dict])}'

    @staticmethod
    def limit_clause(limit: int):
        if not limit:
            return ''
        else:
            return f'LIMIT {limit}'

    @staticmethod
    def offset_clause(offset: int):
        if not offset:
            return ''
        else:
            return f'OFFSET {offset}'


class SQLiteSQLBuilder(BaseSQLBuilder):
    def __init__(self):
        pass

    @classmethod
    def query_sql(cls, table_name: str, fields_list=None, cond_list=None, order_by_dict=None, limit=None, offset=None):
        fields_str = ', '.join(fields_list) if fields_list else "*"
        where_clause = cls.where_clause(cond_list)
        order_by_clause = cls.order_by_clause(order_by_dict)
        limit_clause = cls.limit_clause(limit)
        offset_clause = cls.offset_clause(offset)
        clauses = cls._clause_seperator.join([c for c in [where_clause, order_by_clause, limit_clause, offset_clause] if c != ""])
        sql = f"""
SELECT {fields_str}
FROM {table_name}
{clauses};"""
        return sql.strip()

    @classmethod
    def insert_sql(cls, table_name: str, data_dict_list: list):
        def _fmt_value(value):
            if isinstance(value, str):
                return f'"{value}"'
            else:
                return str(value)

        values_list = [[_fmt_value(value) for value in data_dict.values()] for data_dict in data_dict_list]
        values_str = ', '.join([f'({", ".join(values)})' for values in values_list])
        insert_sql = f"""INSERT INTO {table_name} ({', '.join(data_dict_list[0].keys())}) VALUES {values_str};"""
        return insert_sql.strip()

    @classmethod
    def create_table_sql(cls, table_name: str, fields_dict, primary_key_tuple=None, unique_tuple=None):
        fields_declare = cls.fields_declare_sql(fields_dict)
        primary_key_declare = cls.primary_key_sql(primary_key_tuple) if primary_key_tuple else ""
        unique_declare = cls.unique_sql(unique_tuple) if unique_tuple else ""

        clauses = [fields_declare, primary_key_declare, unique_declare]
        sql_clauses = cls._clause_seperator.join(filter(bool, clauses))

        sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{sql_clauses}
);"""
        return sql.strip()


class SQLBuilder:
    def __init__(self, dialogue: SQLDialect = SQLDialect.SQLITE):
        self.dialogue = dialogue
        self._validate()
        self._builder = self._get_builder()

    def _get_builder(self):
        if self.dialogue == SQLDialect.SQLITE:
            return SQLiteSQLBuilder
        else:
            raise NotImplementedError(f"Dialogue {self.dialogue} is not supported yet.")

    def _validate(self):
        self._validate_dialogue()

    def _validate_dialogue(self):
        if self.dialogue not in SQLDialect:
            raise ValueError(f"Dialogue {self.dialogue} is not supported.")
