from .._base import BaseSQLBuilder
from cetino.utils.string_utils import add_quote


class MySQLSQLBuilder(BaseSQLBuilder):
    @classmethod
    def query_sql(cls, table_name: str, fields_list=None, cond_list=None, order_by_dict=None, limit=None, offset=None):
        fields_str = ', '.join(fields_list) if fields_list else "*"
        where_clause = cls.where_clause(cond_list)
        order_by_clause = cls.order_by_clause(order_by_dict)
        limit_clause = cls.limit_clause(limit)
        offset_clause = cls.offset_clause(offset)
        clauses = cls._select_clause_sep.join(
            [c for c in [where_clause, order_by_clause, limit_clause, offset_clause] if c != ""])
        sql = f"""
SELECT {fields_str}
FROM {table_name}
{clauses};"""
        return sql.strip()

    @classmethod
    def insert_sql(cls, table_name: str, data_dict_list: list):
        if not data_dict_list or len(data_dict_list) == 0:
            raise ValueError(f"data_dict_list must be a list of dict, got {data_dict_list}")
        values_list = [[add_quote(value) for value in data_dict.values()] for data_dict in data_dict_list]
        values_str = ', '.join([f'({", ".join(values)})' for values in values_list])
        insert_sql = f"""INSERT INTO {table_name} ({', '.join(data_dict_list[0].keys())}) VALUES {values_str};"""
        return insert_sql.strip()

    @classmethod
    def create_table_sql(cls, table_name: str, fields_dict, primary_key_tuple=None, unique_tuple=None):
        fields_declare = cls.fields_declare_sql(fields_dict)
        primary_key_declare = cls.primary_key_sql(primary_key_tuple) if primary_key_tuple else ""
        unique_declare = cls.unique_sql(unique_tuple) if unique_tuple else ""

        clauses = [fields_declare, primary_key_declare, unique_declare]
        sql_clauses = cls._declare_clause_sep.join(filter(bool, clauses))

        sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{sql_clauses}
) ENGINE=InnoDB;"""  # Using InnoDB as default storage engine for MySQL
        return sql.strip()


__all__ = ["MySQLSQLBuilder"]
