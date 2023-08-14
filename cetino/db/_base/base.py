class BaseSQLBuilder:
    _select_clause_sep = "\n"
    _declare_clause_sep = ",\n"
    _field_declare_sep = ",\n"



    @staticmethod
    def primary_key_sql(primary_key_tuple):
        return f'PRIMARY KEY ({", ".join(primary_key_tuple)})'

    @classmethod
    def fields_declare_sql(cls, fields_dict):
        if not isinstance(fields_dict, dict):
            raise TypeError(f"fields_dict must be a dict({{[FIELD_NAME]:[FIELD_TYPE]}}), got {type(fields_dict)}")
        fields_str = cls._field_declare_sep.join([f'{field} {(fields_dict[field]).value}' for field in fields_dict])
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
        return f'WHERE {" AND ".join(cond_list)}' if cond_list else ''

    @staticmethod
    def order_by_clause(order_by_dict: dict):
        """
        Generate order by clause.

        :param order_by_dict: (dict) order by clause, e.g. {'name': 'ASC', 'age': 'DESC'}
        :return: (str) order by clause, e.g. 'ORDER BY name ASC, age DESC'
        """
        return f'ORDER BY {", ".join([f"{field} {order_by_dict[field]}" for field in order_by_dict])}' if order_by_dict else ''

    @staticmethod
    def limit_clause(limit: int):
        return f'LIMIT {limit}' if limit else ''

    @staticmethod
    def offset_clause(offset: int):
        return f'OFFSET {offset}' if offset else ''


__all__ = ['BaseSQLBuilder']
