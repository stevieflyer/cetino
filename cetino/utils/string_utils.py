def add_quote(value):
    """
    Add quote to value if it is a string.

    e.g. "John" -> "\"John\""

    :param value: (str) value to be quoted
    :return: (str) quoted value
    """
    if isinstance(value, str):
        escaped_value = value.replace('"', '\\"')
        return f'"{escaped_value}"'
    else:
        return str(value)
