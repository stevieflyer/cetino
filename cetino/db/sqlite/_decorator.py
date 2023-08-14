def connect(commit=False):
    """
    Decorator for managing connection with SQLite Database
    """

    def decorator(func):
        def wrapper(self, *args, **kwargs):
            # Before calling the decorated function, connect to database
            if self.is_connect is False:
                raise ConnectionError(
                    f"Connection with {self._db_file} is not established. You should use 'with' statement or manually manage the connection.")
            result = func(self, *args, **kwargs)
            # After calling the decorated function, commit and disconnect from database if commit is True
            if commit:
                self._commit()
            return result

        return wrapper

    return decorator
