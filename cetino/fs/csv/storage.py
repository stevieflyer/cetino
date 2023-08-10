import csv
import pandas as pd

from cetino.utils.io_utils import ensure_pathlib_path


def fields_match(func):
    """
    A decorator to ensure that the fields of the CSV file match the expected fields.
    """

    def wrapper(self, file_path, *args, **kwargs):
        file_path = ensure_pathlib_path(file_path)
        if not file_path.exists():
            return func(self, file_path, *args, **kwargs)

        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            match = reader.fieldnames is None or reader.fieldnames == self.fields
            print("match: ", match)
            if not match:
                raise ValueError(
                    f"The fields in {file_path} do not match the expected fields. Expected: {self.fields}, Actual: {reader.fieldnames}")
        return func(self, file_path, *args, **kwargs)

    return wrapper


class CSVTableStorage:
    def __init__(self, fields, index_col=None):
        self.fields = fields
        self.index_col = index_col
        self._validate()

    @staticmethod
    def from_dict(record_dict: dict):
        """
        Create a CSVTableStorage from a dict

        :param record_dict: (dict) a dict with fields and index_col
        :return: (CSVTableStorage)
        """
        return CSVTableStorage(fields=list(record_dict.keys()))

    @staticmethod
    def from_dataframe(df: pd.DataFrame):
        """
        Create a CSVTableStorage from a dataframe

        :param df: (pd.DataFrame) a dataframe
        :return: (CSVTableStorage)
        """
        return CSVTableStorage(fields=df.columns.tolist(), index_col=df.index.name)

    @fields_match
    def read(self, file_path):
        """
        Read the csv file, return records

        :param file_path: (str) path to the csv file
        :return: (list<dict>)
        """
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            return [row for row in reader]

    @fields_match
    def read_to_df(self, file_path):
        """
        Read the csv file, return records

        :param file_path: (str) path to the csv file
        :return: (list<dict>)
        """
        record_df = pd.read_csv(file_path, index_col=self.index_col)
        self.fields = record_df.columns.tolist()
        return record_df

    @fields_match
    def write(self, file_path, records: list):
        """
        Write records to the csv file

        If the file does not exist, create it and write the records.
        Else, check if the fields are the same as the existing file, if not, raise ValueError.
        Else, append the records to the existing file.

        :param file_path: (str) path to the csv file
        :param records: (list<dict>)
        :return: None
        """
        file_path = ensure_pathlib_path(file_path)
        if not records or len(records) == 0:
            return
        if not file_path.exists():
            self._write_new_file(file_path, records)
        else:
            self._append_to_existing_file(file_path, records)

    @fields_match
    def write_from_df(self, file_path, record_df: pd.DataFrame):
        """
        Write records to the csv file

        If the file does not exist, create it and write the records.
        Else, check if the fields are the same as the existing file, if not, raise ValueError.
        Else, append the records to the existing file.

        :param file_path: (str) path to the csv file
        :param record_df: (pd.DataFrame) dataframe to write
        :return: None
        """
        # first convert record_df to list of dict
        records = record_df.to_dict(orient='records')
        self.write(file_path, records)

    @staticmethod
    def read_header(file_path):
        """
        Read the csv file, return header

        :param file_path: (str) path to the csv file
        :return: (list<dict>)
        """
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames

    def _write_new_file(self, file_path, records):
        file_path = ensure_pathlib_path(file_path)
        with open(file_path, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writeheader()
            writer.writerows(records)

    def _append_to_existing_file(self, file_path, records):
        file_path = ensure_pathlib_path(file_path)
        with open(file_path, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writerows(records)

    def _validate(self):
        if self.fields is None or len(self.fields) == 0:
            raise ValueError("fields cannot be empty")

    def __str__(self):
        return f"CSVTableStorage(fields={self.fields}, index_col={self.index_col})"

    def __repr__(self):
        return self.__str__()
