# Cetino


Cetino is a lightweight data storage package enabling you store and load structured record data from database, csv files, etc...


## Install

1. Install the package

```python
pip install cetino
```

## Usage

1. Define the data structure

```python
from cetino.db.sqlite import SQLiteTableStorage, SQLiteDataType

class StudentStorage(SQLiteTableStorage):
    fields = {
        'id': SQLiteDataType.INTEGER,
        'name': SQLiteDataType.TEXT,
        'age': SQLiteDataType.INTEGER
    }
    primary_key = 'id'
    table_name = 'student'
```

2. Load and Store records as you like

```python
# test data
student_record = {
   'id': 1,
   'name': 'steve',
   'age': 3
}

student_records = [
    {'name': 'iris', 'age': 4},
    {'name': 'george', 'age': 5},
]

# write one record
stu_storage.insert(student_record)
# write multiple records
stu_storage.insert_many(student_records)

# query
stu_storage.query()

# query dump to csv
stu_storage.query_dump(limit=2, file_path="./students.csv")
```
