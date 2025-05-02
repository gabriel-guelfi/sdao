import pyodbc
import re

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: str="1433"):
        self.cnn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={host},{port};'
            f'DATABASE={database};'
            f'UID={user};'
            f'PWD={password}'
        )

        self.dialect = 'mssql'

    def __del__(self):
        self.cnn.close()

    def _fetchall_as_dicts(self, cursor):
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _fetchone_as_dict(self, cursor):
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        return dict(zip(columns, row)) if row else None

    def create(self, sql: str, data):
        cursor = self.cnn.cursor()

        if isinstance(data, list):
            cursor.executemany(sql, data)
        elif isinstance(data, dict):
            cursor.execute(sql, data)

        # pyodbc does not support lastrowid reliably across all backends
        # Use SCOPE_IDENTITY() explicitly for SQL Server if needed
        cursor.execute("SELECT SCOPE_IDENTITY()")
        last_id = cursor.fetchone()[0]

        self.cnn.commit()
        cursor.close()
        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        result = self._fetchone_as_dict(cursor) if onlyFirstRow else self._fetchall_as_dicts(cursor)
        cursor.close()
        return result

    def update(self, sql: str, params: dict):
        cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
            
        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows

    def delete(self, sql: str, params: dict = {}):
        cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows
