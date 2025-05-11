import pyodbc
import re

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: str="1433", autocommit = True):
        self.autocommit = autocommit
        
        self.cnn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={host},{port};'
            f'DATABASE={database};'
            f'UID={user};'
            f'PWD={password}'
        )

        self.dialect = 'mssql'

    def __del__(self):
        try:
            self._cursor.close()
            self.cnn.close()
        except Exception as exc:
            pass

    def _fetchall_as_dicts(self, cursor):
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _fetchone_as_dict(self, cursor):
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        return dict(zip(columns, row)) if row else None

    def create(self, sql: str, data):
        self._cursor = self.cnn.cursor()

        if isinstance(data, list):
            self._cursor.executemany(sql, data)
        elif isinstance(data, dict):
            self._cursor.execute(sql, data)

        # pyodbc does not support lastrowid reliably across all backends
        # Use SCOPE_IDENTITY() explicitly for SQL Server if needed
        self._cursor.execute("SELECT SCOPE_IDENTITY()")
        last_id = self._cursor.fetchone()[0]

        if self.autocommit:
            self.commit()

        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        self._cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

        result = self._fetchone_as_dict(self._cursor) if onlyFirstRow else self._fetchall_as_dicts(self._cursor)
        self._cursor.close()
        return result

    def update(self, sql: str, params: dict):
        self._cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
            
        affectedRows = self._cursor.rowcount
        
        if self.autocommit:
            self.commit()

        return affectedRows

    def delete(self, sql: str, params: dict = {}):
        self._cursor = self.cnn.cursor()

        # detecta se a query tem marcadores no estilo %(param)s
        has_markers = bool(re.search(r"%\([^)]+\)s", sql))
        has_params = bool(params)

        if has_markers and has_params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

        affectedRows = self._cursor.rowcount
        
        if self.autocommit:
            self.commit()

        return affectedRows
    
    def commit(self):
        self.cnn.commit()
        if bool(self._cursor):
            self._cursor.close()

    def rollback(self):
        self.cnn.rollback()
        if bool(self._cursor):
            self._cursor.close()

    def getPrimaryKey(self, table):
        sql = f"""
        SELECT COLUMN_NAME AS Column_name
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        AND TABLE_NAME = '{table}'
    """
        tbinfo = self.read(sql, onlyFirstRow = True)
        return tbinfo['Column_name'] if tbinfo else None