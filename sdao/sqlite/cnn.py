import sqlite3

class Cnn:
    def __init__(self, database: str, datapath: str = './', autocommit = True):
        self.autocommit = autocommit

        self.cnn = sqlite3.connect(f'{datapath}{database}.db')
        self.cnn.row_factory = sqlite3.Row

        self.dialect = 'sqlite'
        
    def __del__(self):
        try:
            self._cursor.close()
            self.cnn.close()
        except Exception as exc:
            pass

    def create(self, sql: str, data):
        self._cursor = self.cnn.cursor()
        if isinstance(data, list):
            self._cursor.executemany(sql, data)
        elif isinstance(data, dict):
            self._cursor.execute(sql, data)

        last_id = self._cursor.lastrowid

        if self.autocommit:
            self.commit()

        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow = False):
        self._cursor = self.cnn.cursor()
        self._cursor.execute(sql, params)
        result = []
        for item in self._cursor.fetchall():
            result.append(dict(item))

        self._cursor.close()
        if onlyFirstRow:
            return result[0] if len(result) > 0 else None
        else:
            return result

    def update(self, sql: str, params: dict):
        self._cursor = self.cnn.cursor()
        self._cursor.execute(sql, params)
        affectedRows = self._cursor.rowcount

        if self.autocommit:
            self.commit()

        return affectedRows

    def delete(self, sql: str, params: dict = {}):
        self._cursor = self.cnn.cursor()
        self._cursor.execute(sql, params)
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
        cursor = self.cnn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        cols = cursor.fetchall()
        primary_keys = [col['name'] for col in cols if col['pk'] > 0]
        cursor.close()
        if len(primary_keys) == 0: 
            return None
        elif len(primary_keys) == 1: 
            return primary_keys[0]
        else: 
            return primary_keys