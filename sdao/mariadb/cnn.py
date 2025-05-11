import mariadb

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306, useSocket = False, autocommit = True):
        self.autocommit = autocommit

        if host == 'localhost' and not useSocket:
            host = '127.0.0.1'

        self.cnn = mariadb.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

        self.dialect = 'mariadb'
        
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

        last_id = self._cursor.lastrowid

        if self.autocommit:
            self.commit()

        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        self._cursor = self.cnn.cursor()
        self._cursor.execute(sql, params)
        result = self._fetchone_as_dict(self._cursor) if onlyFirstRow else self._fetchall_as_dicts(self._cursor)
        self._cursor.close()
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
        sql = f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'"
        tbinfo = self.read(sql,onlyFirstRow=True)
        return tbinfo['Column_name'] if tbinfo else None
