import mariadb

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306, useSocket = False):
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

        last_id = cursor.lastrowid
        self.cnn.commit()
        cursor.close()
        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        cursor = self.cnn.cursor()
        cursor.execute(sql, params)
        result = self._fetchone_as_dict(cursor) if onlyFirstRow else self._fetchall_as_dicts(cursor)
        cursor.close()
        return result

    def update(self, sql: str, params: dict):
        cursor = self.cnn.cursor()
        cursor.execute(sql, params)
        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows

    def delete(self, sql: str, params: dict = {}):
        cursor = self.cnn.cursor()
        cursor.execute(sql, params)
        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows
