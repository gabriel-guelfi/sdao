import mysql.connector

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: int=3306, autocommit = True):
        self.autocommit = autocommit
        
        self.cnn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

        self.dialect = 'mysql'

    def __del__(self):
        try:
            self._cursor.close()
            self.cnn.close()
        except Exception as exc:
            pass

    def create(self, sql: str, data):
        self._cursor = self.cnn.cursor(buffered=True, dictionary=True)

        if isinstance(data, list):
            self._cursor.executemany(sql, data)
        
        elif isinstance(data, dict):
            self._cursor.execute(sql, data)

        id = self._cursor.lastrowid
        
        if self.autocommit:
            self.commit()

        return id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        self._cursor = self.cnn.cursor(buffered=True, dictionary=True)
        self._cursor.execute(sql, params)
        if onlyFirstRow: 
            result = self._cursor.fetchone()
        else:
            result = self._cursor.fetchall()
        self._cursor.close()
        return result
    
    def update(self, sql: str, mysqlParams: dict):
        self._cursor = self.cnn.cursor(buffered=True, dictionary=True)
        self._cursor.execute(sql, mysqlParams)
        affectedRows = self._cursor.rowcount
        
        if self.autocommit:
            self.commit()

        return affectedRows
    
    def delete(self, sql: str, params: dict = {}):
        self._cursor = self.cnn.cursor(buffered=True, dictionary=True)
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
        tbinfo = self.read(sql, onlyFirstRow = True)
        return tbinfo['Column_name'] if tbinfo else None
