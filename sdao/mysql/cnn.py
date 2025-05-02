import mysql.connector

class Cnn:
    def __init__(self, host: str, database: str, user: str, password: str, port: int=3306):
        self.cnn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

        self.dialect = 'mysql'

    def __del__(self):
        self.cnn.close()

    def create(self, sql: str, data):
        cursor = self.cnn.cursor(buffered=True, dictionary=True)

        if isinstance(data, list):
            cursor.executemany(sql, data)
        
        elif isinstance(data, dict):
            cursor.execute(sql, data)

        id = cursor.lastrowid
        self.cnn.commit()
        cursor.close()
        return id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        cursor = self.cnn.cursor(buffered=True, dictionary=True)
        cursor.execute(sql, params)
        if onlyFirstRow: 
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
        cursor.close()
        return result
    
    def update(self, sql: str, mysqlParams: dict):
        cursor = self.cnn.cursor(buffered=True, dictionary=True)
        cursor.execute(sql, mysqlParams)
        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows
    
    def delete(self, sql: str, params: dict = {}):
        cursor = self.cnn.cursor(buffered=True, dictionary=True)
        cursor.execute(sql, params)
        affectedRows = cursor.rowcount
        self.cnn.commit()
        cursor.close()
        return affectedRows
