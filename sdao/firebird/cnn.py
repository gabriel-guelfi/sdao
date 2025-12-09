# sdao/firebird/cnn.py

import fdb
import re

class Cnn:
    def __init__(
        self,
        database: str,
        host: str = "localhost",
        user: str = "sysdba",
        password: str = "masterkey",
        port: int = 3050,
        charset: str = "UTF8",
        autocommit: bool = True,
    ):
        """
        database: either full path to .fdb file (for local/embedded)
                  or database alias configured on the Firebird server.
        """
        self.autocommit = autocommit

        # Build DSN (classic Firebird style: host/port:db)
        if host is None:
            # embedded or local-only style
            dsn = database
        else:
            dsn = f"{host}/{port}:{database}"

        self.cnn = fdb.connect(
            dsn=dsn,
            user=user,
            password=password,
            charset=charset,
        )

        self.dialect = "firebird"
        self._cursor = None

    def __del__(self):
        try:
            if getattr(self, "_cursor", None):
                try:
                    self._cursor.close()
                except Exception:
                    pass
            if getattr(self, "cnn", None):
                self.cnn.close()
        except Exception:
            # Swallow all cleanup errors at GC time
            pass

    # ---------- Internal helpers ----------

    _pyformat_pattern = re.compile(r"%\(([^)]+)\)s")

    def _convert_pyformat_to_qmark(self, sql: str, params: dict):
        """
        FDB expects positional parameters (qmark style, '?').
        We allow sdao to keep using '%(name)s' style in SQLBuilder
        and here we rewrite the SQL to use '?' and build the positional tuple
        in the correct order.

        If SQL has no '%(name)s' markers, we just return (sql, None) and
        let the caller decide how to pass params.
        """
        if not params:
            return sql, None

        if "%(" not in sql:
            # Caller probably used Firebird-native parameter style or no params
            # at all. We'll just return as-is and pass params untouched.
            return sql, params

        new_sql_parts = []
        values = []
        last_pos = 0

        for match in self._pyformat_pattern.finditer(sql):
            start, end = match.span()
            name = match.group(1)

            # Append text before this placeholder
            new_sql_parts.append(sql[last_pos:start])
            # Replace '%(name)s' with '?'
            new_sql_parts.append("?")
            last_pos = end

            # Pick value by name from dict
            if name not in params:
                raise KeyError(f"Parameter '{name}' not found for SQL: {sql}")
            values.append(params[name])

        # Append the rest of the SQL
        new_sql_parts.append(sql[last_pos:])
        new_sql = "".join(new_sql_parts)

        # Return converted SQL and positional params list
        return new_sql, values

    def _execute_dml(self, sql: str, params):
        """
        Helper for UPDATE and DELETE.
        """
        self._cursor = self.cnn.cursor()
        sql_to_exec, seq = self._convert_pyformat_to_qmark(sql, params)

        if seq is None:
            self._cursor.execute(sql_to_exec)
        else:
            self._cursor.execute(sql_to_exec, seq)

        affected_rows = self._cursor.rowcount

        if self.autocommit:
            self.commit()

        return affected_rows

    # ---------- Public API used by GetDao ----------

    def create(self, sql: str, data):
        """
        INSERT execution. Supports dict or list[dict], like the other dialects.
        Returns 'lastId' when possible, or None otherwise.
        """
        self._cursor = self.cnn.cursor()

        # We need to support both single-row and multi-row inserts
        # where SQL uses pyformat-style '%(name)s'.
        if isinstance(data, list):
            # Build positional order once based on the SQL
            names_in_order = [
                m.group(1) for m in self._pyformat_pattern.finditer(sql)
            ]
            if not names_in_order:
                # No '%(name)s' markers, just execute as-is
                self._cursor.executemany(sql, data)
            else:
                qmark_sql = self._pyformat_pattern.sub("?", sql)
                seq_params = []
                for row in data:
                    seq_params.append(tuple(row[name] for name in names_in_order))
                self._cursor.executemany(qmark_sql, seq_params)
        elif isinstance(data, dict):
            sql_to_exec, seq = self._convert_pyformat_to_qmark(sql, data)
            if seq is None:
                self._cursor.execute(sql_to_exec)
            else:
                self._cursor.execute(sql_to_exec, seq)
        else:
            raise TypeError("Data for 'create' must be dict or list[dict]")

        # Firebird does not have a universal "last inserted ID" like MySQL.
        # You usually use GENERATORs + triggers, or IDENTITY columns + RETURNING.
        # FDB's cursor may have 'lastrowid' only in specific setups.
        last_id = None
        if hasattr(self._cursor, "lastrowid"):
            last_id = self._cursor.lastrowid

        if self.autocommit:
            self.commit()

        return last_id

    def read(self, sql: str, params: dict = {}, onlyFirstRow: bool = False):
        """
        SELECT execution. Returns list[dict], or a single dict/None
        when onlyFirstRow=True, consistent with your SQLite implementation.
        """
        self._cursor = self.cnn.cursor()
        sql_to_exec, seq = self._convert_pyformat_to_qmark(sql, params)

        if seq is None:
            self._cursor.execute(sql_to_exec)
        else:
            self._cursor.execute(sql_to_exec, seq)

        rows = self._cursor.fetchall()

        # Map to dict using cursor.description
        col_names = [col[0].strip() for col in self._cursor.description]
        result = [dict(zip(col_names, row)) for row in rows]

        self._cursor.close()
        self._cursor = None

        if onlyFirstRow:
            return result[0] if len(result) > 0 else None
        else:
            return result

    def update(self, sql: str, params: dict):
        return self._execute_dml(sql, params)

    def delete(self, sql: str, params: dict = {}):
        return self._execute_dml(sql, params)

    def commit(self):
        self.cnn.commit()
        if getattr(self, "_cursor", None):
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None

    def rollback(self):
        self.cnn.rollback()
        if getattr(self, "_cursor", None):
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None

    def getPrimaryKey(self, table: str):
        """
        Returns primary key column name, list of column names (for composite PK),
        or None if no PK exists.

        Based on Firebird system tables RDB$RELATION_CONSTRAINTS and
        RDB$INDEX_SEGMENTS. :contentReference[oaicite:1]{index=1}
        """
        cur = self.cnn.cursor()
        sql = """
            SELECT
                TRIM(seg.RDB$FIELD_NAME)
            FROM
                RDB$RELATION_CONSTRAINTS rc
                JOIN RDB$INDEX_SEGMENTS seg
                    ON seg.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
            WHERE
                rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND rc.RDB$RELATION_NAME = UPPER(?)
            ORDER BY
                seg.RDB$FIELD_POSITION
        """
        cur.execute(sql, (table,))
        cols = [row[0] for row in cur.fetchall()]
        cur.close()

        if len(cols) == 0:
            return None
        elif len(cols) == 1:
            return cols[0]
        else:
            return cols
