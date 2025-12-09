# sdao/firebird/sqlbuilder.py

class SqlBuilder:
    def __init__(self, table: str):
        self.table = table
        # Firebird uses double quotes for identifiers
        self.basicSelect = f'SELECT * FROM "{table}"'

    def insert(self, data):
        if isinstance(data, dict):
            keys = list(data.keys())
        elif isinstance(data, list) and len(data) > 0:
            keys = list(data[0].keys())
        else:
            raise ValueError("Data for insert() must be dict or non-empty list[dict]")

        values = []
        for key in keys:
            values.append(f"%({key})s")

        columns = ",".join(f'"{k}"' for k in keys)
        sql = f'INSERT INTO "{self.table}" ({columns}) VALUES({",".join(values)})'
        return sql

    def update(self, data: dict):
        if not isinstance(data, dict) or len(data) == 0:
            raise ValueError("Data for update() must be non-empty dict")

        pairs = []
        for key in data:
            pairs.append(f'"{key}" = %({key})s')

        return f'UPDATE "{self.table}" SET {",".join(pairs)}'

    def delete(self):
        return f'DELETE FROM "{self.table}"'

    def whereCondition(self, params: list):
        """
        params: list of dicts in the format produced by GetDao.filters
        (paramName, logicalOperator, comparisonOperator, value)
        """
        result = "WHERE"
        usedParamNames = []

        for condition in params:
            paramName = f'"{condition["paramName"]}"'
            paramAlias = f"param_{condition['paramName']}"

            logicalOperator = condition["logicalOperator"]
            comparisonOperator = condition["comparisonOperator"]

            if isinstance(condition["value"], list):
                # For IN/NOT IN, we expand to multiple params
                comparisonOperator = (
                    "IN" if comparisonOperator != "NOT IN" else comparisonOperator
                )

                # avoid clashes when same paramAlias appears multiple times
                next_index = 0
                while f"{paramAlias}_{next_index}" in usedParamNames:
                    next_index += 1

                joinedValues = []
                for i in range(0, len(condition["value"])):
                    inParamName = f"{paramAlias}_{i + next_index}"
                    usedParamNames.append(inParamName)
                    joinedValues.append(f"%({inParamName})s")

                value = f"({','.join(joinedValues)})"

                # If list is empty, neutral condition (1) to avoid syntax errors
                if len(condition["value"]) < 1:
                    value = "1"
                    paramName = ""
                    paramAlias = ""
                    logicalOperator = ""
                    comparisonOperator = ""
            else:
                # Single value or None
                value = f" %({paramAlias})s" if condition["value"] is not None else ""

            # Prepend logical operator if needed
            if logicalOperator is not None:
                result = f"{result} {logicalOperator}"

            result = f"{result} {paramName} {comparisonOperator}{value}"

        return result
