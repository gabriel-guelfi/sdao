class SqlBuilder:
    def __init__(self, table: str):
        self.table = table
        self.basicSelect = f'SELECT * FROM `{table}`'

    def insert(self, data):
        if isinstance(data, dict):
            keys = list(data.keys())
        elif isinstance(data, list):
            keys = list(data[0].keys())

        values = []
        for key in keys:
            values.append(f":{key}")

        sql = f"INSERT INTO `{self.table}` ({','.join(f'`{k}`' for k in keys)}) VALUES({','.join(values)})"
        return sql

    def update(self, data: dict):
        pairs = []
        for key in data:
            pairs.append(f"`{key}` = :{key}")

        return f"UPDATE `{self.table}` SET {','.join(pairs)}"

    def delete(self):
        return f"DELETE FROM `{self.table}`"

    def whereCondition(self, params: list):
        result = 'WHERE'
        usedParamNames = []

        for condition in params:
            paramName = f"`{condition['paramName']}`"
            paramAlias = f"param_{condition['paramName']}"

            logicalOperator = condition['logicalOperator']
            comparisonOperator = condition['comparisonOperator']

            if isinstance(condition['value'], list):
                comparisonOperator = "IN" if comparisonOperator != "NOT IN" else comparisonOperator

                next_index = 0
                while True:
                    if not f"{paramAlias}_{next_index}" in usedParamNames:
                        break
                    next_index += 1

                joinedValues = []
                for i in range(0, len(condition['value'])):
                    inParamName = f"{paramAlias}_{i + next_index}"
                    usedParamNames.append(inParamName)
                    joinedValues.append(f":{inParamName}")

                value = f"({','.join(joinedValues)})"

                if len(condition['value']) < 1: 
                    value = '1'
                    paramName = ''
                    paramAlias = ''
                    logicalOperator = ''
                    comparisonOperator = ''

            else: 
                value = f" :{paramAlias}" if condition['value'] is not None else ''

            if logicalOperator is not None:
                result = f"{result} {logicalOperator}"

            result = f"{result} {paramName} {comparisonOperator}{value}"

        return result
