import hashlib
from importlib import import_module
from typing import Callable

class GetDao:
    def __init__(self, tablename:str, cnn):
        self.table = tablename
        self.filters = []
        self.persistence = {}
        
        if not hasattr(cnn, 'dialect'):
            raise Exception("The Cnn instance must expose a 'dialect' attribute")
        
        self.cnn = cnn
        self.dialect = cnn.dialect

        builder_module = import_module(f"sdao.{self.dialect}.sqlbuilder")
        self.sqlbuilder = builder_module.SqlBuilder(tablename)
    
    def find(self, sql: str = '', debug = False, onlyFirstRow = False):
        userDefinedParams = True
        if sql == '':
            sql = f"{self.sqlbuilder.basicSelect}{(' '+self.sqlbuilder.whereCondition(self.filters)) if len(self.filters) > 0 else ''}"
            userDefinedParams = False

        if debug: return {'SQL':sql, 'Params': self.prepareParams(userDefinedParams)}

        queryHash = self.findQueryHash(sql)
        if not queryHash in self.persistence:
            self.persistence[queryHash] = self.cnn.read(sql, self.prepareParams(userDefinedParams), onlyFirstRow)

        result = self.persistence[queryHash]
        return result
    
    def first(self, sql: str = '', debug = False):
        return self.find(sql, debug, True)
    
    def fetch(self, callback: Callable, sql: str = '', debug = False):
        result = self.find(sql, debug)

        if not debug:
            for row in result:
                callback(row)
        
        return result

    def insert(self, data, debug = False):
        sql = self.sqlbuilder.insert(data)

        if debug: return {'SQL':sql, 'Data': data}

        lastId = self.cnn.create(sql, data)

        tbKeyName = self.cnn.getPrimaryKey(self.table)
        if isinstance(data, dict):
            data[tbKeyName] = lastId
        elif isinstance(data, list):
            for obj in data:
                obj[tbKeyName] = lastId
                lastId = lastId + 1

        return data
    
    def update(self, data: dict, debug = False):
        sql = f"{self.sqlbuilder.update(data)}{(' '+self.sqlbuilder.whereCondition(self.filters)) if len(self.filters) > 0 else ''}"

        sqlParams = data
        sqlParams.update(self.prepareParams())
        if debug: 
            return {'SQL':sql, 'Params': sqlParams}

        affectedRows = self.cnn.update(sql, sqlParams)
        return affectedRows

    def delete(self, debug = False):
        sql = f"{self.sqlbuilder.delete()}{(' '+self.sqlbuilder.whereCondition(self.filters)) if len(self.filters) > 0 else ''}"

        if debug: return {'SQL':sql, 'Params': self.prepareParams()}

        affectedRows = self.cnn.delete(sql, self.prepareParams())
        return affectedRows

    def filter(self, paramName: str):
        self.filters.append({
            'paramName': paramName,
            'logicalOperator': None,
            'comparisonOperator': None,
            'value': None
        })

        return self
    
    def _and(self, paramName: str):
        if len(self.filters) == 0:
            raise Exception(
                "You can only call this method after calling 'filter()' first.")
        
        self.filters.append({
            'paramName': paramName,
            'logicalOperator': 'AND',
            'comparisonOperator': None,
            'value': None
        })

        return self
    
    def _or(self, paramName: str):
        if len(self.filters) == 0:
            raise Exception(
                "You can only call this method after calling 'filter()' first.")
        
        self.filters.append({
            'paramName': paramName,
            'logicalOperator': 'OR',
            'comparisonOperator': None,
            'value': None
        })

        return self

    def equalsTo(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = 'IS NULL' if value == None else '='

        return self

    def notEqualsTo(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = 'IS NOT NULL' if value == None else '!='

        return self

    def biggerThan(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = '>'

        return self

    def lessThan(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = '<'

        return self

    def biggerOrEqualsTo(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = '>='

        return self

    def lessOrEqualsTo(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = '<='

        return self

    def like(self, value):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = f"%{value}%"
        self.filters[i]['comparisonOperator'] = 'LIKE'

        return self

    def _in(self, value: list):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = 'IN'

        return self

    def _notIn(self, value: list):
        i = len(self.filters)
        if i == 0 or self.filters[i - 1]['value'] != None:
            raise Exception("This method can only be called right after one of the filtering methods.")
        
        i = i - 1

        self.filters[i]['value'] = value
        self.filters[i]['comparisonOperator'] = 'NOT IN'

        return self
    
    def prepareParams(self, userDefinedParams: bool = False):
        params = {}
        for condition in self.filters:
            if isinstance(condition['value'], list):
                # Find next:
                next = 0
                while True:
                    if not f"{'param_' if not userDefinedParams else '' }{condition['paramName']}_{next}" in params:
                        break
                    
                    next = next + 1

                for i in range(0, len(condition['value'])):
                    val = condition['value'][i]
                    params[f"{'param_' if not userDefinedParams else '' }{condition['paramName']}_{i + next}"] = val

            else: params[f"{'param_' if not userDefinedParams else '' }{condition['paramName']}"] = condition['value']

        return params
    
    def findQueryHash(self, sql: str):
        result = sql.replace('param_', '')
        params = self.prepareParams()

        for key in params:
            val = params[key]
            key = key.replace('param_', '')
            if isinstance(val, list):
                for i in range(0, len(val)):
                    result = result.replace(f"%({key}_{i})s", str(val[i]))
            else:
                result = result.replace(f"%({key})s", str(val))

        result = hashlib.md5(result.encode())
        return result.hexdigest()