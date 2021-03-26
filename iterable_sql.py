from torch.utils.data import IterableDataset
import torch
import pymssql
from datetime import datetime as dt
import chardet


class MSSQLIterableDataset(IterableDataset):
    def __init__(self, server, database, table, iterable_column,
                 iterate_from=None, iterate_to=None,
                 username=None, password=None,
                 columns=None, exc_cols=None, dt_to_str=True, charset='UTF-8'):
        super(MSSQLIterableDataset).__init__()
        self.table = table
        self.iterable_column = iterable_column
        if type(iterate_from) == str:
            self.iterate_from = f"'{iterate_from}'"
        else:
            self.iterate_from = iterate_from
        if type(iterate_to) == str:
            self.iterate_to = f"'{iterate_to}'"
        else:
            self.iterate_to = iterate_to
        self.dt_to_str = dt_to_str
        self.cnxn = pymssql.connect(server=server,
                                    database=database,
                                    charset=charset,
                                    user=username,
                                    password=password)
        self.len, self.all_columns, self.column_types = self.get_len()
        self.columns = []
        if columns:
            for col in columns:
                if col in self.all_columns:
                    self.columns.append(col)
                else:
                    print(f'{col} is not in table {table}')
        if len(self.columns) == 0:
            self.columns = self.all_columns
        if exc_cols:
            self.columns = list(set(self.columns) - set(exc_cols))
        self.data = self.get_data()

    def __iter__(self):
        return self.data.__iter__()

    def __next__(self):
        return self.data.__next__()

    def __len__(self):
        return self.len

    def get_len(self):
        cur = self.cnxn.cursor()
        sql = f"select count(*) from {self.table}"
        if self.iterate_from is not None and self.iterate_to is not None:
            sql += f""" where {self.iterable_column}
            between {self.iterate_from} and {self.iterate_to}"""
        cur.execute(sql)
        row = cur.fetchone()
        sql = f"select top 1 * from {self.table}"
        cur.execute(sql)
        r = cur.fetchone()
        columns = [column[0] for column in cur.description]
        types = {column[0]: 'string' if column[1] == 1 else
                 'binary' if column[1] == 2 else
                 'number' if column[1] == 3 else
                 'datetime' if column[1] == 4 else
                 'rowid' if column[1] == 5 else
                 'string'
                 for column in cur.description}
        cur.close()
        if row:
            return row[0], columns, types
        else:
            return 0, columns, types

    def get_data(self):
        cur = self.cnxn.cursor(as_dict=True)
        cols = '[' + '],['.join(self.columns) + ']'
        sql = f"select {cols} from {self.table}"
        if self.iterate_from is not None and self.iterate_to is not None:
            sql += f""" where {self.iterable_column}
            between {self.iterate_from} and {self.iterate_to}"""
        cur.execute(sql)
        return cur

    def __del__(self):
        self.cnxn.close()

    def mssqlcollate(self, batch):
        res = []
        for a in batch:
            res_d = {}
            for x, y in a.items():
                if y is None and self.column_types[x] == 'string':
                    res_d[x] = ''
                elif y is None and self.column_types[x] in [
                        'rowid',
                        'datetime',
                        'binary']:
                    res_d[x] = 0
                elif (y is None
                      and self.column_types[x] == 'datetime'
                      and not self.dt_to_str):
                    res_d[x] = dt(1900, 1, 1)
                elif (y is None
                      and self.column_types[x] == 'datetime'
                      and self.dt_to_str):
                    res_d[x] = '1900-01-01'
                elif (y is not None
                      and self.column_types[x] == 'datetime'
                      and self.dt_to_str):
                    res_d[x] = str(y)
                elif (y is not None
                      and self.column_types[x] == 'string'):
                    try:
                        res_d[x] = y.encode(encoding='latin1',
                                            errors='strict').decode('1251')
                    except UnicodeEncodeError:
                        # print(chardet.detect(y.encode())['encoding'])
                        res_d[x] = y.encode('utf-8').decode(
                            chardet.detect(y.encode())['encoding'])
                else:
                    res_d[x] = y
            res.append(res_d)
        if len(batch) == 0:
            return torch.utils.data.dataloader.default_collate(list())
        return torch.utils.data.dataloader.default_collate(res)
