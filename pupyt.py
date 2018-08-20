from functools import reduce
import csv
from copy import deepcopy
from collections import Iterable
from itertools import repeat


def vectorize(f):
    def wrapper(*args):
        arguments = [repeat(a, len(args[0])) if not isinstance(a, Iterable) or type(a) is str else a for a in args]
        return Column(f(*arguments))
    return wrapper


def atomic(x):
    x = x if type(x) is list else [x]
    return x


def lzip(*args):
    """
    works like zip, but will return list objects instead of zip objects and tuples
    :param args:
    :return:
    """
    return [list(x) for x in list(zip(*args))]


def flatten_pupyt_groups(obj):
    items = []
    for k, v in obj.items():
        if type(v) is dict or type(v) is Table:
            items.append(flatten_pupyt_groups(v))
        else:
            return obj
    vals = list(zip(*[list(i.values()) for i in items]))
    for i, val in enumerate(vals):
        vals[i] = [item for sublist in val for item in sublist]
    cols = list(zip(*[list(i.keys()) for i in items]))
    assert all([True if len(set(c)) == 1 else False for c in cols])
    cols = [c[0] for c in cols]
    table = Table(dict(zip(cols, vals)))
    table.verify_integrity()
    return table


class Table(dict):
    def __init__(self, dictionary):
        dict.__init__(self, {k: Column(v) for k, v in dictionary.items()})

    def __setitem__(self, key, value):
        assert type(key) is str
        super(Table, self).__setitem__(key, Column(value))

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start == key.stop:
                return Table({k: [v[key.start]] for k, v in self.items()})
            else:
                return Table({k: v[key] for k, v in self.items()})
        elif isinstance(key, int):
            return Table({k: [v[key]] for k, v in self.items()})
        elif isinstance(key, list):
            if all([type(k) is str for k in key]):
                return Table({k: v for k, v in self.items() if k in key})
            if all([type(k) is int for k in key]):
                return Table({k: [vals[i] for i in key] for k, vals in self.items()})
            if all([type(k) is bool for k in key]):
                return Table({k: [v for i, v in enumerate(vals) if key[i] is True] for k, vals in self.items()})
            else:
                raise ValueError
        else:
            return [v for k, v in self.items() if k == key][0]

    def __iter__(self):
        self.n = -1
        return self

    def __next__(self):
        self.n += 1
        if self.n < self.nrow():
            return self[self.n]
        else:
            raise StopIteration

    def add_column(self, name, values=None, override=False):
        names = [name] if type(name) is not list else name
        for n in names:
            if n in self.keys() and override is False:
                print("WARNING:\n\tThis column already exists. set override to True to replace")
                return
            if values:
                if type(values) == str:
                    self[n] = [values] * self.nrow()
                else:
                    assert len(values) == self.nrow()
                    self[n] = values
            else:
                self[n] = [None for _ in range(self.nrow())]

    def apply(self, funct, columns: list, apply_type='r'):
        """

        :type apply_type: str can be r 'r' or 'g' for 'row' or 'group'. by row will return a list,
            by group returns an atomic
        """
        #assert columns or raw_inputs
        inputs = [self[col] for col in columns]
        if apply_type == 'g':
            return funct(*inputs)
        if len(inputs) == 1:
            return list(map(funct, inputs))
        else:
            return funct(*inputs)

    def copy_from(self, dictionary):
        self.clear()
        for key in dictionary.keys():
            self[key] = dictionary[key]

    # TODO Optimize code
    def de_dup(self):
        seen = []
        delete = []
        for i, row in enumerate(self.iterrows()):
            if row in seen:
                delete.append(i)
            else:
                seen.append(row)
        for i, idel in enumerate(delete):
            for k in self.keys():
                del(self[k][idel-i])

    def drop_none(self):
        none_filter = self.filter(lambda x: None not in x, list(self.keys()))
        self.copy_from(self.filter_by(none_filter))

    def empty_keys(self):
        return Table({k: [] for k in self.keys()})

    def filter(self, funct, columns=None):
        inputs = [self[col] for col in columns]
        filt = list(map(funct, *inputs))
        return self[filt]

    #def filter(self, funct, columns=None):
    #    inputs = [self[col] for col in columns]
    #    filt = list(map(funct, *inputs))
    #    for k, vals in self.items():
    #        self[k] = [v for i, v in enumerate(vals) if filt[i] is True]

    def head(self, n):
        return Table({k: self[k][:n] for k in self.keys()})

    #def filter_by(self, filter_):
    #    assert len(filter_) == len(self[list(self.keys())[0]])
    #    assert {bool} == set([type(x) for x in filter_])
    #    return Table({col: [x for i, x in enumerate(self[col]) if filter_[i] is True] for col in self.keys()})


    def if_then(self, bool_list: list, dict_then):
        assert len(bool_list) == self.nrow()
        for i, _bool in enumerate(bool_list):
            if _bool:
                for k, v in dict_then.items():
                    self[k][i] = v

    def groupby(self, columns: list, keyfunc=lambda *x: x):
        self.sort(columns)
        it = iter(self)
        lb, ub = 0, 0
        currkey = keyfunc(*[v[0] for v in next(it)[columns].values()])
        for row in it:
            currkey, tgtkey = keyfunc(*[v[0] for v in row[columns].values()]), currkey
            if currkey != tgtkey:
                yield (lb, ub), self[lb:ub]
                lb = ub
            ub += 1
        yield (lb, ub), self[lb:]

    def add_row(self, data):
        if type(data) is list:
            for i, k in enumerate(self.keys()):
                self[k].append(data[i])
        if type(data) is dict:
            for k, v in data:
                self[k].extend(v)

    def group_by(self, columns: list):
        if not columns:
            return self
        groups = {x: {col: [] for col in self.keys()} for x in list(set(self[columns[0]]))}
        for i, x in enumerate(self[columns[0]]):
            for c, v in zip(groups[x], self.irow(i)):
                groups[x][c].append(v)
        for key, table in groups.items():
            groups[key] = Table(table).group_by(columns[1:])
        return groups

    def iterrows(self, as_table=False):
        for i in range(self.nrow()):
            yield self.irow(i, as_table=as_table)

    def irow(self, i, as_table=False):
        return [x[i] for x in self.values()] if as_table is False else \
            Table({k: [self[k][i]] for k in self.keys()})

    def mutate(self, column_name, funct, function_input):
        inputs = [self[_input_] for _input_ in function_input]
        self[column_name] = list(map(funct, *inputs))

    def merge(self, other_table, fix_column_names=False, prefix_new=''):
        self.verify_integrity()
        other_table.verify_integrity()
        assert self.nrow() == other_table.nrow()
        for col in other_table.keys():
            colname = prefix_new + deepcopy(col)
            while colname in self.keys() and fix_column_names is True:
                colname = 'x' + colname
            assert colname not in self.keys()
            self[colname] = other_table[col]

    def nrow(self):
        self.verify_integrity()
        return len(self[list(self.keys())[0]])

    def ProperName(self, columns):
        if type(columns) is list:
            for column in columns:
                self[column] = [x.strip()[0].upper() + x.strip()[1:].lower() if x else None for x in self[column]]
        else:
            self[columns] = [x.strip()[0].upper() + x.strip()[1:].lower() if x else None for x in self[column]]

    def rename(self, old_name, new_name):
        if old_name in self.keys():
            self[new_name] = self[old_name]
            del(self[old_name])
        else:
            raise KeyError

    def save(self, name, encoding='UTF-8'):
        self.verify_integrity()
        with open(name, 'w', newline='', encoding=encoding) as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(self.keys())
            for row in self.iterrows():
                writer.writerow(row)

    def select_max(self, column):
        return self.select(lambda x, y: x if x > y else y, column)

    def select_min(self, column):
        return self.select(lambda x, y: x if x < y else y, column)

    def select(self, columns: list):
        """
        :param columns: columns to select and keep
        :return: keeps the selectd columns
        """
        delcols = []
        for col in self.keys():
            if col not in columns:
                delcols.append(col)
        for col in delcols:
            del self[col]

    def subset(self, funct, columns):
        inputs = [self[col] for col in columns]
        filt = list(map(funct, *inputs))
        return Table({
            k: [v for i, v in enumerate(vals) if filt[i] is True] for k, vals in self.items()
        })

    def sort(self, columns):
        for column in reversed(columns):
            new_indices = sorted(range(self.nrow()), key = lambda k: (self[column][k] is None, self[column][k]))
            for k, vals in self.items():
                self[k] = [vals[i] for i in new_indices]

    def table_generator(self, grouped_table, group_item=None):
        if type(list(grouped_table.values())[0]) is dict:
            for k, v in grouped_table.items():
                yield from self.table_generator(v, k)
        else:
            yield(grouped_table, group_item)

    def summarise(self, column_name, funct, funct_inputs):
        group_iterator = self.group_iterator()
        keep_columns = [col for col in self.keys() if col in self.groups]
        self.clear()
        for col in keep_columns + [column_name]:
            self[col] = []
        for group in group_iterator:
            inputs = [group[input_] for input_ in funct_inputs]
            value = list(map(funct, inputs))
            self[column_name].extend(atomic(value))
            for k_col in keep_columns:
                k = group[k_col]
                assert len(set(k)) == 1
                self[k_col].extend(atomic(k[0]))

    def summarise_II(self, *args):
        """

        """

        new_columns, functions, inputs = lzip(*args)

        group_iterator = self.group_iterator()
        keep_columns = [col for col in self.keys() if col in self.groups]
        self.clear()
        for col in keep_columns + new_columns:
            self[col] = []
        for group in group_iterator:
            for i in range(len(args)):
                value = group.apply(functions[i], inputs[i], apply_type='g')
                self[new_columns[i]].extend(atomic(value))
            for k_col in keep_columns:
                k = group[k_col]
                assert len(set(k)) == 1
                self[k_col].extend(atomic(k[0]))

    def tail(self, n):
        return Table({k: self[k][n:] for k in self.keys()})

    def unite(self, columns: list, name: str, sep='', rem=True):
        self[name] = list(map(lambda a, b: str(a) + sep + str(b), *[self[c] for c in columns]))
        if rem is True:
            for col in columns:
                del self[col]

    def update_index(self, row_index, mapped_row: dict):
        assert mapped_row.keys() == self.keys()
        for col in self.keys():
            self[col][row_index] = mapped_row[col]

    def verify_integrity(self):
        l = len(self[list(self.keys())[0]])
        assert all(len(self[k]) == l for k in self.keys()) is True

    def where(self, column: str, funct):
        table = Table(dict(zip(self.keys(),
                               zip(*[self.irow(i) for i, val in enumerate(self[column]) if funct(val) is True]))))
        return table if table != {} else self.empty_keys()


class GroupHierarchy(Table):
    def __init__(self, table, groups):
        self.table = table
        self.groups = groups
        Table.__init__(self, self.set_structure(self.groups, Table(table)))
        pass

    def set_structure(self, groups, data: Table):

        if groups:
            name = groups[0]
            group_values = set(self.table[name])
            datas = [deepcopy(data) for _ in range(len(group_values))]
            structure = Group({val: self.set_structure(groups[1:], datas[i].filter(lambda x: x == val, [name]))
                               for i, val in enumerate(group_values)})
            return structure
        return Table(data)


class Column(list):

    @vectorize
    def __add__(self, other):
        return [s + o for s, o in zip(self, other)]

    @vectorize
    def __sub__(self, other):
        return [s - o for s, o in zip(self, other)]

    @vectorize
    def __mul__(self, other):
        return [s * o for s, o in zip(self, other)]

    @vectorize
    def __truediv__(self, other):
        return [s / o for s, o in zip(self, other)]

    @vectorize
    def __mod__(self, other):
        return [s % o for s, o in zip(self, other)]

    @vectorize
    def __gt__(self, other):
        return [s > o for s, o in zip(self, other)]

    @vectorize
    def __lt__(self, other):
        return [s < o for s, o in zip(self, other)]

    @vectorize
    def __ge__(self, other):
        return [s >= o for s, o in zip(self, other)]

    @vectorize
    def __le__(self, other):
        return [s <= o for s, o in zip(self, other)]

    @vectorize
    def __eq__(self, other):
        return [s == o for s, o in zip(self, other)]

    @vectorize
    def __and__(self, other):
        return [True if s and o else False for s, o in zip(self, other)]

    @vectorize
    def __contains__(self, item):
        print(item)
        return [True if s in item else False for s in self]

    def as_string(self, convert_none=None):
        return Column([str(s) if s else convert_none for s in self])

    def unique(self):
        return Column(list(set(self)))

    def cast(self, funct):
        return Column([x.funct() for x in self])


class Group(Table):
    def __init__(self, table, leaf=False):
        Table.__init__(self, table)
        self.leaf = leaf

    def agg(self, funct, columns, col_names=None):
        if col_names is not None:
            assert len(columns) == len(col_names)
        col_names = zip(columns, col_names) if col_names else zip(columns, columns)
        for col, name in col_names:
            self.aggregate[name] = list(map(lambda t: funct(t[col]), [t for t in self.table_generator(self)]))

    def table_generator(self, obj):
        for k, v in obj.items():
            if type(v) is dict:
                yield from self.table_generator(v)
            else:
                yield(v)
