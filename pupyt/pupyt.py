from collections import abc


class PuPyT(dict):

    @property
    def key_types(self):
        return set(type(k) for k in self.keys())

    @property
    def nrow(self):
        assert len(set([len(self[k]) for k in self.keys()])) == 1
        return len(self[self.get_key()])

    def __init__(self, dictionary):
        super().__init__(dictionary)

    def __getitem__(self, key):
        if type(key) is int and int not in self.key_types:
            return PuPyT({k: v[key] for k, v in self.items()})
        else:
            return super(PuPyT, self).__getitem__(key)

    def get_key(self, ind=0):
        return list(self.keys())[ind]

    def iter_rows(self, as_list=False):
        for i in range(self.nrow):
            if as_list:
                yield [vals[i] for vals in self.values()]
            else:
                yield PuPyT({k: self[k][i] for k in self.keys()})

    def iter_nested(self, nested):
        for key, value in nested.items():
            if isinstance(value, abc.Mapping):
                yield from self.iter_nested(value)
            else:
                yield key, value

    def filter(self, columns: list, f):
        keep_indices = [f(*x) for x in zip(*[self[c] for c in columns])]
        return PuPyT({
            k: [v for k, v in zip(keep_indices, self[k]) if k is True] for k in self.keys()
        })

    def add_row(self, **kwargs):
        if not all(k in kwargs.keys() for k in self.keys()):
            raise AssertionError(
                "\nkeys needed: {}\nkeys provided: {}".format(sorted(self.keys()), sorted(kwargs.keys())))
        for k in self.keys():
            self[k].append(kwargs[k])

    def sort_on(self, column):
        new_indices = [i[1] for i in sorted([(v, i) for i, v in enumerate(self[column])])]
        for k, v in self.items():
            self[k] = [v[i] for i in new_indices]

    def group_by(self, columns: list):
        if not columns:
            return self
        groups = {x: {col: [] for col in self.keys()} for x in list(set(self[columns[0]]))}
        for i, x in enumerate(self[columns[0]]):
            for c, v in zip(groups[x], self.irow(i)):
                groups[x][c].append(v)
        for key, table in groups.items():
            groups[key] = PuPyT(table).group_by(columns[1:])
        return groups

    def irow(self, i, as_table=False):
        return [x[i] for x in self.values()] if as_table is False else \
            PuPyT({k: [self[k][i]] for k in self.keys()})

    @staticmethod
    def merge_dicts(dict1, dict2, method=None):
        assert set(dict1.keys()) == set(dict2.keys())
        if any([] == v for v in dict1.values()): return PuPyT(dict2)
        if any([] == v for v in dict2.values()): return PuPyT(dict1)
        for k in dict1.keys():
            if type(dict1[k]) != list:
                dict1[k] = [dict1[k]]
            method = method if method else 'extend' if type(dict2[k]) in (list, tuple) else 'append'
            if method == 'append':
                dict1[k].append(dict2[k])
            if method == 'extend':
                dict1[k].extend(dict2[k])
        return PuPyT(dict1)
