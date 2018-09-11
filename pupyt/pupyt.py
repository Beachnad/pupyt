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

    def add_row(self, **kwargs):
        if not all(k in kwargs.keys() for k in self.keys()):
            raise AssertionError(
                "\nkeys needed: {}\nkeys provided: {}".format(sorted(self.keys()), sorted(kwargs.keys())))
        for k in self.keys():
            self[k].append(kwargs[k])

    def filter(self, columns: list, f):
        keep_indices = [f(*x) for x in zip(*[self[c] for c in columns])]
        return PuPyT({
            k: [v for k, v in zip(keep_indices, self[k]) if k is True] for k in self.keys()
        })

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

    def get_key(self, ind=0):
        return list(self.keys())[ind]

    def irow(self, i, as_table=False):
        return [x[i] for x in self.values()] if as_table is False else \
            PuPyT({k: [self[k][i]] for k in self.keys()})

    def iter_rows(self, as_list=False):
        for i in range(self.nrow):
            if as_list:
                yield [vals[i] for vals in self.values()]
            else:
                yield PuPyT({k: self[k][i] for k in self.keys()})

    def sort_on(self, column):
        new_indices = [i[1] for i in sorted([(v, i) for i, v in enumerate(self[column])])]
        for k, v in self.items():
            self[k] = [v[i] for i in new_indices]

    def merge(self, other: dict, method=None):
        assert set(self.keys()) == set(other.keys())
        if any([] == v for v in self.values()): return PuPyT(other)
        if any([] == v for v in other.values()): return PuPyT(self)
        for k in self.keys():
            if type(self[k]) != list:
                self[k] = [self[k]]
            method = method if method else 'extend' if type(other[k]) in (list, tuple) else 'append'
            if method == 'append':
                self[k].append(other[k])
            if method == 'extend':
                self[k].extend(other[k])
        return PuPyT(self)
    
    def xxx(self):
        return 'xxx'
