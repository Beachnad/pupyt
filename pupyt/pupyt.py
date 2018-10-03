from itertools import groupby
from datetime import date, datetime, timedelta


class PuPyT(list):
    @property
    def nrow(self):
        return len(self)

    def __init__(self, data):
        if type(data) is dict:
            list.__init__(self, PuPyT.from_dict(data))
        elif type(data) in (list, PuPyT):
            list.__init__(self, data)
        else:
            raise ValueError

    def __getitem__(self, key):
        if type(key) in (slice, int):
            return super(PuPyT, self).__getitem__(key)
        else:
            return [r[key] for r in self]

    def __setitem__(self, key, value):
        if type(key) is str:
            assert len(value) == len(self)
            for i, v in enumerate(value):
                self[i][key] = v
        else:
            super(PuPyT, self).__setitem__(key, value)

    def __delitem__(self, key):
        if type(key) is str:
            for r in self:
                del(r[key])
        else:
            super(PuPyT, self).__delitem__(key)

    def filter_at(self, vars, funs):
        filters = []
        for key in self.select_at(vars):
            filters.append([funs(x) for x in self[key]])
        filters = [all(f[i] for f in filters) for i in range(self.nrow)]
        return self.filter_against(filters)

    def filter_against(self, bool_list):
        """
        :param list: list of boolean values the same length of the table
        :return: a table that keeps only the rows which are indexed TRUE in the input
        """
        return PuPyT([r for r in self._filter_against(bool_list)])

    def _filter_against(self, bool_list):
        for i, tf in enumerate(bool_list):
            if tf:
                yield self[i]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def group_by(self, targets):
        targets = targets if type(targets) is list else list(targets)
        return self._group_by(targets, 0)

    def _group_by(self, targets, i):
        if len(targets) - 1 > i:
            return PuPyG({k: PuPyT(list(v))._group_by(targets, i + 1)
                          for k, v in groupby(self.sort_on(targets[i]),key=lambda x: x[targets[i]])},
                         layer_name=targets[i])
        else:
            return PuPyG({k: PuPyT(list(v)) for k, v in groupby(self.sort_on(targets[i]),key=lambda x: x[targets[i]])},
                         layer_name=targets[i])

    def keys(self):
        return tuple(self[0].keys())

    def values(self):
        return [list(vals) for vals in (zip(*[r.values() for r in self]))]

    def items(self):
        return [(k, v) for k, v in zip(self.keys(), self.values())]

    def replace_nones(self, target, value=None):
        """
        replaces None values in a target column with specified value

        :param target: column to scan and replace
        :param value: value to substitute for the None values
        :return: list with replaces none values
        """
        return [v if v else value for v in self[target]]

    def replace_nones_default(self, target):
        target_type = next((type(x) for x in self[target] if x), None)

        if target_type is str:
            return self.replace_nones(target, '###')
        if target_type is int:
            return self.replace_nones(target, min((x for x in self[target] if x)) - 1)
        if target_type in (date, datetime):
            return self.replace_nones(target, min(x for x in self[target] if x) - timedelta(days=1))
        if not target_type:
            return self.replace_nones(target ,0)
        raise KeyError('{} not coded for default yet'.format(target_type))

    def select_at(self, vars):
        for key in self.keys():
            if vars(key):
                yield key

    def sort_on(self, target):
        # if None in self[target]:
        #     sorted_index = sorted(range(len(self[target])), key=lambda i:  [self[target][i] is None, self[target][i]])
        # else:
        temp_sort_list = self.replace_nones_default(target)
        sorted_index = sorted(range(len(temp_sort_list)), key=temp_sort_list.__getitem__)
        del(temp_sort_list)
        return PuPyT([self[ind] for ind in sorted_index])

    ###############################################
    @staticmethod
    def from_dict(dict):
        n_row = len(dict[list(dict.keys())[0]])
        assert all(len(v) == n_row for v in dict.values())
        return PuPyT([{k: v[i] for k, v in dict.items()} for i in range(n_row)])

    def as_dict(self):
        return dict(self.items())

    def union(self, other):
        other = other if type(other) is PuPyT else PuPyT(other)
        assert set(self.keys()) == set(other.keys())
        self.extend(other)
        return self

    ##### DEV AREA ########
    def mutate_at(self, vars, funs):
        """
        Mimics dplyr's mutate_at() and works similarly

        :param vars: function which after accepting the column name returns True or False
        :param funs: function which is applied to the column of interest
        :return: self, after mutation
        """
        for key in self.keys():
            if vars(key):
                self[key] = [funs(x) for x in self[key]]
        return self


class PuPyG(dict):
    @property
    def child_type(self):
        return type(self[list(self.keys())[0]])

    def __init__(self, dictionary, layer_name):
        dict.__init__(self, dictionary)
        self.layer_name = layer_name

    def iter_leafs(self):
        for key, val in self.items():
            if type(val) is PuPyG:
                yield from val.iter_leafs()
            else:
                yield val

    def iter_term_grps(self):
        for key, val in self.items():
            if type(self.child_type) is PuPyG:
                yield from val.iter_term_grps()
            else:
                yield self

    def peal(self, fun):
        for key, val in self.items():
            if type(val) is PuPyT:
                return fun(self)
            children_types = (self.child_type, val.child_type)

            if all(type(c) is PuPyG for c in children_types):
                val.peal(fun)
            elif children_types[1] is PuPyT:
                self[key] = fun(val)
        return self

    def summarise(self, **funs):
        return self.peal(lambda x: x._summarise(**funs))

    def _summarise(self, **funs):
        output = PuPyT([
            {name: fun(table) for name, fun in funs.items()} for key, table in self.items()
        ])
        output[self.layer_name] = [k for k in self.keys()]

        return output

    def summarise_all(self, **funs):
        cols = next(self.iter_leafs()).keys()
        print(cols)
        new_funs = {'{}_{}'.format(fkey, c): lambda x: print(c) for fkey in funs.keys() for c in cols}
        return self.summarise(**new_funs)

    def _summarise_all(self, **funs):
        new_funs = {}

        fgen = lambda c, f: lambda x: f(x[c])

        cols = next(self.iter_leafs()).keys()
        for prefix, f in funs.items():
            for c in cols:
                fun_name = '{}_{}'.format(prefix, c)
                fun = fgen(c, f)
                new_funs[fun_name] = fun

        return self.summarise(**new_funs)







