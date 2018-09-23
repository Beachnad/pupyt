from itertools import groupby
from collections import OrderedDict


class PuPyT(list):

    def __init__(self, data):
        if type(data) is dict:
            list.__init__(self, PuPyT.from_dict(data))
        elif type(data) is list:
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

    def group_by(self, targets):
        targets = targets if type(targets) is list else list(targets)
        return self._group_by(targets, 0)

    def _group_by(self, targets, i):
        if len(targets) - 1 > i:
            return PuPyG({k: PuPyT(list(v))._group_by(targets, i + 1) for k, v in groupby(self.sort_on(targets[i]),
                                                                                          key=lambda x: x[targets[i]])})
        else:
            return self._group_by(targets[i], i)

    def sort_on(self, target):
        sorted_index = sorted(range(len(self[target])), key=self[target].__getitem__)
        return PuPyT([self[ind] for ind in sorted_index])


    ###############################################
    @staticmethod
    def from_dict(dict):
        n_row = len(dict[list(dict.keys())[0]])
        assert all(len(v) == n_row for v in dict.values())
        return PuPyT([{k: v[i] for k, v in dict.items()} for i in range(n_row)])


class PuPyG(dict):
    def __init__(self, dictionary):
        dict.__init__(self, dictionary)

