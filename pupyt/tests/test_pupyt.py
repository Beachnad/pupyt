from pupyt import PuPyT
from unittest import TestCase
from datetime import datetime

from random import randint
rand_ints = lambda n:  [randint(1, 10) for _ in range(n)]
rand_list = lambda n, m: [randint(1, m) for _ in range(n)]

test_data = {
    'a': [1, 2, 3, 4, 5],
    'b': [1, 2, 3, 4, 5],
    'c': [1, 1, 1, 2, 2],
    'd': [5, 4, 3, 2, 1]
}

big_test_data = {
    'a': rand_ints(1000000),
    'b': rand_ints(1000000),
    'c': rand_list(1000000, 5)
}

pupyt_test = PuPyT(test_data)
pupyt_test_big = PuPyT(big_test_data)


class TestPuPyT(TestCase):
    def test_from_dict(self):
        self.assertEqual([{'a': 1, 'b': 1, 'c': 1, 'd': 5},
                          {'a': 2, 'b': 2, 'c': 1, 'd': 4},
                          {'a': 3, 'b': 3, 'c': 1, 'd': 3},
                          {'a': 4, 'b': 4, 'c': 2, 'd': 2},
                          {'a': 5, 'b': 5, 'c': 2, 'd': 1}], pupyt_test)

    def test_getitem(self):
        self.assertEqual([1, 2, 3, 4, 5], pupyt_test['a'])
        self.assertEqual([1, 2, 3, 4, 5], pupyt_test['b'])
        self.assertEqual([1, 1, 1, 2, 2], pupyt_test['c'])
        self.assertEqual([5, 4, 3, 2, 1], pupyt_test['d'])
        self.assertEqual({'a': 1, 'b': 1, 'c': 1, 'd': 5}, pupyt_test[0])
        self.assertEqual([{'a': 1, 'b': 1, 'c': 1, 'd': 5}, {'a': 2, 'b': 2, 'c': 1, 'd':4}], pupyt_test[0:2])

    def test_group_by(self):
        self.assertEqual(
            {1: [{'a': 1, 'b': 1, 'c': 1, 'd': 5}, {'a': 2, 'b': 2, 'c': 1, 'd': 4}, {'a': 3, 'b': 3, 'c': 1, 'd': 3}],
             2: [{'a': 4, 'b': 4, 'c': 2, 'd': 2}, {'a': 5, 'b': 5, 'c': 2, 'd': 1}]},
            pupyt_test.group_by('c')
        )

        "Perfomance tests."
        t0 = datetime.now()
        pupyt_test_big.group_by('a')
        t1 = datetime.now()
        self.assertGreaterEqual(2, (t1 - t0).seconds)

        t0 = datetime.now()
        pupyt_test_big.group_by(['c', 'a'])
        t1 = datetime.now()
        self.assertGreaterEqual(5, (t1 - t0).seconds)

    def test_sort_on(self):
        self.assertEqual(pupyt_test, pupyt_test.sort_on('a'))
        self.assertEqual(
            [{'a': 5, 'b': 5, 'c': 2, 'd': 1},
             {'a': 4, 'b': 4, 'c': 2, 'd': 2},
             {'a': 3, 'b': 3, 'c': 1, 'd': 3},
             {'a': 2, 'b': 2, 'c': 1, 'd': 4},
             {'a': 1, 'b': 1, 'c': 1, 'd': 5}],
            pupyt_test.sort_on('d'))

    def test_set_del_item(self):
        pupyt_test['new'] = [99, 99, 99, 99, 99]
        self.assertEqual(
            [{'a': 1, 'b': 1, 'c': 1, 'd': 5, 'new': 99}, {'a': 2, 'b': 2, 'c': 1, 'd': 4, 'new': 99},
             {'a': 3, 'b': 3, 'c': 1, 'd': 3, 'new': 99}, {'a': 4, 'b': 4, 'c': 2, 'd': 2, 'new': 99},
             {'a': 5, 'b': 5, 'c': 2, 'd': 1, 'new': 99}],
            pupyt_test
        )

        del(pupyt_test['new'])
        self.assertEqual(
            [{'a': 1, 'b': 1, 'c': 1, 'd': 5}, {'a': 2, 'b': 2, 'c': 1, 'd': 4},
             {'a': 3, 'b': 3, 'c': 1, 'd': 3}, {'a': 4, 'b': 4, 'c': 2, 'd': 2},
             {'a': 5, 'b': 5, 'c': 2, 'd': 1}],
            pupyt_test
        )
        
    def test_dict_functions(self):
        self.assertEqual(('a', 'b', 'c', 'd'), pupyt_test.keys())
        self.assertEqual(
            [[1, 2, 3, 4, 5],
             [1, 2, 3, 4, 5],
             [1, 1, 1, 2, 2],
             [5, 4, 3, 2, 1]], pupyt_test.values())
        self.assertEqual(
            [('a', [1, 2, 3, 4, 5]),
             ('b', [1, 2, 3, 4, 5]),
             ('c', [1, 1, 1, 2, 2]),
             ('d', [5, 4, 3, 2, 1])], pupyt_test.items()
        )
        self.assertEqual(
            {'a': [1, 2, 3, 4, 5],
             'b': [1, 2, 3, 4, 5],
             'c': [1, 1, 1, 2, 2],
             'd': [5, 4, 3, 2, 1]}, pupyt_test.as_dict()
        )
