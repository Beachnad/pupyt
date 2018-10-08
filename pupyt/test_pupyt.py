from pupyt.helper import starts_with
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

test_data_II = {
    'a': [1, 2, None, 4, 5],
    'b': ['a', 'b', 'c', 'd', None],
    'c': [None, 1, None, 1, 2]
}


dict_test_dev = {
    'name': ['alex', 'bob', 'charlie'],
    'balance': [100, 200, 250],
    'balloons': [0, 2, 1],
    'X': [0, 0, 0]
}

pupyt_test = PuPyT(test_data)
pupyt_test_big = PuPyT(big_test_data)
pupyt_test_II = PuPyT(test_data_II)
pupyt_test_dev = PuPyT(dict_test_dev)
pupyt_test_sales = PuPyT({
    'region': [1, 1, 1, 1, 2, 2, 2, 2],
    'product': [1, 1, 2, 2, 1, 1, 2, 2],
    'sales': [100, 50, 75, 90, 45, 235, 165, 20]
})


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
            {1: [{'a': 1, 'b': 1, 'd': 5}, {'a': 2, 'b': 2, 'd': 4}, {'a': 3, 'b': 3, 'd': 3}],
             2: [{'a': 4, 'b': 4, 'd': 2}, {'a': 5, 'b': 5, 'd': 1}]},
            pupyt_test.group_by('c')
        )

        "Perfomance tests."
        t0 = datetime.now()
        pupyt_test_big.group_by('a')
        t1 = datetime.now()
        self.assertGreaterEqual(4, (t1 - t0).seconds)

        t0 = datetime.now()
        pupyt_test_big.group_by(['c', 'a'])
        t1 = datetime.now()
        print((t1 - t0).seconds)
        self.assertGreaterEqual(8, (t1 - t0).seconds)

        self.assertEqual(
            {None: [{'a': 1, 'b': 'a'}, {'a': None, 'b': 'c'}],
             1: [{'a': 2, 'b': 'b'}, {'a': 4, 'b': 'd'}],
             2: [{'a': 5, 'b': None}]},
            pupyt_test_II.group_by(['c']))

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

    def test_filter_at(self):
        self.assertEqual(
            [{'a': 3, 'b': 3, 'c': 1, 'd': 3},
             {'a': 4, 'b': 4, 'c': 2, 'd': 2},
             {'a': 5, 'b': 5, 'c': 2, 'd': 1}],
            pupyt_test.filter_at(starts_with('a'), lambda x: x > 2))

        self.assertEqual(
            [{'X': 0, 'balance': 100, 'balloons': 0, 'name': 'alex'}],
            pupyt_test_dev.filter_at(starts_with('bal'), lambda x: x in (100, 0, 250)))

    # MUTATE ###########################################################################################################
    def test_mutate_at(self):
        self.assertEqual(
            [{'X': 0, 'balance': 1000, 'balloons': 0, 'name': 'alex'},
             {'X': 0, 'balance': 2000, 'balloons': 20, 'name': 'bob'},
             {'X': 0, 'balance': 2500, 'balloons': 10, 'name': 'charlie'}]
            , pupyt_test_dev.mutate_at(starts_with('bal'), lambda x: x * 10))

    # SUMMARISE ########################################################################################################
    def test_summarise(self):
        output = pupyt_test.group_by('c').\
            summarise(
                a=lambda x: sum(x['a']),
                b=lambda x: sum(x['b'])/x.nrow
            )

        self.assertEqual(
            [{'a': 6, 'b': 2.0, 'c': 1},
             {'a': 9, 'b': 4.5, 'c': 2}],
            output)

        region_sales = pupyt_test_sales.\
            group_by('region', 'product').\
            summarise(
                total_sales=lambda t: sum(t['sales']),
                avg_sales=  lambda t: sum(t['sales'])/t.nrow
            )

        self.assertEqual(
            {1: [{'avg_sales': 75.0, 'product': 1, 'total_sales': 150},
                 {'avg_sales': 82.5, 'product': 2, 'total_sales': 165}],
             2: [{'avg_sales': 140.0, 'product': 1, 'total_sales': 280},
                 {'avg_sales': 92.5, 'product': 2, 'total_sales': 185}]},
            region_sales
        )

        total_sales = region_sales.\
            summarise(
                total_sales=lambda t: sum(t['total_sales']),
                avg_sales=lambda t: sum(t['total_sales'])/t.nrow
            )

        self.assertEqual(
            [{'avg_sales': 157.5, 'region': 1, 'total_sales': 315},
             {'avg_sales': 232.5, 'region': 2, 'total_sales': 465}],
            total_sales
        )

    def test_summarise_all(self):
        summary_output = pupyt_test_sales.\
            group_by(['region', 'product']).\
            summarise_all(
                avg=lambda x: sum(x)/len(x),
                tot=lambda x: sum(x)
            )

        self.assertEqual(
            {1: [{'avg_sales': 75.0, 'product': 1, 'tot_sales': 150},
                 {'avg_sales': 82.5, 'product': 2, 'tot_sales': 165}],
             2: [{'avg_sales': 140.0, 'product': 1, 'tot_sales': 280},
                 {'avg_sales': 92.5, 'product': 2, 'tot_sales': 185}]},
            summary_output
        )

    def test_summarise_at(self):
        summary_output = pupyt_test_sales.\
            group_by(['region', 'product']).\
            summarise_at(
                starts_with('sal'),
                tst_avg=lambda x: sum(x)/len(x),
                tst_tot=lambda x: sum(x)
            )

        self.assertEqual(
            {1: [{'tst_avg_sales': 75.0, 'product': 1, 'tst_tot_sales': 150},
                 {'tst_avg_sales': 82.5, 'product': 2, 'tst_tot_sales': 165}],
             2: [{'tst_avg_sales': 140.0, 'product': 1, 'tst_tot_sales': 280},
                 {'tst_avg_sales': 92.5, 'product': 2, 'tst_tot_sales': 185}]},
            summary_output
        )

        summary_output_ii = summary_output.\
            summarise_at(
                starts_with('tst'),
                n=lambda x: len(x),
                avg=lambda x: sum(x)/len(x)
            )

        self.assertEqual(
            [{'avg_tst_avg_sales': 78.75,
              'avg_tst_tot_sales': 157.5,
              'n_tst_avg_sales': 2,
              'n_tst_tot_sales': 2,
              'region': 1},
             {'avg_tst_avg_sales': 116.25,
              'avg_tst_tot_sales': 232.5,
              'n_tst_avg_sales': 2,
              'n_tst_tot_sales': 2,
              'region': 2}],
            summary_output_ii
        )

    # SELECT ###########################################################################################################
    def test_select(self):
        self.assertEqual(
            [{'b': 1, 'c': 1},
             {'b': 2, 'c': 1},
             {'b': 3, 'c': 1},
             {'b': 4, 'c': 2},
             {'b': 5, 'c': 2}],
            pupyt_test.select('b', 'c')
        )

    def test_select_at(self):
        self.assertEqual(
            [{'balance': 100, 'balloons': 0},
             {'balance': 200, 'balloons': 2},
             {'balance': 250, 'balloons': 1}],
            pupyt_test_dev.select_at(starts_with('bal'))
        )



