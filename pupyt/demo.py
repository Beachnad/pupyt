from pupyt import PuPyT
from helper import starts_with

sales_data = {
    'name': ['alice', 'bob', 'charlie', 'dan', 'elle'],
    'region': [1, 1, 2, 2, 2],
    'sales': [100, 250, 45, 25, 340],
    'product_1': [50, 125, 15, 5, 100],
    'product_2': [25, 100, 15, 5, 120],
    'product_3': [25, 25, 15, 15, 120]
}

sales_pupyt = PuPyT(sales_data)

#### MUTATE AT ####
mutate_at = sales_pupyt.mutate_at(starts_with('product'), lambda x: x * 10)

"group by sales region and calculate the average volume of sales for each product"
demo_1 = sales_pupyt.\
    group_by('region').\
    summarise_at(
    starts_with('product'),
    avg=lambda x: sum(x)/len(x)
)
print(demo_1)

"group by sales region and calculate some metrics from the total sales"
demo_2 = sales_pupyt.\
    group_by('region').\
    summarise(
        average=lambda x: sum(x['sales'])/len(x['sales']),
        total=lambda x: sum(x['sales']),
        odd_values=lambda x: sum(1 for s in x['sales'] if s%2==1)
    )
print(demo_2)

