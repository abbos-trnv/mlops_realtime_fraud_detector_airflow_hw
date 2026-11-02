"""Create deterministic synthetic data for CI only; not a substitute for train.csv."""
import csv
from datetime import datetime, timedelta
from pathlib import Path


def main():
    destination = Path('data/ci_transactions.csv')
    destination.parent.mkdir(parents=True, exist_ok=True)
    columns = ['transaction_time', 'merch', 'cat_id', 'amount', 'name_1', 'name_2',
               'gender', 'street', 'one_city', 'us_state', 'post_code', 'lat', 'lon',
               'population_city', 'jobs', 'merchant_lat', 'merchant_lon', 'target']
    with destination.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for i in range(400):
            writer.writerow(dict(zip(columns, [
                (datetime(2020, 1, 1) + timedelta(hours=i)).isoformat(' '),
                'merchant_' + str(i % 7), ['food', 'travel', 'shopping'][i % 3],
                [10, 99.99, 100, 499.99, 500, 1000][i % 6],
                'Client' + str(i % 20), 'Example', 'M' if i % 2 else 'F',
                'Test street', 'Test city', 'CA' if i % 2 else 'NY',
                '10000', 40.0, -70.0, 1000, 'Engineer', 40.1, -70.1,
                int(i % 13 == 0),
            ])))
    print(destination)


if __name__ == '__main__':
    main()
