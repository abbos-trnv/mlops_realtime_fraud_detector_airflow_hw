"""Create deterministic CI-only transactions spanning the retraining window."""
import csv
from datetime import datetime, timedelta
from pathlib import Path


def main():
    path = Path("data/monitoring-ci.csv")
    path.parent.mkdir(exist_ok=True)
    columns = "transaction_time merch cat_id amount name_1 name_2 gender street one_city us_state post_code lat lon population_city jobs merchant_lat merchant_lon target".split()
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        for i in range(6000):
            amount = 1000.0 if i % 5 == 0 else 10.0 + i % 80
            writer.writerow([
                (datetime(2019, 1, 1) + timedelta(hours=i % (24*120))).strftime("%Y-%m-%d %H:%M"),
                "fraud_Test Merchant " + str(i % 4), ["food_dining", "shopping_net"][i % 2],
                amount, "Test", "Customer", ["F", "M"][i % 2], "Main Street",
                "Test city", ["CA", "NY"][i % 2], 10000, 40.0, -70.0, 1000,
                "Engineer", 40.1, -70.1, int(amount >= 500),
            ])
    print(path)


if __name__ == "__main__":
    main()
