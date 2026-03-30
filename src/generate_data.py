import pandas as pd
import numpy as np

n = 1_000_000

data = {
    "order_id": np.arange(1, n+1),
    "customer_id": np.random.randint(1000, 2000, n),
    "product_id": np.random.randint(1, 100, n),
    "quantity": np.random.randint(1, 5, n),
    "price": np.random.randint(100, 5000, n),
    "order_date": pd.date_range(start="2023-01-01", periods=n, freq="min")
}

df = pd.DataFrame(data)
df.to_csv("data/sales.csv", index=False)

print("Generated 1M+ rows dataset")
