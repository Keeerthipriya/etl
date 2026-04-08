import pandas as pd
import numpy as np

def generate_data(n=100):
    data = {
        "order_id": [f"O{i}" for i in range(n)],
        "order_date": pd.date_range(start="2024-01-01", periods=n, freq="D"),
        "store_id": np.random.choice(["S1", "S2", "S3"], n),
        "product_id": np.random.choice(["P1", "P2", "P3"], n),
        "category": np.random.choice(["Electronics", "Clothing", "Grocery"], n),
        "quantity": np.random.randint(-5, 10, n),
        "unit_price": np.random.choice([100, 200, None], n)
    }

    df = pd.DataFrame(data)
    df.to_csv("sample_data.csv", index=False)

# 👉 CALL THE FUNCTION
generate_data(200)