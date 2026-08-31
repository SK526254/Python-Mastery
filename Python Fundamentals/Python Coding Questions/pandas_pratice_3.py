import pandas as pd


df = pd.DataFrame({
    "customer_id": ["C1", "C2", "C1", "C3", "C2", "C3"],
    "amount": [500, 300, 700, 800, 450, 200],
    "status": ["SUCCESS", "FAILED", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]
})



# df = df[(df["status"]=="SUCCESS")&(df["amount"] >= 400)]
# df["fee"] = df["amount"] / 100*2

# print(df)

agg_df = df[(df["status"] == "SUCCESS")].groupby("customer_id")

df.agg()

print(agg_df)