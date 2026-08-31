import pandas as pd


transactions = [
    {"customer_id": "C1", "amount": 500, "status": "SUCCESS"},
    {"customer_id": None, "amount": 300, "status": "SUCCESS"},
    {"customer_id": "C2", "amount": None, "status": "SUCCESS"},
    {"customer_id": "C1", "amount": "700", "status": "SUCCESS"},
    {"customer_id": "C2", "amount": 450, "status": "SUCCESS"},
    {"customer_id": "C3", "amount": "INVALID", "status": "SUCCESS"},
]
agg_amounts = {}
invalid_transactions = []

for transaction in transactions: 
    if transaction["status"] == "SUCCESS":
        # print(transaction["customer_id"] is None, " ", type(transaction["amount"]))
        if transaction["customer_id"] is None or type(transaction["amount"]) != type(1):
            invalid_transactions.append(transaction)
            continue

        if transaction["customer_id"] in agg_amounts.keys():
            agg_amounts[transaction["customer_id"]] += transaction["amount"]
        else:
           agg_amounts[transaction["customer_id"]] = transaction["amount"]
            
print(agg_amounts)
print(invalid_transactions)