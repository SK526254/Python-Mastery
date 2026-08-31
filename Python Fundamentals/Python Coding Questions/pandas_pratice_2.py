import json
import os

checkpoint_file = "checkpoint.json"


def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return json.load(f)["last_batch"]

    return 0


def save_checkpoint(batch_number):
    with open(checkpoint_file, "w") as f:
        json.dump({"last_batch": batch_number}, f)

def process_batch(transaction, invalid_transactions, agg_amounts):
    if transaction["status"] == "SUCCESS":
        # print(transaction["customer_id"] is None, " ", type(transaction["amount"]))
        if transaction["customer_id"] is None or type(transaction["amount"]) != type(1):
            invalid_transactions.append(transaction)
            return

        if transaction["customer_id"] in agg_amounts.keys():
            agg_amounts[transaction["customer_id"]] += transaction["amount"]
        else:
           agg_amounts[transaction["customer_id"]] = transaction["amount"]


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


last_completed_batch = load_checkpoint()
batch_number = 0


for batch_number, batch in enumerate(transactions, start=1):

    # Skip batches that were already successfully processed
    if batch_number <= last_completed_batch:
        continue

    try:
        print(f"Processing batch {batch_number}: {batch}")

        # Your actual processing
        process_batch(batch, invalid_transactions, agg_amounts)

        # Only checkpoint AFTER successful processing
        save_checkpoint(batch_number)

    except Exception as e:
        print(f"Batch {batch_number} failed: {e}")
        break

if batch_number == transactions.__len__():
    # os.remove(checkpoint_file)
    with open(checkpoint_file, 'w') as f:   
        json.dump({"last_batch":0} , f)

print(agg_amounts)
print(invalid_transactions)



