import csv

print("Start Validation")
valid_rows = 0
invalid_rows = 0
total_rows_processed = 0



with open('transaction_data.csv', 'r') as transactions:
    rows = csv.DictReader(transactions)
    dist_tran_ids = set()

    for row in rows:
        transaction_id = row["transaction_id"]
        amount = row["amount"]
        total_rows_processed +=1
        errors = []

        if not transaction_id:
            errors.append("Missing Transaction ID")
        elif transaction_id in dist_tran_ids:
            errors.append("Duplicate Transaction ID")
        else: 
            dist_tran_ids.add(transaction_id)

        if not amount:
            errors.append("Missing Amount")
        # try:
        #     if type(transaction_id) != int: raise("Invalid Tran ID")
        # except Exception as e:
        #         invalid = True
        #         print(f'Row {total_rows_processed} : Invalid Transaction ID {transaction_id}')

        try:
            amount_conv = float(amount)
            if amount_conv == float('Infinity'):
                raise ValueError("Infinity")
            if amount_conv == float('Nan'):
                raise ValueError("NaN")
            if amount_conv < 0:
                raise ValueError("Negative Number")
        except ValueError as e:
                errors.append("Invalid Amount")

        if errors:       
                 invalid_rows += 1     
                 print(f'Row {total_rows_processed} : {errors}')
        else:
            print( f'Valid Row : tran Id {transaction_id} amount = {amount}')
            valid_rows += 1

print(f'\nTotal Rows Processed = {total_rows_processed}')
print(f'Total Valid Rows  = {valid_rows}')
print(f'Total InValid Rows  = {invalid_rows}')