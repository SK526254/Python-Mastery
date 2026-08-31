import csv
import logging
import math

print("Start Validation")




file_name = './inputs/transaction_data.csv'
# log_file = 'transactions_log.log'

class Result:
    def __init__(self):
     self.errors = []
     self.status = ""


class Validation:

    def __init__(self, file_name):
        self.valid_rows = 0
        self.invalid_rows = 0
        self.total_rows_processed = 0
        self.file_name = file_name

    def validate_row_data(self,transaction_id,amount, seen_transaction_ids):

            result = Result()

            if not transaction_id:
                result.errors.append("Missing Transaction ID")
                result.status = "Invalid Row"

            elif transaction_id in seen_transaction_ids:
                result.errors.append("Duplicate Transaction ID")
                result.status = "Duplicate Row"
    
            else: 
                seen_transaction_ids.add(transaction_id)
                result.status = "Valid Row"

            if not amount:
                result.status = "Invalid Row"
                result.errors.append("Missing Amount")

            try:
                amount_conv = float(amount)
                if math.isinf(amount_conv):
                    raise ValueError("Infinity")
                if math.isnan(amount_conv) :
                    raise ValueError("NaN")
                if amount_conv < 0:
                    raise ValueError("Negative Number")
            except ValueError as e:
                    result.status = "Invalid Row"
                    result.errors.append(e.__str__())
            return result

    def print_summary(self):

        print(f'\nTotal Rows Processed = {self.total_rows_processed}')
        print(f'Total Valid Rows  = {self.valid_rows}') 
        print(f'Total InValid Rows  = {self.invalid_rows}')


    def process_file(self):

        with (open(self.file_name, 'r') as transactions,
              open('./outputs/valid_rows.csv','w') as valid_rows,
              open('./outputs/duplicate_rows.csv','w') as duplicate_rows,
              open('./outputs/invalid_rows.csv','w') as invalid_rows
             ):
                rows = csv.DictReader(transactions)
                seen_transaction_ids = set()
                duplicate_rows_writer = csv.writer(duplicate_rows)
                invalid_rows_writer = csv.writer( invalid_rows)
                valid_rows_writer = csv.writer( valid_rows )

                for row in rows:
                    self.total_rows_processed +=1
                    transaction_id = row["transaction_id"]
                    amount = row["amount"]

                    result =  self.validate_row_data(transaction_id,amount, seen_transaction_ids)
                    if result.status == "Valid Row":
                        # logging.info(msg=f'Valid Row : tran Id {transaction_id} amount = {amount}')
                        valid_rows_writer.writerow(row)
                        self.valid_rows += 1
                    else: 
                        if result.status == "Duplicate Row":       
                            duplicate_rows_writer.writerow(row)
                            self.invalid_rows += 1    
                            # logging.warning(msg=f'Row {self.total_rows_processed}')
                            # for error in result.errors:
                            #     logging.warning(msg=f'           {error}') 
                        if result.status == "Invalid Row":       
                            invalid_rows_writer.writerow(row)
                            self.invalid_rows += 1    

validator = Validation(file_name)

validator.process_file()
validator.print_summary()

