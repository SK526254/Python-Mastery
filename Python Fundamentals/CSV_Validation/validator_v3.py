import csv
import logging
import math

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s : %(message)s"
)


INPUT_FILE = "./inputs/transaction_data copy.csv"

VALID = "VALID"
INVALID = "INVALID"
DUPLICATE = "DUPLICATE"


class Result:

    def __init__(self):
        self.status = VALID
        self.errors = []


class Validation:

    def __init__(self, file_name):

        self.file_name = file_name

        self.total_rows_processed = 0
        self.valid_rows = 0
        self.invalid_rows = 0
        self.duplicate_rows = 0

    def validate_row(self, row, seen_transaction_ids):

        result = Result()

        transaction_id = row.get("transaction_id", "").strip()
        amount = row.get("amount", "").strip()

        # -----------------------------
        # Transaction ID Validation
        # -----------------------------

        if not transaction_id:
            result.status = INVALID
            result.errors.append("Missing Transaction ID")

        elif transaction_id in seen_transaction_ids:
            result.status = DUPLICATE
            result.errors.append("Duplicate Transaction ID")

        else:
            seen_transaction_ids.add(transaction_id)

        # -----------------------------
        # Amount Validation
        # -----------------------------

        if not amount:
            result.status = INVALID
            result.errors.append("Missing Amount")

        else:

            try:

                amount_value = float(amount)

                if math.isnan(amount_value):
                    raise ValueError("NaN")

                if math.isinf(amount_value):
                    raise ValueError("Infinity")

                if amount_value < 0:
                    raise ValueError("Negative Amount")

            except ValueError as ex:

                result.status = INVALID
                result.errors.append(str(ex))

        return result

    def print_summary(self):

        print("\n==============================")
        print("Validation Summary")
        print("==============================")

        print(f"Rows Processed : {self.total_rows_processed}")
        print(f"Valid Rows     : {self.valid_rows}")
        print(f"Invalid Rows   : {self.invalid_rows}")
        print(f"Duplicate Rows : {self.duplicate_rows}")

        print("==============================")

    def process_file(self):

        with (
            open(self.file_name, "r", newline="") as input_file,
            open("./outputs/valid_rows.csv", "w", newline="") as valid_file,
            open("./outputs/invalid_rows.csv", "w", newline="") as invalid_file,
            open("./outputs/duplicate_rows.csv", "w", newline="") as duplicate_file,
        ):

            reader = csv.DictReader(input_file)

            valid_writer = csv.DictWriter(
                valid_file,
                fieldnames=["transaction_id", "amount"]
            )

            invalid_writer = csv.DictWriter(
                invalid_file,
                fieldnames=["transaction_id", "amount", "errors"]
            )

            duplicate_writer = csv.DictWriter(
                duplicate_file,
                fieldnames=["transaction_id", "amount", "errors"]
            )

            valid_writer.writeheader()
            invalid_writer.writeheader()
            duplicate_writer.writeheader()

            seen_transaction_ids = set()

            for row in reader:

                self.total_rows_processed += 1

                result = self.validate_row(row, seen_transaction_ids)

                if result.status == VALID:

                    valid_writer.writerow({
                        "transaction_id": row["transaction_id"],
                        "amount": row["amount"]
                    })

                    self.valid_rows += 1

                elif result.status == DUPLICATE:

                    duplicate_writer.writerow({
                        "transaction_id": row["transaction_id"],
                        "amount": row["amount"],
                        "errors": "; ".join(result.errors)
                    })

                    self.duplicate_rows += 1

                    logging.warning(
                        f"Row {self.total_rows_processed}: "
                        f"{'; '.join(result.errors)}"
                    )

                else:

                    invalid_writer.writerow({
                        "transaction_id": row["transaction_id"],
                        "amount": row["amount"],
                        "errors": "; ".join(result.errors)
                    })

                    self.invalid_rows += 1

                    logging.warning(
                        f"Row {self.total_rows_processed}: "
                        f"{'; '.join(result.errors)}"
                    )


validator = Validation(INPUT_FILE)

validator.process_file()

validator.print_summary()