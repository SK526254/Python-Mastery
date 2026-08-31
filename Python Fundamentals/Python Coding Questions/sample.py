def add_transaction(amount, tran_list = []):
    if amount > 0: 
        tran_list.append(amount)
    return tran_list


print(add_transaction(100))
print(add_transaction(200))