def process_order(order_list, user_dict, item_index):
    # 1. TypeError Check: Is order_list actually a list?
    if not isinstance(order_list, list):
        raise TypeError("order_list must be a list type")

    # 2. IndexError Check: Did they ask for item #100 in a list of 5?
    if item_index >= len(order_list):
        raise IndexError("Item index is out of range")

    # 3. KeyError Check: Does the user dictionary have a 'balance'?
    if "balance" not in user_dict:
        raise KeyError("User dictionary is missing 'balance' key")

    # 4. ValueError Check: Is the balance negative?
    if user_dict["balance"] < 0:
        raise ValueError("User balance cannot be negative")

    print("Order processed!")


if __name__ == '__main__':
    process_order(
        ["apple", "mango", "orange"],
        {"name": "Punit", "balance": 12345, "city": "Jalgoan"},
        2,
    )

    