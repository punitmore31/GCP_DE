def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        print(f"checking dict or not")
        if type(x) is dict:
            print("dict type")
            for a in x:
                print(f"a : {a}")
                flatten(x[a], name + a + ".")
        else:
            print("not dict type")
            out[name[:-1]] = x

    flatten(y)
    return out


json_data = {
    "user": {
        "id": 1,
        "name": "Alice",
        "address": {"city": "New York", "country": "USA"},
    }
}

flat_data = flatten_json(json_data)
print(flat_data)
