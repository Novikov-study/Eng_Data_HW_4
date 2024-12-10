import pickle

def show_pickle(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data


def extract_unique(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)

    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        unique_methods = {item['method'] for item in data if 'method' in item}
    else:
        raise ValueError("Нет")
    return sorted(list(unique_methods))

file_path = "./data/4/_update_data.pkl"
print(extract_unique(file_path))
print(show_pickle(file_path))

