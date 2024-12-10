import sqlite3
import json


def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def create_table(connection):
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS properties (
        id INTEGER PRIMARY KEY,
        name TEXT,
        street TEXT,
        city TEXT,
        zipcode INTEGER,
        floors INTEGER,
        year INTEGER,
        parking INTEGER,
        prob_price INTEGER,
        views INTEGER
        UNIQUE (name)
    );
    """)
    connection.commit()


def insert_data(connection, data):
    cursor = connection.cursor()

    prepared_data = [
        (
            item["id"],
            item["name"],
            item["street"],
            item["city"],
            item["zipcode"],
            item["floors"],
            item["year"],
            int(item["parking"]),  # Преобразуем boolean в int
            item["prob_price"],
            item["views"]
        )
        for item in data
    ]

    cursor.executemany("""
    INSERT INTO properties (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, prepared_data)

    connection.commit()


def export_sorted_to_json(connection, output_path, sort_field, limit):

    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM properties ORDER BY {sort_field} ASC LIMIT ?;", (limit,))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    result = [dict(zip(columns, row)) for row in rows]

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=4)


def calculate_statistics(connection, field):

    cursor = connection.cursor()
    cursor.execute(f"SELECT SUM({field}), MIN({field}), MAX({field}), AVG({field}) FROM properties;")
    return cursor.fetchone()


def calculate_frequency(connection, field):

    cursor = connection.cursor()
    cursor.execute(f"SELECT {field}, COUNT(*) FROM properties GROUP BY {field};")
    return cursor.fetchall()


def export_filtered_sorted_to_json(connection, output_path, predicate, sort_field, limit):

    cursor = connection.cursor()
    query = f"""
    SELECT * FROM properties WHERE {predicate} ORDER BY {sort_field} ASC LIMIT ?;
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    result = [dict(zip(columns, row)) for row in rows]

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=4)



file_path = 'data/1-2/item.json'
db_path = "first.db"


data = load_json(file_path)
conn = sqlite3.connect(db_path)
#create_table(conn)
#insert_data(conn, data)

var_plus_10 = 26
output_path_sorted = "first_task_sorted_properties.json"
output_path_filtered_sorted = "first_task_filtered_sorted_properties.json"

# Экспорт первых 26 записей отсортированных
export_sorted_to_json(conn, output_path_sorted, "prob_price", var_plus_10)

# Статистика по полю views
statistics_views = calculate_statistics(conn, "views")
print("Статистика по views: ", statistics_views)

# Частоты по городам
frequency_city = calculate_frequency(conn, "city")
print("Частота по городам: ", frequency_city)

# Экспорт первых 26 отфильтрованных по parking=1 и отсортированных по views
export_filtered_sorted_to_json(conn, output_path_filtered_sorted, "parking=1", "views", var_plus_10)
conn.close()
