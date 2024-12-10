import pandas as pd
import pickle
import sqlite3
import json
import numpy as np


def create_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        quantity INTEGER,
        category TEXT,
        fromCity TEXT,
        isAvailable INTEGER DEFAULT 0,
        views INTEGER DEFAULT 0,
        update_count INTEGER DEFAULT 0
    );
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_counter_trigger
    AFTER UPDATE ON products_data
    BEGIN
        UPDATE products_data
        SET update_count = update_count + 1
        WHERE id = NEW.id;
    END;
    """)
    connection.commit()

def load_csv_to_dataframe(csv_file_path):
    df = pd.read_csv(csv_file_path, delimiter=";")

    # Обработка каждой строки
    def fix_row(row):
        # Проверяем, на месте ли category
        if not (isinstance(row["category"], str) and row["category"].islower() and row["category"].isalpha()):
            # Смещение данных
            row["fromCity"], row["isAvailable"], row["views"] = row["category"], row["fromCity"], row["isAvailable"]
            row["category"] = None
        return row

    df = df.apply(fix_row, axis=1)
    df["category"] = df["category"].fillna("Unknown")
    df["isAvailable"] = df["isAvailable"].map({"True": 1, "False": 0}).fillna(0).astype(int)

    df = df[["name", "price","quantity", "category", "fromCity", "isAvailable", "views"]]

    return df


def save_dataframe_to_db(connection, dataframe):
    dataframe.to_sql("products_data", connection, if_exists="append", index=False)


def apply_updates_from_pickle(connection, pickle_file_path):
    with open(pickle_file_path, "rb") as file:
        updates = pickle.load(file)

    connection.execute("BEGIN TRANSACTION;")
    cursor = connection.cursor()

    for update in updates:
        name, method, param = update["name"], update["method"], update["param"]

        cursor.execute("SELECT price, views, quantity FROM products_data WHERE name = ?;", (name,))
        row = cursor.fetchone()

        if not row:
            continue

        current_price, current_views, current_quantity = row

        if method == "available":
            cursor.execute("UPDATE products_data SET isAvailable = ? WHERE name = ?;", (param, name))

        elif method == "price_abs":
            new_price = max(1, param)
            cursor.execute("UPDATE products_data SET price = ? WHERE name = ?;", (new_price, name))

        elif method == "price_percent":
            new_price = max(1, current_price * (1 + param))
            cursor.execute("UPDATE products_data SET price = ? WHERE name = ?;", (new_price, name))

        elif method == "quantity_add":
            new_quantity = max(1, current_quantity + param)
            cursor.execute("UPDATE products_data SET quantity = ? WHERE name = ?;", (new_quantity, name))

        elif method == "quantity_sub":
            new_quantity = max(1, current_quantity - param)
            cursor.execute("UPDATE products_data SET quantity = ? WHERE name = ?;", (new_quantity, name))

        elif method == "remove":
            cursor.execute("DELETE FROM products_data WHERE name = ?;", (name,))

    connection.commit()

def get_top_updated_products(connection):
    query = """
    SELECT DISTINCT name, update_count
    FROM products_data
    ORDER BY update_count DESC
    LIMIT 10;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def analyze_prices(connection):
    query = """
    SELECT 
        category,
        SUM(price) AS total_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,
        COUNT(*) AS product_count
    FROM products_data
    GROUP BY category;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def analyze_quantities(connection):
    query = """
    SELECT 
        category,
        SUM(quantity) AS total_quantity,
        MIN(quantity) AS min_quantity,
        MAX(quantity) AS max_quantity,
        AVG(quantity) AS avg_quantity,
        COUNT(*) AS product_count
    FROM products_data
    GROUP BY category;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def get_top_updated_with_high_prices(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT AVG(price) FROM products_data;")
    avg_price = cursor.fetchone()[0]

    query = """
    SELECT name, update_count, price
    FROM products_data
    WHERE price > ?
    ORDER BY update_count DESC
    LIMIT 10;
    """
    cursor.execute(query, (avg_price,))
    result = cursor.fetchall()
    return avg_price, result

def save_analysis_to_json(connection, file_path):
    top_updated = get_top_updated_products(connection)
    price_analysis = analyze_prices(connection)
    quantity_analysis = analyze_quantities(connection)
    avg_price, high_price_products = get_top_updated_with_high_prices(connection)

    analysis_results = {
        "top_10_updated_products": [{"name": row[0], "updates": row[1]} for row in top_updated],
        "price_analysis": [
            {
                "category": row[0],
                "total_price": row[1],
                "min_price": row[2],
                "max_price": row[3],
                "avg_price": row[4],
                "product_count": row[5]
            }
            for row in price_analysis
        ],
        "quantity_analysis": [
            {
                "category": row[0],
                "total_quantity": row[1],
                "min_quantity": row[2],
                "max_quantity": row[3],
                "avg_quantity": row[4],
                "product_count": row[5]
            }
            for row in quantity_analysis
        ],
        "avg_price_and_high_price_products": {
            "avg_price": avg_price,
            "products": [
                {"name": product[0], "updates": product[1], "price": product[2]}
                for product in high_price_products
            ]
        }
    }

    with open(file_path, "w") as file:
        json.dump(analysis_results, file, indent=4)



# Основной код
db_path = "third.db"
conn = sqlite3.connect(db_path)

# Создаем таблицу, если ее нет
create_table(conn)

csv_file_path = "./data/4/_product_data.csv"
pickle_file_path = "./data/4/_update_data.pkl"
df = load_csv_to_dataframe(csv_file_path)
#print(df.head())
save_dataframe_to_db(conn, df)
apply_updates_from_pickle(conn, pickle_file_path)

top_products = get_top_updated_products(conn)
print("Топ-10 обновляемых товаров:", top_products)

price_analysis = analyze_prices(conn)
print("Анализ цен по категориям:")
for row in price_analysis:
    print(row)

quantity_analysis = analyze_quantities(conn)
print("Анализ остатков по категориям:")
for row in quantity_analysis:
    print(row)

avg_price, high_price_products = get_top_updated_with_high_prices(conn)
print(f"Средняя цена по базе данных: {avg_price}")
print("Топ-10 товаров с ценой выше средней и максимальным числом обновлений:")
for product in high_price_products:
    print(product)

analysis_json_path = "fourth_task_analysis.json"
save_analysis_to_json(conn, analysis_json_path)


conn.close()
