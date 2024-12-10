import sqlite3
import csv

def create_reviews_table(connection):
    create_table_query = """
    CREATE TABLE property_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        property_name TEXT,
        rating REAL,
        convenience INTEGER,
        security INTEGER,
        functionality INTEGER,
        comment TEXT,
        FOREIGN KEY (property_name) REFERENCES properties(name)
    );
    """
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()


def load_reviews_from_csv(connection, csv_file_path):
    with open(csv_file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=";")
        insert_query = """
        INSERT INTO property_reviews (property_name, rating, convenience, security, functionality, comment)
        VALUES (?, ?, ?, ?, ?, ?);
        """
        cursor = connection.cursor()
        for row in reader:
            cursor.execute(insert_query, (
                row['name'], float(row['rating']), int(row['convenience']),
                int(row['security']), int(row['functionality']), row['comment']
            ))
        connection.commit()


def execute_queries(connection):
    cursor = connection.cursor()

    query1 = """
    SELECT p.name, AVG(r.rating) AS avg_rating
    FROM properties p
    JOIN property_reviews r ON p.name = r.property_name
    GROUP BY p.name;
    """
    cursor.execute(query1)
    avg_ratings = cursor.fetchall()

    query2 = """
    SELECT p.name, p.city, r.rating
    FROM properties p
    JOIN property_reviews r ON p.name = r.property_name
    WHERE r.rating > 4;
    """
    cursor.execute(query2)
    high_rated = cursor.fetchall()

    query3 = """
    SELECT p.name, COUNT(r.id) AS review_count
    FROM properties p
    JOIN property_reviews r ON p.name = r.property_name
    GROUP BY p.name;
    """
    cursor.execute(query3)
    review_counts = cursor.fetchall()

    return avg_ratings, high_rated, review_counts

db_path = "first.db"
csv_file_path = "./data/1-2/subitem.csv"
conn = sqlite3.connect(db_path)
#create_reviews_table(conn)
#load_reviews_from_csv(conn, csv_file_path)
avg_ratings, high_rated, review_counts = execute_queries(conn)
conn.close()

print("Средний рейтинг по каждому объекту:")
print(avg_ratings)

print("\nОбъекты с рейтингом выше 4:")
print(high_rated)

print("\nКоличество отзывов для каждого объекта:")
print(review_counts)
