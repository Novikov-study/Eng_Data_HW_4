import sqlite3
import pandas as pd
import json


def create_and_populate_database(csv_path, json_path, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS csv_movies (
        id INTEGER PRIMARY KEY,
        original_title TEXT,
        release_date TEXT,
        genre TEXT,
        duration INTEGER,
        country TEXT,
        director TEXT,
        income REAL,
        _votes_ INTEGER,
        score REAL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS json_movies (
        id INTEGER PRIMARY KEY,
        original_title TEXT,
        release_date TEXT,
        genre TEXT,
        duration INTEGER,
        country TEXT,
        director TEXT,
        income REAL,
        _votes_ INTEGER,
        score REAL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS combined_movies AS
    SELECT * FROM csv_movies
    UNION ALL
    SELECT * FROM json_movies;
    """)

    csv_data = pd.read_csv(csv_path, delimiter=';')
    if 'country' not in csv_data.columns:
        csv_data['country'] = None  # Добавляем столбец country, если отсутствует

    csv_data.to_sql("csv_movies", conn, if_exists="replace", index=False)

    json_data = pd.read_json(json_path, lines=True)
    if 'country' not in json_data.columns:
        json_data['country'] = None  # Добавляем столбец country, если отсутствует

    json_data.to_sql("json_movies", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()


def analyze_genre_income(connection):
    query = """
    SELECT genre, 
           COUNT(*) AS movie_count,
           SUM(income) AS total_income,
           MIN(income) AS min_income,
           MAX(income) AS max_income,
           AVG(income) AS avg_income
    FROM (
        SELECT genre, income FROM csv_movies
        UNION ALL
        SELECT genre, income FROM json_movies
    )
    GROUP BY genre
    ORDER BY total_income DESC;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def analyze_movies_by_year(connection):
    query = """
    SELECT strftime('%Y', release_date) AS year, 
           COUNT(*) AS movie_count,
           AVG(income) AS avg_income
    FROM combined_movies
    WHERE release_date IS NOT NULL AND income IS NOT NULL
    GROUP BY year
    ORDER BY year DESC;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def analyze_movies_by_country(connection):
    query = """
    SELECT country, 
           COUNT(*) AS movie_count,
           AVG(income) AS avg_income
    FROM combined_movies
    WHERE country IS NOT NULL AND income IS NOT NULL
    GROUP BY country
    ORDER BY movie_count DESC;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def select_top_movies_by_income(connection):
    query = """
    SELECT original_title, income
    FROM combined_movies
    WHERE income IS NOT NULL
    ORDER BY income DESC
    LIMIT 10;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def analyze_movies_by_year_and_income(connection):
    query = """
    SELECT strftime('%Y', release_date) AS year,
           COUNT(*) AS movie_count,
           AVG(income) AS avg_income
    FROM combined_movies
    WHERE release_date IS NOT NULL AND income IS NOT NULL
    GROUP BY year
    ORDER BY year;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def analyze_genre_distribution(connection):
    query = """
    SELECT genre,
           COUNT(*) AS movie_count,
           AVG(income) AS avg_income
    FROM combined_movies
    WHERE genre IS NOT NULL AND income IS NOT NULL
    GROUP BY genre
    ORDER BY movie_count DESC;
    """
    result = pd.read_sql_query(query, connection)
    return result.to_dict(orient="records")


def save_analysis_to_json(output_json_path, data):
    with open(output_json_path, "w") as file:
        json.dump(data, file, indent=4)


csv_path = "data/5/AllMoviesDetails_filtred.csv"
json_path = "data/5/Movie_cleaned.json"
db_path = "fourth.db"
output_json_path = "movie_analysis.json"

create_and_populate_database(csv_path, json_path, db_path)

conn = sqlite3.connect(db_path)
analysis = {
    "genre_income_analysis": analyze_genre_income(conn),
    "movies_by_year": analyze_movies_by_year(conn),
    "movies_by_country": analyze_movies_by_country(conn),
    "top_movies_by_income": select_top_movies_by_income(conn),
    "movies_by_year_and_income": analyze_movies_by_year_and_income(conn),
    "genre_distribution": analyze_genre_distribution(conn),
}
conn.close()

save_analysis_to_json(output_json_path, analysis)
