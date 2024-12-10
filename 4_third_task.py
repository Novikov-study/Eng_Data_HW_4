import sqlite3
import pickle
import json


def create_table(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        UNIQUE (artist, song, year)
    );
    """)
    connection.commit()


def load_part1(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read().strip().split('====')
        for block in content:
            if not block.strip():
                continue
            fields = block.strip().split('\n')
            record = {}
            for field in fields:
                if '::' not in field:
                    continue
                key, value = field.split('::', 1)
                record[key.strip()] = value.strip()
            if 'duration_ms' in record:
                record['duration_ms'] = int(record['duration_ms'])
            if 'year' in record:
                record['year'] = int(record['year'])
            if 'tempo' in record:
                record['tempo'] = float(record['tempo'])
            data.append(record)
    return data


def load_part2(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data


def process_part2(data):
    processed_data = []
    for record in data:
        try:
            processed_data.append({
                'artist': record.get('artist', None),
                'song': record.get('song', None),
                'duration_ms': int(record.get('duration_ms', 0)),
                'year': int(record.get('year', 0)),
                'tempo': float(record.get('tempo', 0)),
                'genre': record.get('genre', '')
            })
        except (TypeError, ValueError):
            continue
    return processed_data


def insert_data(connection, data):
    cursor = connection.cursor()
    for record in data:
        if not record.get('artist') or not record.get('song') or not record.get('year'):
            continue  # Пропускаем записи
        cursor.execute("""
        SELECT COUNT(*) FROM music_data
        WHERE artist = ? AND song = ? AND year = ?;
        """, (record['artist'], record['song'], record['year']))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            INSERT INTO music_data (
                artist, song, duration_ms, year, tempo, genre
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """, (
                record.get('artist'),
                record.get('song'),
                record.get('duration_ms'),
                record.get('year'),
                record.get('tempo'),
                record.get('genre')
            ))
    connection.commit()


def export_to_json(connection, output_path, sort_field, limit):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM music_data ORDER BY {sort_field} ASC LIMIT ?;", (limit,))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def calculate_statistics(connection, field):
    cursor = connection.cursor()
    cursor.execute(f"SELECT SUM({field}), MIN({field}), MAX({field}), AVG({field}) FROM music_data;")
    return cursor.fetchone()


def calculate_frequency(connection, field):
    cursor = connection.cursor()
    cursor.execute(f"SELECT {field}, COUNT(*) FROM music_data GROUP BY {field};")
    return cursor.fetchall()


def export_filtered_sorted_to_json(connection, output_path, predicate, sort_field, limit):
    import json
    cursor = connection.cursor()
    query = f"SELECT * FROM music_data WHERE {predicate} ORDER BY {sort_field} ASC LIMIT ?;"
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


db_path = 'second.db'
part1_path = 'data/3/_part_1.text'
part2_path = 'data/3/_part_2.pkl'

conn = sqlite3.connect(db_path)
create_table(conn)

part1_data = load_part1(part1_path)
part2_data = load_part2(part2_path)
processed_part2_data = process_part2(part2_data)

all_data = part1_data + processed_part2_data
insert_data(conn, all_data)

export_to_json(conn, 'third_task_sorted.json', 'duration_ms', 26)
stats = calculate_statistics(conn, 'tempo')
print('Статистика по tempo:', stats)

frequency = calculate_frequency(conn, 'genre')
print('Частота по жанрам:', frequency)

export_filtered_sorted_to_json(conn, 'third_task_filtered.json', 'year > 2010', 'duration_ms', 31)

conn.close()
