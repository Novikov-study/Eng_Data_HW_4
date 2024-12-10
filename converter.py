import pandas as pd
import json


def csv_to_json(input_csv_path, output_json_path):
    df = pd.read_csv(input_csv_path)

    data = df.to_dict(orient="records")

    with open(output_json_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)


def filter_dataset(csv_file_path, output_file_path):

    try:
        data = pd.read_csv(csv_file_path, sep=';', engine='python')

        columns_to_keep = [
            "id", "original_title", "release_date", "genres", "runtime",
            "production_countries", "revenue", "vote_average", "vote_count"
        ]

        filtered_data = data[columns_to_keep]

        filtered_data.rename(columns={
            "genres": "genre",
            "runtime": "duration",
            "production_countries": "country",
            "revenue": "income",
            "vote_average": "score",
            "vote_count": "_votes_"
        }, inplace=True)

        filtered_data.to_csv(output_file_path, index=False, sep=';')

    except Exception as e:
        print(f"Ошибка {e}")

# def transform_data_format(csv_path, json_path):
#     csv_data = pd.read_csv(csv_path, delimiter=';')
#     csv_data['release_date'] = pd.to_datetime(csv_data['release_date'], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
#     csv_data['genre'] = csv_data['genre'].str.split('|').str[0]
#     csv_data.to_csv(csv_path, index=False, sep=';')
#
#     json_data = pd.read_json(json_path)
#     json_data['release_date'] = pd.to_datetime(json_data['release_date'], errors='coerce').dt.strftime('%Y-%m-%d')
#     json_data['genre'] = json_data['genre'].str.split('|').str[0]
#     json_data.to_json(json_path, orient='records', lines=True)

filter_dataset("./data/5/AllMoviesDetailsCleaned.csv", "./data/5/AllMoviesDetails_filtred.csv")

#transform_data_format(csv_path, json_path)
#input_csv_path = "./data/5/Movie_cleaned (1).csv"
#output_json_path = "./data/5/Movie_cleaned.json"

#csv_to_json(input_csv_path, output_json_path)