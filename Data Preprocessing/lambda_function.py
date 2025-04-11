import numpy as np
import pandas as pd
import boto3
import io
import ast
from datetime import datetime
import os

s3_client = boto3.client("s3")

def get_latest_file(bucket_name: str, folder_name: str):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder_name}/")

        if "Contents" not in response:
            print(f"No files found in {folder_name}")
            return None
        
        files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
        latest_file_key = files[0]["Key"]
        print(f"Latest file in {folder_name}: {latest_file_key}")

        return latest_file_key
    except Exception as e:
        print(f"Error fetching latest file: {e}")
        return None

def read_csv_from_s3(bucket_name: str, file_key: str):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response["Body"].read()
        print(f"Read {file_key}")
        data = pd.read_csv(io.BytesIO(file_content))
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def convert(obj):
    try:
        return [i['name'] for i in ast.literal_eval(obj)]
    except Exception:
        return []

def fetch_director(obj):
    try:
        return [i['name'] for i in ast.literal_eval(obj) if i['job'] == 'Director'][:1]
    except Exception:
        return []

def lambda_handler(event, context):
    bucket_name = os.getenv("TRAIN_BUCKET_NAME")
    preprocessed_bucket_name = os.getenv("PREPROCESSED_BUCKET_NAME")
    folder_name = os.getenv("PREPROCESSED_FOLDER_NAME")

    latest_movies_file = get_latest_file(bucket_name, "Movies")
    latest_credits_file = get_latest_file(bucket_name, "Credits")

    if not latest_movies_file or not latest_credits_file:
        return {"status": "Error", "message": "Missing required files"}

    movies = read_csv_from_s3(bucket_name, latest_movies_file)
    credits = read_csv_from_s3(bucket_name, latest_credits_file)

    if movies is None or credits is None:
        return {"status": "Error", "message": "Failed to read files"}

    movies = movies.merge(credits, on="title")

    movies = movies[['movie_id', 'title', 'overview', 'genres', 'keywords', 'cast', 'crew']]
    movies.dropna(inplace=True)

    movies['genres'] = movies['genres'].apply(convert)
    movies['keywords'] = movies['keywords'].apply(convert)
    movies['cast'] = movies['cast'].apply(convert)
    movies['crew'] = movies['crew'].apply(fetch_director)

    movies['overview'] = movies['overview'].apply(lambda x: x.split())
    for col in ['genres', 'keywords', 'cast', 'crew']:
        movies[col] = movies[col].apply(lambda x: [i.replace(" ", "") for i in x])

    movies['tags'] = movies['overview'] + movies['genres'] + movies['keywords'] + movies['cast'] + movies['crew']
    new_df_movies = movies[['movie_id', 'title', 'tags']].copy()
    new_df_movies['tags'] = new_df_movies['tags'].apply(lambda x: " ".join(x).lower())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"preprocessed_data_{timestamp}.csv"

    csv_buffer = io.StringIO()
    new_df_movies.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=preprocessed_bucket_name,
        Key=f"{folder_name}/{file_name}",
        Body=csv_buffer.getvalue()
    )

    return {"status": "Success", "file_path": f"s3://{preprocessed_bucket_name}/{folder_name}/{file_name}"}
