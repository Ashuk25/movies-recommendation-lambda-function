import numpy as np
import pandas as pd
import boto3
import io
import os
import pickle
from datetime import datetime
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

s3_client = boto3.client("s3")

# Read bucket names from environment variables
SOURCE_BUCKET = os.getenv("PREPROCESSED_BUCKET_NAME")
DEST_BUCKET = os.getenv("MODEL_BUCKET_NAME")
MOVIES_FOLDER = "Movies"
SIMILARITY_FOLDER = "Similarity"

# Function to fetch latest file from an S3 folder
def get_latest_file(bucket_name: str, folder_name: str):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name + "/")
        if "Contents" not in response:
            print(f"No files found in {folder_name}")
            return None
        
        files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
        return files[0]["Key"]
    except Exception as e:
        print(f"Error fetching latest file: {e}")
        return None

# Function to read CSV file from S3
def read_csv_from_s3(bucket_name: str, file_key: str):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = pd.read_csv(io.BytesIO(response["Body"].read()))
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

# Function to preprocess movie tags
def stem(text):
    ps = PorterStemmer()
    return " ".join(ps.stem(word) for word in text.split())

def lambda_handler(event, context):
    try:
        preprocessed_movies_file = get_latest_file(SOURCE_BUCKET, "Preprocessed")
        if not preprocessed_movies_file:
            return {"statusCode": 400, "body": "No file found in source bucket."}
        
        movies = read_csv_from_s3(SOURCE_BUCKET, preprocessed_movies_file)
        if movies is None:
            return {"statusCode": 500, "body": "Error reading CSV file."}
        
        # Apply stemming
        movies['tags'] = movies['tags'].apply(stem)
        
        # Create feature vectors
        cv = CountVectorizer(max_features=5000, stop_words='english')
        vectors = cv.fit_transform(movies['tags']).toarray()
        similarity = cosine_similarity(vectors)
        
        # Generate filenames with timestamps
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        movies_file_name = f"movies_{timestamp}.pkl"
        similarity_file_name = f"similarity_{timestamp}.pkl"
        
        # Upload processed data to S3
        for data, folder, filename in [(movies, MOVIES_FOLDER, movies_file_name), (similarity, SIMILARITY_FOLDER, similarity_file_name)]:
            buffer = io.BytesIO()
            pickle.dump(data, buffer)
            buffer.seek(0)
            s3_client.put_object(Bucket=DEST_BUCKET, Key=f"{folder}/{filename}", Body=buffer.getvalue())
        
        return {"statusCode": 200, "body": "Files processed and uploaded successfully."}
    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {"statusCode": 500, "body": "Internal server error."}
