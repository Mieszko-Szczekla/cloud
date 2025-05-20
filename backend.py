import requests
import boto3
from flask import Flask, request, jsonify
import mysql.connector
import os

rekordy = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "localhost").split(":")[0],
    port=5432,
    user='backend',
    password=os.getenv("MYSQL_PASSWORD", "password"),
    database='records'
)

rekordy.cursor().execute("CREATE TABLE IF NOT EXISTS rekordy (id INT AUTO_INCREMENT PRIMARY KEY, record VARCHAR(255))")

bucket_name = os.getenv("BUCKET_NAME", "rekordy-bucket")
s3_client = boto3.client("s3",
    aws_access_key_id=os.getenv("AK_ID", ""), 
    aws_secret_access_key=os.getenv("AK_SECRET", ""), 
    region_name=os.getenv("REGION", "us-east-1"),
    aws_session_token=os.getenv("SESSION_TOKEN", "")
)

app = Flask(__name__)


@app.route("/upload_url", methods=["GET"])
def generate_upload():
    filename = request.args.get("filename")
    url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": bucket_name, 
                "Key": filename},
            ExpiresIn=3000
        )
    response = jsonify({"url": url})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/download_url", methods=["GET"])
def download_url():
    download_details = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket_name,
            "Key": request.args.get("filename")
        },
        ExpiresIn=3000
    )
    response = jsonify({"url": download_details})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/add_record", methods=["GET"])
def add_record():
    record = request.args.get("record")
    if record:
        with rekordy.cursor() as cur:
            cur.execute("INSERT INTO rekordy (record) VALUES (%s)", (record,))
            rekordy.commit()
        response = jsonify({"message": "Record added successfully"})
    else:
        response = jsonify({"error": "No record provided"}), 400
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/get_records", methods=["GET"])
def get_records():
    with rekordy.cursor() as cur:
        cur.execute("SELECT * FROM rekordy")
        rekordy_records = cur.fetchall()
        response = jsonify(rekordy_records)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)