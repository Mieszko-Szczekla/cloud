import requests
import boto3
from flask import Flask, request, jsonify
import mysql.connector
import os

rekordy = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "localhost").split(":")[0],
    port=5432,
    user='postgres',
    password='password',
    database='records'
)

bucket_name = "avn29rbyczr9b3vz9wbycri3oivbsieuybc3cwi"
s3_client = boto3.client("s3",
    aws_access_key_id=os.getenv("AK_ID", ""), 
    aws_secret_access_key=os.getenv("AK_SECRET", ""), 
    region_name=os.getenv("REGION", "us-east-1"),
    session_token=os.getenv("SESSION_TOKEN", "")
)

app = Flask(__name__)


@app.route("/upload_url", methods=["GET"])
def upload_url():
    upload_details = s3_client.generate_presigned_post(
        bucket_name,
        request.args.get("filename")
    )
    url_trail = "?"
    for k in upload_details["fields"]:
        url_trail += f"{k}={upload_details['fields'][k]}&"
    url_trail = url_trail[:-1]  # Remove the last '&'
    return upload_details

@app.route("/upload_message", methods=["POST"])
def upload_message():
    filename = request.args.get("filename")
    message = request.data.decode("utf-8")
    res = requests.get("127.0.0.1:8080/upload_url?filename="+filename).json()
    url = res["url"]
    with open("temp", 'wb') as file_to_upload:
        file_to_upload.write(message.encode("utf-8"))
    with open("temp", 'rb') as file_to_upload:
        files = {'file': ("temp", file_to_upload)}
        upload_response = requests.post(
            res['url'], 
            data=res['fields'], 
            files=files
        )
    

@app.route("/download_url", methods=["GET"])
def download_url():
    download_details = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket_name,
            "Key": request.args.get("filename")
        },
        ExpiresIn=3600
    )
    return jsonify({"url": download_details})

@app.route("/add_record", methods=["GET"])
def add_record():
    record = request.args.get("record")
    if record:
        with rekordy.cursor() as cur:
            cur.execute("INSERT INTO rekordy (record) VALUES (%s)", (record,))
            rekordy.commit()
        return jsonify({"message": "Record added successfully"})
    else:
        return jsonify({"error": "No record provided"}), 400

@app.route("/get_records", methods=["GET"])
def get_records():
    with rekordy.cursor() as cur:
        cur.execute("SELECT * FROM rekordy")
        rekordy_records = cur.fetchall()
        return jsonify(rekordy_records)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)