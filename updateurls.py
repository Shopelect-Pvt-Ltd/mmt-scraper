import http.client
import json
import ssl
import requests
from pymongo import MongoClient
import logging
from io import StringIO
import csv
from datetime import datetime
import os
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()
from urllib import parse
from urllib.parse import urlparse
import time
import psycopg2
import hashlib

# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')

aws_access_key_id = os.getenv('AWS_ACCESS')
aws_secret_access_key = os.getenv('AWS_SECRET')
bucket_name = os.getenv('DEST_AWS_BUCKET_NAME')

postgres_host = os.getenv("PG_HOST")
postgres_db = os.getenv("PG_DATABASE")
postgres_user = os.getenv("PG_USER")
postgres_password = os.getenv("PG_PASSWORD")
postgres_port = os.getenv("PG_PORT")

conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_db,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password
)

client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

HOST_URL = "corpcb.makemytrip.com"
context = ssl._create_unverified_context()

fields_to_clean = ['PNR No(s)', 'Ticket No(s)']


def getFileNameFromURL(url):
    parsed_url = urlparse(url)
    # Extract the path component
    path = parsed_url.path
    # Get the file name
    file_name = path.split('/')[-1]
    return file_name


def findFileHash(file_path, hash_algo='sha256'):
    hash_func = hashlib.new(hash_algo)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def downloadFile(filename, url):
    try:
        for i in range(1, 3, 1):
            response = requests.get(url)
            if response.status_code == 200:
                local_file_name = "temp/" + filename
                with open(local_file_name, 'wb') as file:
                    file.write(response.content)
                filehash = findFileHash(local_file_name)
                return local_file_name, filehash
            time.sleep(2)
        return None, None
    except Exception as e:
        logging.info("Exception happened in the downloadFile: " + str(e))
        return None, None
def insertData(booking_id, file_hash, booking_type, file_type, service_provider, s3_link):
    try:
        insert_query = """
                INSERT INTO mmt_airline_hotel_booking (booking_id,file_hash,booking_type,file_type,service_provider,status,s3_link)
                VALUES (%s, %s, %s,%s, %s, %s,%s) ON CONFLICT DO NOTHING
                """
        data_to_insert = (booking_id, file_hash, booking_type, file_type, service_provider, "PENDING", s3_link)
        with conn.cursor() as cursor:
            cursor.execute(insert_query, data_to_insert)
            conn.commit()
        logging.info(
            "Data inserted successfully for (booking_id,file_hash,booking_type,file_type,service_provider,status,s3_link): " + str(
                data_to_insert))
        return True
    except Exception as e:
        logging.info("Exception happened in the insertData: " + str(e))
        return False
def getS3Url(url, booking_id, booking_type, file_type, service_provider):
    try:
        s3_file = getFileNameFromURL(url)
        if s3_file == None:
            return None

        local_file, file_hash = downloadFile(s3_file, url)
        if local_file is None:
            return None

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
        object = f"v0/MMTScraping/{booking_type}/{file_type}/{s3_file}"
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{object}"
        s3.upload_file(local_file,
                       bucket_name,
                       Key=object
                       )
        # insertData(booking_id, file_hash, booking_type, file_type, service_provider, s3_url)
        # logging.info("S3 Url is generated successfully: " + str(s3_url))
        if os.path.exists(local_file):
            os.remove(local_file)
        return s3_url
    except Exception as e:
        logging.info("Exception happened in the getS3Url: " + str(e))
        return None

def processResponse(resp, booking_id, booking_type, service_provider):
    try:
        if "invoiceData" in resp:
            for i in range(len(resp["invoiceData"])):
                invoicedata = resp["invoiceData"][i]
                if "invoiceTypeWiseData" in invoicedata:
                    if "MMT" in invoicedata["invoiceTypeWiseData"]:
                        for j in range(len(invoicedata["invoiceTypeWiseData"]["MMT"])):
                            if "invoiceUrl" in invoicedata["invoiceTypeWiseData"]["MMT"][j]:
                                newS3url = getS3Url(invoicedata["invoiceTypeWiseData"]["MMT"][j]["invoiceUrl"],
                                                    booking_id,
                                                    booking_type, "MMT", service_provider)
                                if newS3url is not None:
                                    invoicedata["invoiceTypeWiseData"]["MMT"][j]["invoiceUrl"] = newS3url

                    if "eVOUCHER" in invoicedata["invoiceTypeWiseData"]:
                        for j in range(len(invoicedata["invoiceTypeWiseData"]["eVOUCHER"])):
                            if "invoiceUrl" in invoicedata["invoiceTypeWiseData"]["eVOUCHER"][j]:
                                newS3url = getS3Url(invoicedata["invoiceTypeWiseData"]["eVOUCHER"][j]["invoiceUrl"],
                                                    booking_id, booking_type, "eVOUCHER", service_provider)
                                if newS3url is not None:
                                    invoicedata["invoiceTypeWiseData"]["eVOUCHER"][j]["invoiceUrl"] = newS3url

                    if "GST" in invoicedata["invoiceTypeWiseData"]:
                        for j in range(len(invoicedata["invoiceTypeWiseData"]["GST"])):
                            if "invoiceUrl" in invoicedata["invoiceTypeWiseData"]["GST"][j]:
                                newS3url = getS3Url(invoicedata["invoiceTypeWiseData"]["GST"][j]["invoiceUrl"],
                                                    booking_id,
                                                    booking_type, "GST", service_provider)
                                if newS3url is not None:
                                    invoicedata["invoiceTypeWiseData"]["GST"][j]["invoiceUrl"] = newS3url
        return resp
    except Exception as e:
        logging.info("Exception happened in the processResponse: " + str(e))

def getInvoiceData(client_id, org_id, booking_id, service_provider, booking_type):
    try:
        conn = http.client.HTTPSConnection(HOST_URL, context=context)
        payload = json.dumps({
            "expense-client-id": client_id,
            "external-org-id": org_id,
            "booking-id": booking_id
        })
        headers = {
            'Content-Type': 'application/json',
            'Cookie': '_abck=17E55C4AA211B67E2013B8DAC748B665~-1~YAAQmKIauIuvyfGOAQAAABH7CgvADy+xDRdL180XIaiJ3irmnnBSM+RMIwv5tkxrggRXsYaCFbxY8x1MmaHVyogQy3PgqByI6VhzRrVFc/CO6bVpvvz9eEUnu8HSpJ2/6IEYcO3szuvfu5Rie/3l2ncHljNPodObiY/v/O5T1L6DqjHiprPE1XAYxMO4bcbWFY5bdHd/5lt7o1M4NYxPdDAHZ0AzgxBokaEPlKqNUOwcrdbz9yGBdKpEuWqQ5dL3kSNQUmdkJMCrR1zj9yEaMd+q1E+fJxS18J/IDquf26vlOcrbY7Rn7z8wBcwvuAOh9tBW2875OypRY9Q1tChLDA7UHw95ggCJ/6H2XxBEZI3Pn3UVyUGJWfS1j2lveih5mWezKAiexsctMR4=~-1~-1~-1; bm_sz=6D14478164410E7C7B7A8B4A2D9159E5~YAAQmKIauIyvyfGOAQAAABH7ChcmVrqMEgi4J1TS4LA7oDvhVP8acrCwLRC2SY+zqfIL77pjl+JBfZ1Wn3aTPAY11YY6bSNry88ixL90LlqPK+zc368emSSnH0Wc7iRnNAY8x5nf6IUQJTZipbluHJX/P9sbHZB8evG8cw5SvdZiKYrmS0xkQYJCnbPqXSLt3t1HvZ3THrjYXWTEM/P24SMfVgv2kq0KXuwXSaOAQJ1sd++QQeymTa3NpcWSzI9K25/CA2uQWzGN5aqm0q/ad/Q1sl+iKweDTJUhQzvBDRi/DPenESz12FUo87xaR2Ds+Mh61y6JUyT8bDm/qb6LVWib0jet886lQz96mTY8TE71~3486008~3556163'
        }
        for i in range(1, 3, 1):
            conn.request("POST", "/external/invoice/data", payload, headers)
            res = conn.getresponse()
            if res.status == 200:
                data = res.read().decode("utf-8")
                newresp = processResponse(json.loads(data), booking_id, booking_type, service_provider)
                if "invoiceData" in newresp:
                    return newresp["invoiceData"]
                break
            time.sleep(2)
        return {}
    except Exception as e:
        logging.info("Exception happened in the getInvoiceData: " + str(e))
        return {}



def updateS3Url(booking_data):
    for booking in booking_data:
        client_id=booking["expense_client_id"]
        org_id=booking["external_org_id"]
        booking_id=booking["bookingId"]
        booking_type=booking["booking_type"]
        service_provider = None
        service_provider_arr = []
        for i in range(len(booking["booking_data"])):
            if booking_type == "FLIGHT":
                service_provider_arr.append(booking["booking_data"][i]['Airline Name'])
            elif booking_type == "HOTEL":
                service_provider_arr.append(booking["booking_data"][i]['Hotel Name'])
        service_provider_arr = list(set(service_provider_arr))
        delimiter = ';'
        service_provider = delimiter.join(str(item) for item in service_provider_arr)

        invoicedata=getInvoiceData(client_id, org_id, booking_id, service_provider, booking_type)
        print(invoicedata)


if __name__ == '__main__':
    try:

        logging.info("========================================================")

        # MongoDB connection setup
        db = client['MakeMyTrip']
        booking_data_collection = db['mmt_data_test']
        # Get all the documents in the 'Client_ID' collection
        booking_data = list(booking_data_collection.find({"bookingId":'NH93044306134378'}))
        updateS3Url(booking_data)
        logging.info(booking_data)
        logging.info("========================================================")
    except Exception as e:
        logging.info("Exception happened in the main: " + str(e))



