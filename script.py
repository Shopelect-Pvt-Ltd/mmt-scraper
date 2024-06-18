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

cursor = conn.cursor()
logging.info("Postgres DB connected successfully.")

client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

HOST_URL = "corpcb.makemytrip.com"
context = ssl._create_unverified_context()

fields_to_clean = ['PNR No(s)', 'Ticket No(s)']


def clean_data(data, fields_to_clean):
    prefix_pnr = "PNR No(s) - "
    prefix_ticket = "Ticket No(s) - "
    for field in fields_to_clean:
        if field in data and isinstance(data[field], str):
            data[field] = data[field].replace(prefix_pnr, "").replace(prefix_ticket, "").strip()
        else:
            logging.error(f"Field {field} is missing or not a string in data: {data.get(field)}")
    return data


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
        insertData(booking_id, file_hash, booking_type, file_type, service_provider, s3_url)
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

def getTransactionData(db, startepoch, endepoch, client_data_document, booking_type):
    try:
        collection_data = db['mmt_data_test']
        client_id = client_data_document['expense_client_id']
        org_id = client_data_document['external_org_id']
        logging.info(f"Processing client {client_id} with org {org_id} for {booking_type}")
        conn = http.client.HTTPSConnection(HOST_URL, context=context)
        payload = {
            "expense-client-id": client_id,
            "external-org-id": org_id,
            "from-date": str(startepoch),
            "to-date": str(endepoch),
            "report-type": booking_type,
            "level": "INVOICE"
        }
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/csv',
            'Cookie': '_abck=17E55C4AA211B67E2013B8DAC748B665~-1~YAAQmKIauIuvyfGOAQAAABH7CgvADy+xDRdL180XIaiJ3irmnnBSM+RMIwv5tkxrggRXsYaCFbxY8x1MmaHVyogQy3PgqByI6VhzRrVFc/CO6bVpvvz9eEUnu8HSpJ2/6IEYcO3szuvfu5Rie/3l2ncHljNPodObiY/v/O5T1L6DqjHiprPE1XAYxMO4bcbWFY5bdHd/5lt7o1M4NYxPdDAHZ0AzgxBokaEPlKqNUOwcrdbz9yGBdKpEuWqQ5dL3kSNQUmdkJMCrR1zj9yEaMd+q1E+fJxS18J/IDquf26vlOcrbY7Rn7z8wBcwvuAOh9tBW2875OypRY9Q1tChLDA7UHw95ggCJ/6H2XxBEZI3Pn3UVyUGJWfS1j2lveih5mWezKAiexsctMR4=~-1~-1~-1; bm_sz=6D14478164410E7C7B7A8B4A2D9159E5~YAAQmKIauIyvyfGOAQAAABH7ChcmVrqMEgi4J1TS4LA7oDvhVP8acrCwLRC2SY+zqfIL77pjl+JBfZ1Wn3aTPAY11YY6bSNry88ixL90LlqPK+zc368emSSnH0Wc7iRnNAY8x5nf6IUQJTZipbluHJX/P9sbHZB8evG8cw5SvdZiKYrmS0xkQYJCnbPqXSLt3t1HvZ3THrjYXWTEM/P24SMfVgv2kq0KXuwXSaOAQJ1sd++QQeymTa3NpcWSzI9K25/CA2uQWzGN5aqm0q/ad/Q1sl+iKweDTJUhQzvBDRi/DPenESz12FUo87xaR2Ds+Mh61y6JUyT8bDm/qb6LVWib0jet886lQz96mTY8TE71~3486008~3556163'

        }
        for i in range(1, 3, 1):
            conn.request("POST", "/transaction/data", json.dumps(payload), headers)
            res = conn.getresponse()
            if res.status == 200:
                data = res.read().decode("utf-8")
                csv_reader = csv.DictReader(StringIO(data))
                booking_data = list(csv_reader)
                booking_map = {}
                for booking in booking_data:
                    if 'Booking ID' in booking:
                        bookingId = booking['Booking ID']
                        if bookingId in booking_map:
                            booking_map[bookingId].append(booking)
                        else:
                            booking_map[bookingId] = [booking]

                for bookingId, booking_data in booking_map.items():
                    if booking_data is not None and len(booking_data)!=0:
                        logging.info("BookingId: " + str(bookingId))
                        logging.info("Booking Details: " + str(booking_data))
                        tempdoc = {}
                        if booking_type == "FLIGHT":
                            temp_booking_data=[]
                            for i in range(len(booking_data)):
                                clean_booking_data = clean_data(booking_data[i], fields_to_clean)
                                temp_booking_data.append(clean_booking_data)
                            booking_data=temp_booking_data

                        service_provider = None
                        if booking_type == "FLIGHT":
                            service_provider = booking_data[0]['Airline Name(s)']
                        elif booking_type == "HOTEL":
                            service_provider = booking_data[0]['Hotel Name']

                        invoicedata=None
                        if service_provider is not None:
                            invoicedata = getInvoiceData(client_id, org_id, bookingId, service_provider, booking_type)

                        key_to_check = {"bookingId": bookingId}
                        if collection_data.find_one(key_to_check) is None:
                            if '_id' in client_data_document:
                                del client_data_document['_id']
                            tempdoc.update(client_data_document)
                            tempdoc["bookingId"] = bookingId
                            tempdoc["booking_data"] = booking_data
                            tempdoc["invoice_data"] = invoicedata
                            tempdoc["booking_type"] = booking_type
                            collection_data.insert_one(tempdoc)
                        else:
                            result=collection_data.update_one(
                                key_to_check,
                                {
                                    "$set": {
                                        "booking_data": booking_data,
                                        "invoice_data": invoicedata
                                    }
                                })
                            if result.matched_count > 0:
                                logging.info("Updated the document for bookingId: "+str(bookingId))
                            else:
                                logging.info("No updates for the bookingId: "+str(bookingId))
                break
            time.sleep(2)
    except Exception as e:
        logging.info("Exception happened in the getTransactionData: " + str(e))


if __name__ == '__main__':
    try:
        endepoch = int(
            int(datetime.now().replace(microsecond=0, second=0, minute=0, hour=0).strftime('%s')) - 1 * 19800) * 1000
        startepoch = endepoch - 86400000

        logging.info("========================================================")
        logging.info("Start Time: " + str(startepoch) + " End Time: " + str(endepoch))
        # MongoDB connection setup
        db = client['MakeMyTrip']
        client_data_collection = db['Client_ID']
        # Get all the documents in the 'Client_ID' collection
        client_data = list(client_data_collection.find())
        # Processing each client
        for client_data_document in client_data:
            if client_data_document["expense_client_id"] == 'ed019308-6cc6-4f26-9e61-6b4941715819' and \
                    client_data_document['external_org_id'] == '35c22a35-6354-4cca-a0a3-3790a623af53':
                logging.info("-----------------------------------------------------------")
                logging.info("Processing For Org Name: " + str(client_data_document["org_name"]))
                logging.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                logging.info("Processing For Flight")
                getTransactionData(db, startepoch, endepoch, client_data_document, "FLIGHT")
                logging.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                logging.info("Processing For Hotel")
                getTransactionData(db, startepoch, endepoch, client_data_document, "HOTEL")
                logging.info("-----------------------------------------------------------")
        logging.info("========================================================")
    except Exception as e:
        logging.info("Exception happened in the main: " + str(e))
    finally:
        logging.info("Closing the DB connection")
        cursor.close()
        conn.close()
