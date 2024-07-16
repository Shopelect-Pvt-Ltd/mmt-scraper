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
import sys

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

DAY_IN_MILLISECONDS = 86400000


def getTransactionData(db, startepoch, endepoch, client_data_document, booking_type):
    try:
        collection_data = db['mmt_data']
        client_id = client_data_document['expense_client_id']
        org_id = client_data_document['external_org_id']
        logging.info(f"Processing client {client_id} with org {org_id} for {booking_type}")
        conn = http.client.HTTPSConnection(HOST_URL, context=context)

        if booking_type == "FLIGHT":
            reporttype = "FLIGHT"

        payload = {
            "expense-client-id": client_id,
            "external-org-id": org_id,
            "from-date": str(startepoch),
            "to-date": str(endepoch),
            "report-type": reporttype,
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
                            booking_map[bookingId].add(booking["Customer GSTN"])
                        else:
                            booking_map[bookingId] = set([booking["Customer GSTN"]])

                for bookingId, customer_gstin in booking_map.items():
                    key_to_check = {"bookingId": bookingId}
                    exiting_data = collection_data.find_one(key_to_check)
                    if exiting_data  is not None:
                        logging.info("Updating for the bookingId: "+str(bookingId))
                        booking_data=exiting_data["booking_data"]
                        newbooking_data=[]
                        for i in range(len(booking_data)):
                            booking_obj=booking_data[i]
                            booking_obj["Customer GSTN"]=list(customer_gstin)[0]
                            newbooking_data.append(booking_obj)

                        if len(newbooking_data)!=0:
                            collection_data.update_one(
                                key_to_check,
                                {
                                    "$set": {
                                        "booking_data": booking_data
                                    }
                                })
                break
            time.sleep(2)
    except Exception as e:
        logging.info("Exception happened in the getTransactionData: " + str(e))


if __name__ == '__main__':
    try:
        # endepoch = int(int(datetime.now().replace(microsecond=0, second=0, minute=0, hour=0).strftime('%s')) - 1 * 19800) * 1000
        # startepoch = endepoch - 86400000
        if len(sys.argv) < 3:
            logging.info("Invalid argument..")
            exit(0)
        tstarttime = int(sys.argv[1])
        tendtime = tstarttime + DAY_IN_MILLISECONDS
        scriptendtime = int(sys.argv[2])
        nd = 1
        while (tendtime <= scriptendtime):
            logging.info("========================================================")
            logging.info("Start Time: " + str(tstarttime) + " End Time: " + str(tendtime))
            # MongoDB connection setup
            db = client['MakeMyTrip']
            client_data_collection = db['Client_ID']
            # Get all the documents in the 'Client_ID' collection
            client_data = list(client_data_collection.find())
            # Processing each client
            for client_data_document in client_data:
                logging.info("-----------------------------------------------------------")
                logging.info("Processing For Org Name: " + str(client_data_document["org_name"]))
                logging.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                logging.info("Processing For Flight")
                getTransactionData(db, tstarttime, tendtime, client_data_document, "FLIGHT")
            logging.info("========================================================")
            tstarttime = tstarttime + DAY_IN_MILLISECONDS
            tendtime = tendtime + DAY_IN_MILLISECONDS
            logging.info("No. of days completed: " + str(nd))
            nd = nd + 1
    except Exception as e:
        logging.info("Exception happened in the main: " + str(e))


