import csv
import pandas as pd
from datetime import datetime
import pytz
import logging
import os
import ssl
import http.client
import json
import time
from io import StringIO
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')
logging.info(MONGO_URL)
HOST_URL = "corpcb.makemytrip.com"
context = ssl._create_unverified_context()
mongodbclient = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")


def getEpoch(date_string):
    # Convert the date string to a datetime object, ignoring the time in the input
    dt_object = datetime.strptime(date_string, "%H:%M %d-%b-%Y").replace(hour=0, minute=0, second=0)
    # Set the timezone to IST
    ist = pytz.timezone('Asia/Kolkata')
    dt_ist = ist.localize(dt_object)
    # Convert the datetime object to an epoch timestamp
    starttime = int(dt_ist.timestamp()) * 1000
    endtime = starttime + (24 * 60 * 60 * 1000)
    return starttime, endtime


def getTransactionCustomerData(startepoch, endepoch, client_id, org_id):
    conn = http.client.HTTPSConnection(HOST_URL, context=context)
    payload = {
        "expense-client-id": client_id,
        "external-org-id": org_id,
        "from-date": str(startepoch),
        "to-date": str(endepoch),
        "report-type": "FLIGHT",
        "level": "INVOICE"
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'text/csv',
        'Cookie': '_abck=17E55C4AA211B67E2013B8DAC748B665~-1~YAAQmKIauIuvyfGOAQAAABH7CgvADy+xDRdL180XIaiJ3irmnnBSM+RMIwv5tkxrggRXsYaCFbxY8x1MmaHVyogQy3PgqByI6VhzRrVFc/CO6bVpvvz9eEUnu8HSpJ2/6IEYcO3szuvfu5Rie/3l2ncHljNPodObiY/v/O5T1L6DqjHiprPE1XAYxMO4bcbWFY5bdHd/5lt7o1M4NYxPdDAHZ0AzgxBokaEPlKqNUOwcrdbz9yGBdKpEuWqQ5dL3kSNQUmdkJMCrR1zj9yEaMd+q1E+fJxS18J/IDquf26vlOcrbY7Rn7z8wBcwvuAOh9tBW2875OypRY9Q1tChLDA7UHw95ggCJ/6H2XxBEZI3Pn3UVyUGJWfS1j2lveih5mWezKAiexsctMR4=~-1~-1~-1; bm_sz=6D14478164410E7C7B7A8B4A2D9159E5~YAAQmKIauIyvyfGOAQAAABH7ChcmVrqMEgi4J1TS4LA7oDvhVP8acrCwLRC2SY+zqfIL77pjl+JBfZ1Wn3aTPAY11YY6bSNry88ixL90LlqPK+zc368emSSnH0Wc7iRnNAY8x5nf6IUQJTZipbluHJX/P9sbHZB8evG8cw5SvdZiKYrmS0xkQYJCnbPqXSLt3t1HvZ3THrjYXWTEM/P24SMfVgv2kq0KXuwXSaOAQJ1sd++QQeymTa3NpcWSzI9K25/CA2uQWzGN5aqm0q/ad/Q1sl+iKweDTJUhQzvBDRi/DPenESz12FUo87xaR2Ds+Mh61y6JUyT8bDm/qb6LVWib0jet886lQz96mTY8TE71~3486008~3556163'

    }
    booking_map = {}
    for i in range(1, 3, 1):
        conn.request("POST", "/transaction/data", json.dumps(payload), headers)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read().decode("utf-8")
            csv_reader = csv.DictReader(StringIO(data))
            booking_data = list(csv_reader)
            for booking in booking_data:
                if 'Booking ID' in booking:
                    bookingId = booking['Booking ID']
                    if bookingId in booking_map:
                        booking_map[bookingId].add(booking["Customer GSTN"])
                    else:
                        booking_map[bookingId] = set([booking["Customer GSTN"]])
        time.sleep(2)
    return booking_map


def getTransactionCustomerData(startepoch, endepoch, client_id, org_id):
    conn = http.client.HTTPSConnection(HOST_URL, context=context)
    payload = {
        "expense-client-id": client_id,
        "external-org-id": org_id,
        "from-date": str(startepoch),
        "to-date": str(endepoch),
        "report-type": "FLIGHT",
        "level": "INVOICE"
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'text/csv',
        'Cookie': '_abck=17E55C4AA211B67E2013B8DAC748B665~-1~YAAQmKIauIuvyfGOAQAAABH7CgvADy+xDRdL180XIaiJ3irmnnBSM+RMIwv5tkxrggRXsYaCFbxY8x1MmaHVyogQy3PgqByI6VhzRrVFc/CO6bVpvvz9eEUnu8HSpJ2/6IEYcO3szuvfu5Rie/3l2ncHljNPodObiY/v/O5T1L6DqjHiprPE1XAYxMO4bcbWFY5bdHd/5lt7o1M4NYxPdDAHZ0AzgxBokaEPlKqNUOwcrdbz9yGBdKpEuWqQ5dL3kSNQUmdkJMCrR1zj9yEaMd+q1E+fJxS18J/IDquf26vlOcrbY7Rn7z8wBcwvuAOh9tBW2875OypRY9Q1tChLDA7UHw95ggCJ/6H2XxBEZI3Pn3UVyUGJWfS1j2lveih5mWezKAiexsctMR4=~-1~-1~-1; bm_sz=6D14478164410E7C7B7A8B4A2D9159E5~YAAQmKIauIyvyfGOAQAAABH7ChcmVrqMEgi4J1TS4LA7oDvhVP8acrCwLRC2SY+zqfIL77pjl+JBfZ1Wn3aTPAY11YY6bSNry88ixL90LlqPK+zc368emSSnH0Wc7iRnNAY8x5nf6IUQJTZipbluHJX/P9sbHZB8evG8cw5SvdZiKYrmS0xkQYJCnbPqXSLt3t1HvZ3THrjYXWTEM/P24SMfVgv2kq0KXuwXSaOAQJ1sd++QQeymTa3NpcWSzI9K25/CA2uQWzGN5aqm0q/ad/Q1sl+iKweDTJUhQzvBDRi/DPenESz12FUo87xaR2Ds+Mh61y6JUyT8bDm/qb6LVWib0jet886lQz96mTY8TE71~3486008~3556163'

    }
    booking_map = {}
    for i in range(1, 3, 1):
        conn.request("POST", "/transaction/data", json.dumps(payload), headers)
        res = conn.getresponse()
        if res.status == 200:
            data = res.read().decode("utf-8")
            csv_reader = csv.DictReader(StringIO(data))
            booking_data = list(csv_reader)
            for booking in booking_data:
                if 'Booking ID' in booking:
                    bookingId = booking['Booking ID']
                    if bookingId in booking_map:
                        booking_map[bookingId].add(booking["Customer GSTN"])
                    else:
                        booking_map[bookingId] = set([booking["Customer GSTN"]])
        time.sleep(2)
    return booking_map


def updateBooking(collection_name, inputbookingId):
    try:
        db = mongodbclient['MakeMyTrip']
        mmt_data_collection = db[str(collection_name)]
        mmt_booking_data = list(mmt_data_collection.find({"bookingId": inputbookingId}))
        logging.info(mmt_booking_data)

        if mmt_booking_data is not None:
            created_at = mmt_booking_data[0]["booking_data"][0]["Created Date"]
            starttime, endtime = getEpoch(created_at)
            booking_type = mmt_booking_data[0]["booking_type"]
            expense_client_id = mmt_booking_data[0]["expense_client_id"]
            external_org_id = mmt_booking_data[0]["external_org_id"]
            customergstmap = dict()
            if booking_type == "FLIGHT":
                customergstmap = getTransactionCustomerData(starttime, endtime, expense_client_id, external_org_id)
                if collection_name == "mmt_data":
                    reporttype = "FLIGHT_PNR"
                elif collection_name == "mmt_data_test2":
                    reporttype = "FLIGHT_PAX_PNR"
            elif booking_type == "HOTEL":
                reporttype = "HOTEL"
            conn = http.client.HTTPSConnection(HOST_URL, context=context)
            payload = {
                "expense-client-id": expense_client_id,
                "external-org-id": external_org_id,
                "from-date": str(starttime),
                "to-date": str(endtime),
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
                                booking_map[bookingId].append(booking)
                            else:
                                booking_map[bookingId] = [booking]
                    logging.info("Customer GSTIN: "+str(customergstmap.get(booking_data[i]['Booking ID'])))
                    for bookingId, booking_data in booking_map.items():
                        if booking_data is not None and len(booking_data) != 0 and bookingId == inputbookingId:
                            logging.info("BookingId: " + str(bookingId))
                            logging.info("Booking Details: " + str(booking_data))
                            tempdoc = {}
                            for i in range(len(booking_data)):
                                if booking_type == "FLIGHT":
                                    if len(customergstmap) != 0:
                                        customergst = customergstmap.get(booking_data[i]['Booking ID'])
                                        if customergst is not None:
                                            booking_data[i]["Customer GSTN"] = list(customergst)[0]

                            key_to_check = {"bookingId": bookingId}
                            result = mmt_data_collection.update_one(
                                key_to_check,
                                {
                                    "$set": {
                                        "booking_data": booking_data,
                                    }
                                })
                            if result.matched_count > 0:
                                logging.info("Updated the document for bookingId: " + str(bookingId))
                            else:
                                logging.info("No updates for the bookingId: " + str(bookingId))
                    break
                time.sleep(2)
    except Exception as e:
        logging.info("Exception happened in the updateBooking: " + str(e))

if __name__ == '__main__':

    df = pd.read_csv('/Users/komalkantmillan/Downloads/Failed.csv')
    for index, row in df.iterrows():
        bookingId = row['Booking ID']
        logging.info("Processing for bookingId: "+str(bookingId))
        updateBooking( "mmt_data", bookingId)
        updateBooking( "mmt_data_test2", bookingId)

