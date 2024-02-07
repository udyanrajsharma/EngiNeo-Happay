import json
import json
import boto3
import requests
import random
from datetime import datetime
import urllib
import csv
import paramiko
import pandas
import uuid

def get_system_ip():
    import urllib.request
    # external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    external_ip = "103.173.93.237"
    print(external_ip)


#FILE PROPERTIES
DELIMITER = '|'

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_dropdown_values/"

request_headers = {
    "authorization": "Bearer bjWYFMBE0ftgd1m2upFcaolxY",
    "content-type": "application/json",
}

file_path = "D:/Animesh/HAPPAY/Integration/Suzuki Motor Gujarat/CSV Template/Master Data - Dropdown/SMG_HR_Master_Data_Test.csv"


def create_payload_from_row(row):
    if row[3] == '':
        payload = {
            "dd_values": [row[2]],
            "field_type": "User", 
            "ef_name": row[1], 
            "requestId": row[0],
        }
    else :
        payload = {
            "requestId": row[0], 
            "field_type": "User", 
            "ef_name": row[1],  
            "parent_name": row[3],  
            "dd_values": { 
                row[4]: [row[2]]
            }
        }
    return payload

process_id = uuid.uuid4().hex
get_system_ip()
try:
    error_rows = []
    file = open(file_path)
    lines = csv.reader(file, delimiter=DELIMITER)
    headers = next(lines) #If file has headers; repeat for as many times as number of headers

    for row_count, row in enumerate(lines):
        try:
            payload = create_payload_from_row(row)
            jsonPayload = json.dumps(payload)
            print("###### Invoking Happay API ######")
            print("Request Payload => ", jsonPayload)
            response = requests.post(url=api_url, headers=request_headers, json=payload)
            if response.status_code != 200:
                print(
                    "/n/nFailed record Field Value = ",
                    payload.get("dd_values"),
                    "; Field Name = ",
                    payload.get("ef_name"),
                    "; error = ",
                    response.text,
                )
            else:
                print("/n/nSuccessfull API invocation")
            print("###### Happay API Invocation ended ######/n/n")
           
        except Exception as e:
            print (str(e))
            error_rows.append({'row_index' : row_count+1, 'error' : str(e)})

    print (str(error_rows))
    # return True
except Exception as e:
        # print(e)
        # print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.')
    raise e