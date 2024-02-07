import json
import json
# import boto3
import requests
# import random
from datetime import datetime
# import urllib
import csv
# import paramiko
# import pandas
import uuid


def get_system_ip():
    import urllib.request

    # external_ip = urllib.request.urlopen("https://ident.me").read().decode("utf8")
    # external_ip = "103.173.93.237"
    external_ip = "223.190.87.254"
    
    
    print(external_ip)

# FILE PROPERTIES
DELIMITER = "|"
HAPPAY_API_TOKEN = "Bearer 5HWfCR2xOVUeCSGHDB1uoAEne"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": HAPPAY_API_TOKEN,
    "content-type": "application/json",
}

file_path = "D:/EngiNeo/Demo Integration/First Integration file.csv"



def create_payload_from_row(row):
    payload = {
        "emailId": row[0],
        "firstName": row[1],
        "title": row[2],
        "middleName": row[3],
        "dob": row[4],
        "gender": row[5],
        "password": "Smg@2024",
        "mobileNo": row[6],
        "mobile_extension": "+91",
        "userId": row[8],
        "requestId": row[8],
        "lastName": row[7],
        "metaFields": {
            "Employee_Code": row[8],
            "Dt_Designation": row[9],
            "Dt_Grades": row[10],
            "City": row[11],
            "Usecase_Entity": row[12],
        },
        "supervisors": list(),
    }
    payload ["supervisors"].append(
        {
            "supervisorId": "1",
            "roleName":"super admin"
        }
    )
    # if(row [45]):
    #     payload["supervisors"].append({
    #     "roleName": "DPM",
    #     "supervisorId": row [45]
    #     })

    return payload


process_id = uuid.uuid4().hex
get_system_ip()
try:
    error_rows = []
    file = open(file_path)
    lines = csv.reader(file, delimiter=DELIMITER)
    headers = next(
        lines
    )  # If file has headers; repeat for as many times as number of headers

    for row_count, row in enumerate(lines):
        try:
            payload = create_payload_from_row(row)
            jsonPayload = json.dumps(payload)
            print("###### Invoking Happay API ######")
            print("Request Payload => ", jsonPayload)
            response = requests.post(url=api_url, headers=request_headers, json=payload)
            if response.status_code != 200:
                print(
                    "\n\nFailed record emailId = ",
                    payload.get("emailId"),
                    "; userId = ",
                    payload.get("userId"),
                    "; error = ",
                    response.text,
                )
            else:
                print("\n\nSuccessfull API invocation")
            print("###### Happay API Invocation ended ######\n\n")
        except Exception as e:
            print(str(e))
            error_rows.append({"row_index": row_count + 1, "error": str(e)})

    print(str(error_rows))
except Exception as e:
    raise e
