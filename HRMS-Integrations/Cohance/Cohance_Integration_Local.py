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
    external_ip = urllib.request.urlopen("https://ident.me").read().decode("utf8")
    print(external_ip)


# FILE PROPERTIES
DELIMITER = "|"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": "Bearer kHmTlijheltnbfhZMA0sKEVDw",
    "content-type": "application/json",
}

file_path = "D:/Animesh/Integration/Cohance/UserData/Cohance_User_Data_11_09_23.csv"  

def create_payload_from_row(row):
    payload = {
        "emailId": row[13],
        "firstName": row[0],
        "title": row[11],
        "middleName": row[1],
        "dob": row[15],
        "gender": row[10],
        "mobileNo": row[14],
        "mobile_extension": "+91",
        "password": "Cohance@2023",
        "userId": row[6],
        "requestId": row[6],
        "lastName": row[2],
        "metaFields": {
            "Employee_Code": row[6],
            "ENTITY_NAME": row[7],
            # "BUSINESS_UNIT": row[9],
            "DEPT_NAME": row[3],
            # "COST_CENTER": row[3],
            # "COST_CENTER_CODE": row[4],
            "LEVEL": row[5],
            "DESIGNATION": row[4]
        },
        "supervisors": list(),
    }
    payload["supervisors"].append(
        {"supervisorId": "EMP_001", "roleName": "super admin"}
    )

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user. 
    # if(row [17]):
    #     payload["supervisors"].append({
    #     "roleName": "Reporting Manager",
    #     "supervisorId": row [17]
    #     })
    # if(row [19]):
    #     payload["supervisors"].append({
    #     "roleName": "Head Of Department",
    #     "supervisorId": row [19]
    #     })
    # if(row [20]):
    #     payload["supervisors"].append({
    #     "roleName": "BU Head",
    #     "supervisorId": row [20]
    #     })
    # if(row [22]):
    #     payload["supervisors"].append({
    #     "roleName": "CHRO",
    #     "supervisorId": row [22]
    #     })
    # if(row [24]):
    #     payload["supervisors"].append({
    #     "roleName": "CEO",
    #     "supervisorId": row [24]
    #     })
    # if(row [26]):
    #     payload["supervisors"].append({
    #     "roleName": "Finance Team",
    #     "supervisorId": row [26]
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




