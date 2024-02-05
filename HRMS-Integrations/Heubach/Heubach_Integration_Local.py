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
    "authorization": "Bearer GyXSkJknySQifxnTAjD8MKQ2s",
    "content-type": "application/json",
}

file_path = "D:/Animesh/Integration/Heubach/User_Data/Heubach_User_Data_10_10_2023.csv"  

def create_payload_from_row(row):
    payload = {
        "emailId": row[7],
        "firstName": row[1],
        "title": row[0],
        "middleName": row[2],
        "dob": row[4],
        "gender": row[5],
        "mobileNo": row[6],
        "mobile_extension": "+91",
        "password": "Bach@2023",
        "userId": row[15],
        "requestId": row[15],
        "lastName": row[3],
        "metaFields": {
            "Department": row[8],
            "Employee_Type": row[9],
            "Company_Code": row[10],
            "Business_Unit": row[13],
            "Cost_Center_Name": row[12],
            "Employee_Code": row[15],
            "Employee_Vendor_Code": row[16],
            "Grade": row[17],
            "Designation": row[18]
        },
        "supervisors": list(),
    }
    payload["supervisors"].append(
        {"supervisorId": "E99", "roleName": "super admin"}
    )

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user. 
    if(row [19]):
        payload["supervisors"].append({
        "roleName": "Line Manager",
        "supervisorId": row [19]
        })
    # if(row [21]):
    #     payload["supervisors"].append({
    #     "roleName": "finance",
    #     "supervisorId": row [21]
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




