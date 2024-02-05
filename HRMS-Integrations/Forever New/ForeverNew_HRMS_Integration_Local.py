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
    external_ip = "103.173.93.237"
    # external_ip = "111.223.1.99"
    print(external_ip)

# FILE PROPERTIES
DELIMITER = "|"
HAPPAY_API_TOKEN = "Bearer ECrgMpmGp8nmFs215CMuedl0W"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": HAPPAY_API_TOKEN,
    "content-type": "application/json",
}

file_path = "D:/Animesh/HAPPAY/.csv"

def create_payload_from_row(row):
    payload = {
        "firstName": row[0],
        "middleName": row[1],
        "lastName": row[2],
        "userId": row[6],
        "requestId": row[6],
        "gender": row[8],
        "title": row[9],
        "mobile_extension": "+91",
        "emailId": row[11],
        "mobileNo": row[12],
        "dob": row[13],
        "metaFields": {
            "Employee_Code": row[6],
            # "Bank Account Number": row[],
            # "IFSC": row[],
            "Employee_Id": row[6],
            # "State": row[],
            # "City": row[],
            # "Department": row[],
            # "Department_Code": row[],
            # "Location_Code": row[],
            # "Store_Name": row[],
            # "Store_Code": row[],
            # "Store_Group": row[],
            "Grade": row[3],
            "Designation": row[4],
            "Employee_Type": row[5],
            # "Store_Expense_Limit": row[]
        },
        "supervisors": list(),
    }
    payload["supervisors"].append(
        {"supervisorId": "EMP_001", "roleName": "super admin"}
    )
    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "Regional Manager",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "CBO",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "HOD",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "SVP-Finance",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "VP Operations",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "Store Manager",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "Reporting Manager",
    #     "supervisorId": row []
    #     })
    # if(row []):
    #     payload["supervisors"].append({
    #     "roleName": "finance",
    #     "supervisorId": row []
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
