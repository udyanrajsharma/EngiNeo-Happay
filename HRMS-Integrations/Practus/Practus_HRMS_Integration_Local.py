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
    
    print(external_ip)

# FILE PROPERTIES
DELIMITER = "|"
HAPPAY_API_TOKEN = "Bearer ozfWlnuAnGKReN2F46VqZnl2C"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": HAPPAY_API_TOKEN,
    "content-type": "application/json",
}

file_path = "D:/Animesh/HAPPAY/Integration/Practus/DATA/Employee_HRMS_Data.csv"



def create_payload_from_row(row):
    payload = {
        "emailId": row[0],
        "firstName": row[2],
        "title": row[1],
        "middleName": row[3],
        "dob": row[5],
        "gender": row[6],
        "password": "Practus@2023",
        "mobileNo": row[7],
        "mobile_extension": "+91",
        "userId": row[8],
        "requestId": row[8],
        "lastName": row[4],
        "metaFields": {
            "Employee_Code": row[8],
            "Company_Name": row[9],
            "Company_Code": row[10],
            "Department_Name": row[11],
            "Delivery_Or_Non_Delivery": row[12],
            "Grade": row[13],
            "Direct_Indirect_Hidden": row[14],
            "Designation": row[15],
            "Project_Role": row[16],
            "Employee_Type": row[17],
            "Bank_Name": row[18],
            "Mode_Of_Transfer": row[19],
            "Mode_Of_Transfer_Code": row[20],
            "Ifsc_Code": row[21],
            "Bank_Account_Number": row[22],
            "Bank_Branch_Name": row[23],
            "State_Gstin_Hidden": row[25],
            "Last_Working_Day": row[24],
        },
        "supervisors": list(),
    }

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    
    if(row [26]):
        payload["supervisors"].append({
        "roleName": "Expense Approver",
        "supervisorId": row [26]
        })
    if(row [27]):
        payload["supervisors"].append({
        "roleName": "finance",
        "supervisorId": row [27]
        })
    if(row [28]):
        payload["supervisors"].append({
        "roleName": "US Delivery Head",
        "supervisorId": row [28]
        })
    if(row [29]):
        payload["supervisors"].append({
        "roleName": "India Delivery Head",
        "supervisorId": row [29]
        })
    if(row [30]):
        payload["supervisors"].append({
        "roleName": "Director",
        "supervisorId": row [30]
        })
    if(row [31]):
        payload["supervisors"].append({
        "roleName": "CFO",
        "supervisorId": row [31]
        })
   

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
