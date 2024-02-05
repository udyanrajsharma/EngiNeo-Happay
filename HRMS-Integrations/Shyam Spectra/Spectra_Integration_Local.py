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

    # external_ip = urllib.request.urlopen("https://ident.me").read().decode("utf8")
    external_ip = "103.173.93.237"
    print(external_ip)


# FILE PROPERTIES
DELIMITER = "|"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": "Bearer MPiGPGLKAkf4jlt6pVIB50tjU",
    "content-type": "application/json",
}

file_path = "D:/Animesh/Integration/Shyam Spectra/SSPL Code/Empl_Master_24102023.csv"


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
        "password": "Spectra@2023",
        "userId": row[15],
        "requestId": row[15],
        "lastName": row[3],
        "metaFields": {
            "Entity": row[8],
            # This field is removed from Happay.
            # "Business_Unit": row[10],
            "Department": row[9],
            "Sub_Department": row[10],
            "Cost_Center": row[12],
            "Cost_Center_Code": row[11],
            "Cost_Category": row[13],
            "Last_Working_Day": row[14],
            "Employee_Code": row[15],
            "Employee_Id": row[15],
            "Employee_Grade": row[16],
            "Employee_Designation": row[17],
            "Date_Of_Joining": row[18],
            "Location": row[19],
            # Location_Code is dependent on location so no need to take value from csv file.
            # "Location_Code": row[20],
            "Bank_Name": row[21],
            "Ifsc_Code": row[22],
            "Account_Number": row[24],
            "Entity_Code": row[23]
        },
        "supervisors": list(),
    }
    payload["supervisors"].append(
        {"supervisorId": "EMP_002", "roleName": "super admin"}
    )

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user. 
    # if(row [32]):
    #     payload["supervisors"].append({
    #     "roleName": "L1 Manager",
    #     "supervisorId": row [32]
    #     })
    # if(row [34]):
    #     payload["supervisors"].append({
    #     "roleName": "L2 Manager",
    #     "supervisorId": row [34]
    #     })
    # if(row [36]):
    #     payload["supervisors"].append({
    #     "roleName": "Function Head",
    #     "supervisorId": row [36]
    #     })
    # if(row [38]):
    #     payload["supervisors"].append({
    #     "roleName": "Admin SPOC",
    #     "supervisorId": row [38]
    #     })
    # if(row [40]):
    #     payload["supervisors"].append({
    #     "roleName": "Finance SPOC",
    #     "supervisorId": row [40]
    #     })
    
    # if(row [37]):
    #     payload["supervisors"].append({
    #     "roleName": "CEO",
    #     "supervisorId": row [33]
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
    # print(e)
    # print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.')
    raise e
