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
    external_ip = "103.87.59.124"
    
    
    print(external_ip)

# FILE PROPERTIES
DELIMITER = "|"
HAPPAY_API_TOKEN = "Bearer 2g4v6jPcgMWMqI2GzmxoDTRK5"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": HAPPAY_API_TOKEN,
    "content-type": "application/json",
}

file_path = "D:/Animesh/HAPPAY/Integration/Suzuki Motor Gujarat/DATA/Data Sheet/UAT testing_20012024_2.csv"



def create_payload_from_row(row):
    payload = {
        "emailId": row[0],
        "firstName": row[2],
        "title": row[1],
        "middleName": row[4],
        "dob": row[6],
        "gender": row[5],
        "password": "Smg@2024",
        "mobileNo": row[7],
        "mobile_extension": "+91",
        "userId": row[8],
        "requestId": row[8],
        "lastName": row[3],
        "metaFields": {
            "Employee_Id": row[8],
            "Designation_Sf_Code": row[9],
            "Designation_Name": row[10],
            "Position_Code": row[11],
            "Position_Name": row[12],
            "Department_Sf_Code": row[13],
            "Department_Smg_Code": row[14],
            "Department_Name": row[15],
            "Company_Code": row[16],
            "Company_Name": row[17],
            "Level_Name": row[18],
            "Level_Code": row[19],
            "Division_Sf_Code": row[20],
            "Division_Smg_Code": row[21],
            "Division_Name": row[22],
            "Vertical_Sf_Code": row[23],
            "Vertical_Smg_Code": row[24],
            "Vertical_Name": row[25],
            "Division_Group_Sf_Code": row[26],
            "Division_Group_Smg_Code": row[27],
            "Division_Group_Name": row[28],
            "Division_Cluster_Name": row[29],
            "Division_Cluster_Sf_Code": row[30],
            "Location_Code": row[31],
            "Location_Name": row[32],
            "Cost_Center_Code": row[33],
            "Cost_Center_Name": row[34],
            "Last_Working_Day": row[35],
        },
        "supervisors": list(),
    }

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    
    if(row [36]):
        payload["supervisors"].append({
        "roleName": "Director - GA",
        "supervisorId": row [36]
        })
    if(row [37]):
        payload["supervisors"].append({
        "roleName": "Departmental Director",
        "supervisorId": row [37]
        })
    if(row [38]):
        payload["supervisors"].append({
        "roleName": "Plant Head",
        "supervisorId": row [38]
        })
    if(row [39]):
        payload["supervisors"].append({
        "roleName": "HR 2",
        "supervisorId": row [39]
        })
    if(row [40]):
        payload["supervisors"].append({
        "roleName": "HR 1",
        "supervisorId": row [40]
        })
    if(row [41]):
        payload["supervisors"].append({
        "roleName": "Finance 1",
        "supervisorId": row [41]
        })
    if(row [42]):
        payload["supervisors"].append({
        "roleName": "Finance 2",
        "supervisorId": row [42]
        })
    if(row [43]):
        payload["supervisors"].append({
        "roleName": "Finance 3",
        "supervisorId": row [43]
        })
    if(row [44]):
        payload["supervisors"].append({
        "roleName": "DDPM",
        "supervisorId": row [44]
        })
    if(row [45]):
        payload["supervisors"].append({
        "roleName": "DPM",
        "supervisorId": row [45]
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
