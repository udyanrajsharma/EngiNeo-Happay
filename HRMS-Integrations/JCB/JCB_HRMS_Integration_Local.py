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
HAPPAY_API_TOKEN = "Bearer QJXOjM2ttFBbGDsYmY8DqIFIq"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": HAPPAY_API_TOKEN,
    "content-type": "application/json",
}

file_path = "D:/Animesh/HAPPAY/Integration/JCB/Data/HRMS Sheets/JCB_HRMS_Data_01022024_1.csv"


def create_payload_from_row(row):
    payload = {
        "emailId": row[5],
        "firstName": row[0],
        "title": row[4],
        "middleName": row[1],
        "dob": row[7],
        "gender": row[3],
        "password": "Jcb@2023",
        "mobileNo": row[6],
        "mobile_extension": "+91",
        "userId": row[10],
        "requestId": row[10],
        "lastName": row[2],
        "metaFields": {
            "Grade": row[8],
            "Designation": row[9],
            "Employee_Id": row[10],
            "Legal_Entity": row[11],
            "Legal_Entity_Code": row[12],
            "Function_Name": row[13],
            "Bu_Name": row[14],
            "Division": row[15],
            "Department": row[16],
            "Location": row[17],
            "Cost_Center": row[18],
            "Location_Group": row[19],
            "Working_Location": row[20],
            "Employee_Type": row[21],
            "Payroll_Area": row[22],
            "Jcb_Date_Of_Joining": row[23],
            "Date_Of_Joining_Hidden": row[24],
            "Expat": row[25],
            "Last_Working_Day": row[26]
            
        },
        "supervisors": list(),
    }

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    
    # if(row [27]):
    #     payload["supervisors"].append({
    #     "roleName": "L1 Manager",
    #     "supervisorId": row [27]
    #     })
    # if(row [28]):
    #     payload["supervisors"].append({
    #     "roleName": "L2 Manager",
    #     "supervisorId": row [28]
    #     })
    # if(row [29]):
    #     payload["supervisors"].append({
    #     "roleName": "Expat Manager",
    #     "supervisorId": row [29]
    #     })
    # if(row [30]):
    #     payload["supervisors"].append({
    #     "roleName": "Admin Head",
    #     "supervisorId": row [30]
    #     })
    #     # Not coming
    # if(row [31]):
    #     payload["supervisors"].append({
    #     "roleName": "COO",
    #     "supervisorId": row [31]
    #     })
    # if(row [32]):
    #     payload["supervisors"].append({
    #     "roleName": "Business Head",
    #     "supervisorId": row [32]
    #     })
    # if(row [33]):
    #     payload["supervisors"].append({
    #     "roleName": "Site HR Head",
    #     "supervisorId": row [33]
    #     })
    # if(row [34]):
    #     payload["supervisors"].append({
    #     "roleName": "Unit EVP",
    #     "supervisorId": row [34]
    #     })
    #      # Not coming
    # if(row [35]):
    #     payload["supervisors"].append({
    #     "roleName": "Unit Executive",
    #     "supervisorId": row [35]
    #     })
    #      # Not coming
    # if(row [36]):
    #     payload["supervisors"].append({
    #     "roleName": "HOD",
    #     "supervisorId": row [36]
    #     })
    
    # if(row [37]):
    #     payload["supervisors"].append({
    #     "roleName": "EVP Finance",
    #     "supervisorId": row [37]
    #     })
    # if(row [38]):
    #     payload["supervisors"].append({
    #     "roleName": "Site Head",
    #     "supervisorId": row [38]
    #     })
    
    # if(row [39]):
    #     payload["supervisors"].append({
    #     "roleName": "Travel Admin",
    #     "supervisorId": row [39]
    #     })
    # if(row [40]):
    #     payload["supervisors"].append({
    #     "roleName": "MD",
    #     "supervisorId": row [40]
    #     })
    # if(row [41]):
    #     payload["supervisors"].append({
    #     "roleName": "Finance Controller",
    #     "supervisorId": row [41]
    #     })
    # if(row [42]):
    #     payload["supervisors"].append({
    #     "roleName": "Aviation Liaison Manager",
    #     "supervisorId": row [42]
    #     })
    # if(row [43]):
    #     payload["supervisors"].append({
    #     "roleName": "Executive",
    #     "supervisorId": row [43]
    #     })
    # if(row [44]):
    #     payload["supervisors"].append({
    #     "roleName": "Finance Verifier",
    #     "supervisorId": row [44]
    #     })
    # if(row [45]):
    #     payload["supervisors"].append({
    #     "roleName": "HR Business Partner",
    #     "supervisorId": row [45]
    #     })
    # if(row [46]):
    #     payload["supervisors"].append({
    #     "roleName": "EVP HR",
    #     "supervisorId": row [46]
    #     })
    #      # Not coming
    # if(row [47]):
    #     payload["supervisors"].append({
    #     "roleName": "Processor",
    #     "supervisorId": row [47]
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
