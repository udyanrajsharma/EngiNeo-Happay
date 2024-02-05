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


# SFTP CONFIGURATION

SFTP_HOST = "13.234.100.189"
SFTP_USERNAME = "happay"
SFTP_PASSWORD = "A83et3f27QV"
SFTP_PORT = int(32254)
DIR_PATH = "/HR/"

# FILE PROPERTIES
DELIMITER = "|"

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    "authorization": "Bearer mKMTIABYdgfcrd4y3l5VY5vfu",
    "content-type": "application/json",
}

def generate_file_name():
    generate_file_name = datetime.today().strftime('%d%m%Y')
    return "Empl_Master_"+generate_file_name+".csv"
    #return "Empl_Master_13072023.csv"


def get_file_from_sftp(file_name):
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(None,SFTP_USERNAME,SFTP_PASSWORD)

    connection = paramiko.SFTPClient.from_transport(transport)
    return connection.open(DIR_PATH + file_name)


def create_payload_from_row(row):
    payload = {
        "firstName": row[2],
        # "middleName": row[],
        "lastName": row[3],
        "userId": row[1],
        "requestId": row[1],
        "gender": row[5],
        "title": row[4],
        "password": "Ceat@123",
        "mobile_extension": "+91",
        "emailId": row[8],
        "mobileNo": row[7],
        "dob": row[6],
        "metaFields": {
            "Test_Employee_Code": row[1],
            "Employee_Code": row[1],
            "Grade": row[12],
            "Designation": row[0],
            "Employee_Type": row[10],
            "Company_Name": row[13],
            "Company_Code": row[14],
            "Country": row[15],
            "Region": row[24],
            "Last_Working_Day": row[9],
            "Business_Unit": row[16],
            "Business_Unit_Code": row[17],
            "Sbu": row[19],
            "Sbu_Code": row[18],
            "Function": row[20],
            "Department": row[22],
            "Location": row[26],
            "Cost_Center": row[27],
            # "Rc_Upload": row[],
            # "Dl_Upload": row[],
            "Is_Tne_Admin": row[31],  
            "International_Calling_": row[32],
        },
        "supervisors": list(),
    }

    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    
    if(row [28]):
        payload["supervisors"].append({
        "roleName": "Line Manager",
        "supervisorId": row [28]
        })
    # if(row [29]):
    #     payload["supervisors"].append({
    #     "roleName": "Exco",
    #     "supervisorId": row [29]
    #     })
    # if(row [30]):
    #     payload["supervisors"].append({
    #     "roleName": "",
    #     "supervisorId": row [30]
    #     })
    return payload


process_id = uuid.uuid4().hex
get_system_ip()
try:
    error_rows = []
    file_name = generate_file_name()
    print("file name = ", file_name)
    file = get_file_from_sftp(file_name)
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
            print (str(e))
            error_rows.append({'row_index': row_count+1, 'error': str(e)})

    print (str(error_rows))
except Exception as e:
    # print(e)
    # print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.')
    raise e
