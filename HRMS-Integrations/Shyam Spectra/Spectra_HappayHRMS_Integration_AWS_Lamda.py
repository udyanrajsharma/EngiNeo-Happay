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
    external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    print(external_ip)

#SFTP CONFIGURATION

SFTP_HOST = '180.151.96.125'
SFTP_USERNAME = 'ftpuser'
SFTP_PASSWORD = 'Spectra@2023'
SFTP_PORT = int(22)
DIR_PATH = '/SSPL/HRMS/'

#FILE PROPERTIES
DELIMITER = '|'

#Connect to Queue for putting messages
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='lambda_api_processor')
print (str(queue))

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_update_user/"

request_headers = {
    'authorization': "Bearer MPiGPGLKAkf4jlt6pVIB50tjU",
    'content-type': "application/json"
}

def generate_file_name():
    generate_file_name = datetime.today().strftime('%d%m%Y')
    return "Empl_Master_"+generate_file_name+".csv"
    #return "Empl_Master_13072023.csv"

def get_file_from_sftp(file_name):

    transport = paramiko.Transport((SFTP_HOST,SFTP_PORT))
    transport.connect(None,SFTP_USERNAME,SFTP_PASSWORD)

    connection = paramiko.SFTPClient.from_transport(transport)
    return connection.open(DIR_PATH+file_name)

def create_payload_from_row(row):
    print (row[0])

    payload = {
        "emailId": row[7],
        "firstName": row[1],
        "title": row[0],
        "middleName": row[2],
        "dob": row[4],
        "gender": row[5],
        "mobileNo": row[6],
        "mobile_extension": "+91",
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
            "Entity_Code": row[23],
            },
            "supervisors": list()
        }
    #Hemang's super admin to be assigned to all users
    payload["supervisors"].append(
        {"supervisorId": "EMP_002", "roleName": "super admin"}
    )
    # When SuperVisor Id (with role) is attached to any userId then automatically that Role will get assigned to that supervisor user.
    if(row [32]):
        payload["supervisors"].append({
        "roleName": "L1 Manager",
        "supervisorId": row [32]
        })
    if(row [34]):
        payload["supervisors"].append({
        "roleName": "L2 Manager",
        "supervisorId": row [34]
        })
    if(row [36]):
        payload["supervisors"].append({
        "roleName": "Function Head",
        "supervisorId": row [36]
        })
    if(row [38]):
        payload["supervisors"].append({
        "roleName": "Admin SPOC",
        "supervisorId": row [38]
        })
    if(row [40]):
        payload["supervisors"].append({
        "roleName": "Finance SPOC",
        "supervisorId": row [40]
        })
    return payload

def lambda_handler(event, context):
    process_id = uuid.uuid4().hex
    get_system_ip()
    try:
        error_rows = []
        file_name = generate_file_name()
        file = get_file_from_sftp(file_name)

        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(lines) #If file has headers; repeat for as many times as number of headers

        for row_count, row in enumerate(lines):
            try:
                payload = create_payload_from_row(row)

                response = queue.send_message(MessageBody=json.dumps({
                    "api_url" : api_url,
                    "headers" : request_headers,
                    "payload_data" : payload,
                    "lambda_function_name" : "Spectra_HRMS",
                    "row_number" : row_count,
                    "process_id" : process_id
                }))
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index' : row_count+1, 'error' : str(e)})

        print (str(error_rows))
        return True
    except Exception as e:
        # print(e)
        # print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.')
        raise e