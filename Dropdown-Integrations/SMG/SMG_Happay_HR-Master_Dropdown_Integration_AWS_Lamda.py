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
import fnmatch
from os import listdir

def get_system_ip():
    import urllib.request
    external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    print(external_ip)

#SFTP CONFIGURATION
SFTP_HOST = "203.197.227.18"
SFTP_USERNAME = 'SAPCCUSER01'
SFTP_PASSWORD = 'Maruti786'
SFTP_PORT = int(22)
DIR_PATH = 'C:/Users/sapccuser01.MSILSAPCCDMZ/HAPPAY/HR_MASTER_SMG/'

#FILE PROPERTIES
DELIMITER = '|'

#Connect to Queue for putting messages
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='lambda_api_processor')
print (str(queue))

# Header details for the API call.
api_url = "https://api-v2.happay.in/auth/v1/add_dropdown_values/"

request_headers = {
    "authorization": "Bearer bjWYFMBE0ftgd1m2upFcaolxY",
    "content-type": "application/json",
}

def get_sftp_connection():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(None,SFTP_USERNAME,SFTP_PASSWORD)
    return paramiko.SFTPClient.from_transport(transport)

generate_today_date = datetime.today().strftime('%d%m%Y')
def get_file(file_name):
    sftp = get_sftp_connection()
    file_pattern = file_name+generate_today_date+"*.csv"
    matching_file = [ file for file in sftp.listdir(DIR_PATH) if fnmatch.fnmatch(file, file_pattern)]
    latest_file = max(matching_file, key=lambda file: sftp.stat(DIR_PATH + '/' + file).st_mtime)
    return sftp.open(DIR_PATH + latest_file)

# HR Master Field Name
DEPT_SFCode_NAME = 'Department_SF_Code'
DEPT_SMGCode_NAME = 'Department_SMG_Code'
DIV_SFCode_NAME = 'Division_SF_Code'
DIV_SMGCode_NAME = 'Division_SMG_Code'
DIV_CLUSTER_SFCode_NAME = 'Division_Cluster_SF_Code'
DIV_CLUSTER_SMGCode_NAME = 'Division_Cluster_SMG_Code'
DIV_GROUP_SFCode_NAME = 'Division_Group_SF_Code'
DIV_GROUP_SMGCode_NAME = 'Division_Group_SMG_Code'
VERT_SFCode_NAME = 'Vertical_SF_Code'
VERT_SMGCode_NAME = 'Vertical_SMG_Code'
DESIG_SFCode_NAME = 'Designation_SF_Code'

# File Name Patterns
DEPT_FILE_NAME = 'EMPLOYEE_DEPT_SMG_'
DIV_FILE_NAME = 'EMPLOYEE_DIVN_SMG_'
DIV_CLSTR_FILE_NAME = 'EMPLOYEE_DIVN_CLSTR_SMG_'
DIV_GROUP_FILE_NAME = 'EMPLOYEE_DIVN_GRP_SMG_'
VERT_FILE_NAME = 'EMPLOYEE_VERTICAL_SMG_'
DESIG_FILE_NAME = 'EMPLOYEE_DESG_SMG_'

def create_payload_for_dropdown_api(Dropdown_Field_Name, Dropdown_Field_Value):
    payload = {
        "dd_values": [Dropdown_Field_Value],
        "field_type": "User", 
        "ef_name": Dropdown_Field_Name, 
        "requestId": generate_today_date,
    }
    return payload

# Department File : Department SF Code
def insertDeptSFCodeDropdownValues (row):
    DeptSFCodePayload = create_payload_for_dropdown_api(DEPT_SFCode_NAME, row[0])
    jsonPayload = json.dumps(DeptSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DeptSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DeptSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Department File : Department SMG Code
def insertDeptSMGCodeDropdownValues (row):
    DeptSMGCodePayload = create_payload_for_dropdown_api(DEPT_SMGCode_NAME, row[1])
    jsonPayload = json.dumps(DeptSMGCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DeptSMGCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DeptSMGCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division File : Division SF Code
def insertDivSFCodeDropdownValues (row):
    DivSFCodePayload = create_payload_for_dropdown_api(DIV_SFCode_NAME, row[0])
    jsonPayload = json.dumps(DivSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DivSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DivSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division File : Division SMG Code
def insertDivSMGCodeDropdownValues (row):
    DivSMGCodePayload = create_payload_for_dropdown_api(DIV_SMGCode_NAME, row[1])
    jsonPayload = json.dumps(DivSMGCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DivSMGCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DivSMGCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division Cluster File : Cluster SF Code
def insertClusterSFCodeDropdownValues (row):
    clusterSFCodePayload = create_payload_for_dropdown_api(DIV_CLUSTER_SFCode_NAME, row[0])
    jsonPayload = json.dumps(clusterSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=clusterSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : clusterSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division Cluster File : Cluster SMG Code
def insertClusterSMGCodeDropdownValues (row):
    clusterSMGCodePayload = create_payload_for_dropdown_api(DIV_CLUSTER_SMGCode_NAME, row[1])
    jsonPayload = json.dumps(clusterSMGCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=clusterSMGCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : clusterSMGCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division Group File : Division Group SF Code
def insertDivGrpSFCodeDropdownValues (row):
    DivGrpSFCodePayload = create_payload_for_dropdown_api(DIV_GROUP_SFCode_NAME, row[0])
    jsonPayload = json.dumps(DivGrpSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DivGrpSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DivGrpSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Division Group File : Division Group SMG Code
def insertDivGrpSMGCodeDropdownValues (row):
    DivGrpSMGCodePayload = create_payload_for_dropdown_api(DIV_GROUP_SMGCode_NAME, row[1])
    jsonPayload = json.dumps(DivGrpSMGCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DivGrpSMGCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DivGrpSMGCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Vertical File : Vertical SF Code
def insertVertSFCodeDropdownValues (row):
    VertSFCodePayload = create_payload_for_dropdown_api(VERT_SFCode_NAME, row[0])
    jsonPayload = json.dumps(VertSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=VertSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : VertSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Vertical File : Vertical SMG Code
def insertVertSMGCodeDropdownValues (row):
    VertSMGCodePayload = create_payload_for_dropdown_api(VERT_SMGCode_NAME, row[1])
    jsonPayload = json.dumps(VertSMGCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=VertSMGCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : VertSMGCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

# Designation File : Designation SF Code
def insertDesigSFCodeDropdownValues (row):
    DesigSFCodePayload = create_payload_for_dropdown_api(DESIG_SFCode_NAME, row[0])
    jsonPayload = json.dumps(DesigSFCodePayload)
    response = requests.post(url=api_url, headers=request_headers, json=DesigSFCodePayload)
    queue.send_message(MessageBody=json.dumps({
        "api_url" : api_url,
        "headers" : request_headers,
        "payload_data" : DesigSFCodePayload,
        "lambda_function_name" : "SMG_HR_Master",
        "process_id" : process_id
    }))

def SMG_department():
    try:
        error_rows = []
        file_pattern_name = DEPT_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertDeptSFCodeDropdownValues(row)
                insertDeptSMGCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e

def SMG_division():
    try:
        error_rows = []
        file_pattern_name = DIV_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertDivSFCodeDropdownValues(row)
                insertDivSMGCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e

def SMG_division_cluster():
    try:
        error_rows = []
        file_pattern_name = DIV_CLSTR_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertClusterSFCodeDropdownValues(row)
                insertClusterSMGCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e

def SMG_division_group():
    try:
        error_rows = []
        file_pattern_name = DIV_GROUP_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertDivGrpSFCodeDropdownValues(row)
                insertDivGrpSMGCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e

def SMG_vertical():
    try:
        error_rows = []
        file_pattern_name = VERT_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertVertSFCodeDropdownValues(row)
                insertVertSMGCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e
    
def SMG_designation():
    try:
        error_rows = []
        file_pattern_name = DESIG_FILE_NAME
        file = get_file(file_pattern_name)
        lines = csv.reader(file, delimiter=DELIMITER)
        headers = next(
            lines
        )  
        for row_count, row in enumerate(lines):
            try:
                insertDesigSFCodeDropdownValues(row)
            except Exception as e:
                print (str(e))
                error_rows.append({'row_index': row_count+1, 'error': str(e)})
        print (str(error_rows))
    except Exception as e:
        raise e

process_id = uuid.uuid4().hex
# Lambda Function
def lambda_handler(event, context):
    get_system_ip()
    try:
        SMG_department()
        SMG_division()
        SMG_division_cluster()
        SMG_division_group()
        SMG_vertical()
        SMG_designation()        
    except Exception as e:
        raise e
  
