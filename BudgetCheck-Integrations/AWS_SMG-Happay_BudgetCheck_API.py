import json
import requests
from datetime import datetime

def lambda_handler(event, context):
    BUDGET_STATUS_AVAILABLE = 'Available'
    BUDGET_STATUS_NOT_AVAILABLE = 'Not Available'
    SUBMIT_ACTION = 'Submit'
    BUDGET_CHECK_ACTION = 'Budget Check'

    try:
        # Extract data from the event
        data = json.loads(event['body'])

        Employee_Id = data.get('user_data', {}).get('user_extra_fields', {}).get('Employee_Id', '')
        Report_ID = data.get('report_id', 'N/A')
        claim_Id = data.get('report_extra_fields', {}).get('Claim_Id', '')
        Account_Code = data.get('report_extra_fields', {}).get('Account_Code', '')
        Sub_Account_Code = data.get('report_extra_fields', {}).get('Sub_Account_Code', '')
        Cost_Center_Code = data.get('user_data', {}).get('user_extra_fields', {}).get('Cost_Center_Code', '')
        Location_Code = data.get('user_data', {}).get('user_extra_fields', {}).get('Location_Code', '')
        transactions = data.get('transactions', [])
        total_amount = sum(float(transaction.get('approved_amount', 0)) for transaction in transactions)

        # SMG_BudgetCheck_API_requestData
        SMG_API_RequestBody = {
            "p_control_no" : claim_Id,
            "p_company_code" : "30",
            "p_ccen" : Cost_Center_Code,
            "p_acct" : Account_Code, 
            "p_gsacc" : Sub_Account_Code,
            "p_gpr" : "000000",
            "p_gge" : Location_Code,
            "p_gfu1" : "0000",
            "p_gfu2" : "0000",
            "p_dr_amount" : total_amount,
            "p_sno" : 1
        }

        # Call the Invoke_SMG_BudgetCheck_API 
        response_from_SMG_BudgetCheck_api = Invoke_SMG_BudgetCheck_API(SMG_API_RequestBody) 

        if response_from_SMG_BudgetCheck_api.get('budgetStatus') == 'TRUE':
            BudgetStatus = BUDGET_STATUS_AVAILABLE
            Action = SUBMIT_ACTION
            # Call the Invoke_Happay_budgetStatusUpdate_API to change Report Extra Field Value
            response_from_Happay_budgetStatusUpdate_api = Invoke_Happay_budgetStatusUpdate_API(Report_ID,BudgetStatus)
            # Call the Invoke_Happay_ContainerUpdate_API to Change Container Status
            response_from_ContainerUpdate_api = Invoke_Happay_ContainerUpdate_API(Report_ID,Employee_Id,Action)

        else :
            BudgetStatus = BUDGET_STATUS_NOT_AVAILABLE
            Action = BUDGET_CHECK_ACTION
            # Call the Invoke_Happay_budgetStatusUpdate_API to change Report Extra Field Value
            response_from_Happay_budgetStatusUpdate_api = Invoke_Happay_budgetStatusUpdate_API(Report_ID,BudgetStatus)
            # Call the Invoke_Happay_ContainerUpdate_API to Change Container Status
            response_from_ContainerUpdate_api = Invoke_Happay_ContainerUpdate_API(Report_ID,Employee_Id,Action)

        # Return the response from the Budget Check Wrapper API
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Budget Check Wrapper API called successfully", "Request Payload to SMG API :": SMG_API_RequestBody, "Response from SMG Budget Check API": response_from_SMG_BudgetCheck_api, "Response from Happay Budget Status Update API =":response_from_Happay_budgetStatusUpdate_api, "Response from Container Update API :":response_from_ContainerUpdate_api})
        }

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"Error Calling Budget Check Wrapper API": str(e)})
        }

def Invoke_SMG_BudgetCheck_API(SMG_API_RequestBody):
    try:
        SMG_BudgetCheck_api_url = 'http://203.197.227.58/happay/msilapi/checkbudget'
        SMG_BudgetCheck_api_header = {
            "content-type": "application/json",
        }
        response = requests.post(url=SMG_BudgetCheck_api_url, json=SMG_API_RequestBody, headers=SMG_BudgetCheck_api_header)

        # Return the JSON response from the SMG_BudgetCheck_API
        return response.json()

    except Exception as e:
        raise Exception(f"Error calling SMG BudgetCheck API: {str(e)}")

def Invoke_Happay_budgetStatusUpdate_API(report_id,BudgetStatus):
    try:
        ReportField_Update_api_url = 'https://api-v2.happay.in/auth/v1/updateobjectstatus/'
        ReportField_Update_api_header = {
            "authorization": "Bearer 9KWd9lJjw959tIXI55YJYB1MU",
            "content-type": "application/json",
        }
        ReportField_Update_api_body = {
            "objectId": report_id,
            "requestId": report_id,
            "objectType": "Reports",
            "metaFields": {
                "Budget_Status": BudgetStatus,
            }
        }
        response = requests.post(url=ReportField_Update_api_url, json=ReportField_Update_api_body, headers=ReportField_Update_api_header)

        return response.json()

    except Exception as e:
        raise Exception(f"Error calling Happay BudgetCheck Update RI API: {str(e)}")
    
def Invoke_Happay_ContainerUpdate_API(report_id,Employee_Id,Action):
    try:
        Happay_ContainerUpdate_api_url = 'https://api-v2.happay.in/auth/v1/container-action/'

        Happay_ContainerUpdate_api_header = {
            "authorization": "Bearer 9KWd9lJjw959tIXI55YJYB1MU",
            "content-type": "application/json",
        }
        Today_date = datetime.today().strftime('%d%m%Y')

        Happay_ContainerUpdate_api_body = {
            "object_id": report_id, 
            "object_type": "Reports",
            "actor_id": Employee_Id,
            "action": Action,
            "requestId": Today_date
        }
        response = requests.post(url=Happay_ContainerUpdate_api_url, json=Happay_ContainerUpdate_api_body, headers=Happay_ContainerUpdate_api_header)

        return response.json()

    except Exception as e:
        raise Exception(f"Error calling Happay Container Update API: {str(e)}")
