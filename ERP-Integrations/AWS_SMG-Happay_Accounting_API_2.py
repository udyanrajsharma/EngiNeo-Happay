import json
import requests
from datetime import datetime


current_date_time = datetime.now()
current_system_date = current_date_time.strftime('%d-%b-%y %I:%M:%S %p')
def lambda_handler(event, context):
    try:
        # Extract data from the event
        report_data = json.loads(event['body'])
   
        mapped_values = []

        for transaction in report_data.get( 'transactions' ):
            if 'transaction_extra_fields' in transaction:
                Txn_Number = transaction.get('transaction_extra_fields').get('Txn_Number','')
                approval_date = transaction.get('approval_date', '')
                approval_date_txn =  datetime.strptime(approval_date, '%Y-%m-%d %H:%M:%S').strftime('%d-%b-%y %I:%M:%S %p')
                transaction_date = transaction.get('transaction_date', '')
                transaction_date_txn =  datetime.strptime(transaction_date, '%Y-%m-%d %H:%M:%S').strftime('%d-%b-%y %I:%M:%S %p')
                transaction_description = transaction.get('transaction_description', '')
                amount = transaction.get('Amount', '')
                approved_amount = transaction.get('approved_amount', '')

            if transaction['flight_data'] or transaction['cab_data'] or transaction['train_data']:
                rquest_data = transaction.get('flight_data','') or transaction.get('cab_data','') or transaction.get('train_data','')
                trip_id = rquest_data.get('trip_detail').get('trip_id','')
                trip_startdate = rquest_data.get('trip_detail','').get('trip_start_date','')
                trip_start_date =  datetime.strptime(trip_startdate, '%Y-%m-%d').strftime('%d-%b-%y')
                trip_enddate = rquest_data.get('trip_detail','').get('trip_end_date','')
                trip_end_date =  datetime.strptime(trip_enddate, '%Y-%m-%d').strftime('%d-%b-%y')
                travel_start_end_date = 'From'+ trip_start_date + ' To' + trip_end_date
            
            else:
                trip_id = ''
                trip_start_date = ''
                trip_end_date = ''
                travel_start_end_date = ''
            
            # Append to mapped_values list
            mapped_values.append({'amount': amount, 'approved_amount': approved_amount, 'Txn_Number':Txn_Number, 'transaction_date_txn':transaction_date_txn, 'transaction_description':transaction_description, 'trip_id':trip_id, 'trip_start_date': trip_start_date, 'trip_end_date': trip_end_date, 'travel_start_end_date':travel_start_end_date, 'approval_date_txn':approval_date_txn})

        employee_Id = report_data.get('user_data').get('user_extra_fields').get('Employee_Id', '')
        Location_Code = report_data.get('user_data').get('user_extra_fields').get('Location_Code', '')
        Cost_Center_Code = report_data.get('user_data').get('user_extra_fields').get('Cost_Center_Code', '')
        Claim_Id = report_data.get('report_extra_fields').get('Claim_Id', '')
        Account_Code = report_data.get('report_extra_fields').get('Account_Code', '')
        Report_Type_value = report_data.get('report_extra_fields').get('Report_Type', '')
     
        if Report_Type_value == 'Domestic travel' or 'Overseas Travel':
            report_type = 'TOURCLM'
        else:
            report_type = 'NON TRAVEL'

        first_name = report_data.get('user_data').get('first_name', '')
        last_name = report_data.get('user_data').get('last_name', '')
        employee_name =  f"{first_name} {last_name}" 

        request_payload_smg_api = {
            'p_list_param': []
        }

        counter = 0
        for value in mapped_values:
            if counter == 0:
                api_key_value = "279ce6bffe05f45e5a5ab42aaf8f7522c1f4b0aa205e269d930184e662868b0e"
            else:
                api_key_value = None
            counter = counter + 1
            request_payload_smg_api['p_list_param'].append({
                "p_estg_serial_no" : "",
                "p_estg_inv_cntrl_no" : Claim_Id,
                "p_estg_company_code" : "30",
                "p_estg_cost_center" : Cost_Center_Code,
                "p_estg_account" : Account_Code,
                "p_estg_sub_account" : "0000",
                "p_estg_product" : "000000",
                "p_estg_geography" : Location_Code,
                "p_estg_future1" : "0000",
                "p_estg_future2" : "0000",
                "p_estg_attribute_category" : report_type,
                "p_estg_created_by" : "HAPPAY",
                "p_estg_created_on" : current_system_date,
                "p_estg_approved_by" : "HAPPAY",
                "p_estg_approved_on" : value["approval_date_txn"],
                "p_estg_invoice_date" : value["transaction_date_txn"],
                "p_estg_party_code" : employee_Id,
                "p_estg_party_site" : "OFFICE",
                "p_estg_party_name" : employee_name,
                "p_estg_dff" : value['travel_start_end_date'],
                "p_estg_attribute1" : "",
                "p_estg_attribute2" : "",
                "p_estg_attribute3" : "",
                "p_estg_attribute4" : "",
                "p_estg_attribute5" : "",
                "p_estg_attribute6" : "",
                "p_estg_attribute87" : value['trip_start_date'],
                "p_estg_attribute8" : value['trip_end_date'],
                "p_estg_attribute9" : "",
                "p_estg_attribute10" : "",
                "p_estg_attribute11" : value['trip_id'],
                "p_estg_attribute12" : "",
                "p_estg_attribute13" : "",
                "p_estg_attribute14" : "",
                "p_estg_attribute15" : "",
                "p_estg_pay_group_lookup_code" : "",
                "p_estg_gl_date" : current_system_date,
                "p_estg_amount_cr" : "",
                "p_estg_amount_dr" : value['approved_amount'],
                "p_estg_terms_date" : current_system_date,
                "p_estg_description" : value['transaction_description'],
                "p_estg_item_type" : "STANDARD",
                "p_estg_transfer_flag" : "",
                "p_estg_error_description" : "",
                "p_estg_user_je_source_name" : ""
        })
        
        print(request_payload_smg_api)
        
        j = 0
        for i in request_payload_smg_api['p_list_param']:
            i["p_estg_serial_no"] = j + 1
            # i["p_api_key"] = "279ce6bffe05f45e5a5ab42aaf8f7522c1f4b0aa205e269d930184e662868b0e"
            j = j + 1
        request_payload_smg_api['p_list_param'][0]["p_api_key"] = "279ce6bffe05f45e5a5ab42aaf8f7522c1f4b0aa205e269d930184e662868b0e"
        print(request_payload_smg_api)
        # Call the Invoke_SMG_Accounting_API 
        response_from_SMG_Accounting_api = Invoke_SMG_Accounting_API(request_payload_smg_api) 

        # Return the response from the Accounting Wrapper API
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Accounting Wrapper API called successfully", "Request Payload to SMG API :": request_payload_smg_api, "Response from SMG Accounting API": response_from_SMG_Accounting_api})
        }

    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"Error Calling Accounting Wrapper API": str(e)})
        }

def Invoke_SMG_Accounting_API(SMG_Accounting_API_RequestBody):
    try:
        SMG_Accounting_api_url = 'https://qualitymonth.maruti.co.in/happaypayment/msilapi/payment'
        SMG_Accounting_api_header = {
            "content-type": "application/json",
            # "x-api-key": "279ce6bffe05f45e5a5ab42aaf8f7522c1f4b0aa205e269d930184e662868b0e"
        }
        response = requests.post(url=SMG_Accounting_api_url, json=SMG_Accounting_API_RequestBody, headers=SMG_Accounting_api_header)
        # Return the JSON response from the SMG_Accounting_API
        return response.json()

    except Exception as e:
        raise Exception(f"Error calling SMG BudgetCheck API: {str(e)}")