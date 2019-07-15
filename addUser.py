import json
from db.Connection import Conn
from aws import lambdas
import bcrypt
import base64
import hashlib
def lambda_handler(event, context):
    
   
    password = lambdas.get_event_body(event)['password']
    franchise = lambdas.get_event_body(event)['franchise']
    userName = lambdas.get_event_body(event)['username']
    department = lambdas.get_event_body(event)['department']
    # fName = lambdas.get_event_body(event)['FirstName']
    # lName = lambdas.get_event_body(event)['LastName']
    designation = lambdas.get_event_body(event)['designation']
    
    addUser(designation,department,franchise,hashPassword(password),userName)

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
   
def addUser(designation,department,franchise,password,userName):
    password = password.decode()
    print(password)
    sql = f"INSERT INTO Users (designationID, departmentID, franchiseID, password, username) values ({designation},{department},{franchise},'{password}','{userName}')"
    print(sql)
    conn = Conn()
    cursor = conn.getCursor()
    cursor.execute(sql)
    conn.commit()
def hashPassword(password):

    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(14))
    return hashed