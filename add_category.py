import json
from db.Connection import Conn
from aws import lambdas

def lambda_handler(event, context):
    franchiseID = lambdas.get_query_parameter(event,"franchise")
    typeID = lambdas.get_query_parameter(event,"type")
    category = lambdas.get_event_body(event)['cat']
    addCat(franchiseID,typeID,category)

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
   
def addCat(f,t,c):
    sql = f"INSERT INTO Category (franchiseID, typeID, categoryName,categoryID) values ({f},{t},'{c}',5)"
    print(sql)
    conn = Conn()
    cursor = conn.getCursor()
    cursor.execute(sql)
    conn.commit()