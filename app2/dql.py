 
import json 
from openai import OpenAI
import  oracledb
import json
import os  
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
#app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)

 

def get_ora_error(ora_err_code ):
    """Get the error description for ora error code"""
    print("calling get_ora_error to check " +ora_err_code.lower())
    connection =  oracledb.connect(user="rco", password="a",host="ahd-wxapancb601", port=1521, service_name="orapdb193")
    
    cursor = connection.cursor()
    res=[]
    ##res.append({"ora_err_code": ora_err_code.lower(), "desc":  ''}) 
    #for result in cursor.execute("select constraint_name||':'||decode(source ,null,table_name||'('||child_columns||') do not exists in parent table '||parent_table_name||'('||parent_columns||')'  , source)  source from table_constraints where constraint_name like '%'||:cs_name ||'%'" ,cs_name=ora_err_code.upper()):
    #    res.append( {"ora_err_code": ora_err_code, "desc":  result }) 
    res.append( {"ora_err_code": ora_err_code, "desc": str(cursor.execute("select constraint_name||':'||decode(source ,null,table_name||'('||child_columns||') do not exists in parent table '||parent_table_name||'('||parent_columns||')'  , source)  source from table_constraints where constraint_name like '%'||:cs_name ||'%'" ,cs_name=ora_err_code.upper()).fetchall())}) 
    
    print(json.dumps(res) )
    return json.dumps(res) 
    if "ora-00001" in ora_err_code.lower():
        return json.dumps({"ora_err_code": "ora-00001", "desc": "unique constraint (schema_name.constraint_name) violated"})
    elif "ora-00060" in ora_err_code.lower():
        return json.dumps({"ora_err_code": "ora-00060", "desc": "deadlock detected while waiting for resource"})
    elif "ora-01555" in ora_err_code.lower(): 
        return json.dumps({"ora_err_code": "ora-01555", "desc": "oracle snapshot too old, need to increase undo"})
    elif "ora-00600" in ora_err_code.lower(): 
        return json.dumps({"ora_err_code": "ora-00600", "desc": "oracle internal error, Please check metalink with your oracle support id "})
    else:
        return json.dumps({"ora_err_code": ora_err_code, "desc": "unknown error"})



def run_conversation(question):
    # Step 1: send the conversation and available functions to the model
    messages = []
    messages.append({"role": "system", "content": "Answer user questions by generating SQL queries against RFO table_constraints"})
    messages.append({"role": "user", "content": question})
    
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_ora_error",
                "description": "Get the error description for given error code",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ora_err_code": {
                            "type": "string",
                            "description": "error code ",
                        },
                        "desc":  {
                            "type": "string",
                            "description": "error code description",
                        },
                    },
                    "required": ["ora_err_code"],
                },
            },
        }
    ]
    response = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=messages,
        tools=tools,
        tool_choice="auto",  # auto is default, but we'll be explicit
    )
    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls
    # Step 2: check if the model wanted to call a function
    if tool_calls:
        # Step 3: call the function
        # Note: the JSON response may not always be valid; be sure to handle errors
        available_functions = {
            "get_ora_error": get_ora_error,
        }  # only one function in this example, but you can have multiple
        messages.append(response_message)  # extend conversation with assistant's reply
        # Step 4: send the info for each function call and function response to the model
        for tool_call in tool_calls:
            function_name = tool_call.function.name
            function_to_call = available_functions[function_name]
            function_args = json.loads(tool_call.function.arguments)
            function_response = function_to_call(
                ora_err_code=function_args.get("ora_err_code") 
            )
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": function_response,
                }
            )  # extend conversation with function response
        second_response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages,
        )  # get a new response from the model where it can see the function response
        print(" \n " )  
        print(second_response )
        return second_response.choices[0].message.content
    
