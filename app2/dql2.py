 
import json 
from openai import OpenAI
import  oracledb
import json
import os  
import chromadb
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
    res.append( {"reference": ora_err_code, "desc": str(cursor.execute("select constraint_name||':'||decode(source ,null,table_name||'('||child_columns||') do not exists in parent table '||parent_table_name||'('||parent_columns||')'  , source)  source from table_constraints where constraint_name like '%'||:cs_name ||'%'" ,cs_name=ora_err_code.upper()).fetchall())}) 
    
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


def run_plsql(plsql ):
    """Get the error description for ora error code"""
    print("calling plsql to check " +plsql)
    return json.dumps({"reference": plsql, "desc": plsql+" is completed"})


def check_knowledge(question ):
    c = chromadb.HttpClient(host='localhost', port=8000)
    chromadb.PersistentClient(path="c:/ma/openai/db/")
    collection = c.get_or_create_collection("model2")
    results = collection.query(
        query_texts=[ question],
        n_results=3#,
        # where={"kk": "notion"}, # optional filter
        # where_document={"$contains":"search_string"}  # optional filter
    )
    return json.dumps({" reference": results, "desc":question})


def get_prompt(question,knowledge):
    return [
        {"role": "system", "content": "You are a helpful assistant."},
        {
            "role": "user",
            "content": f"""Answer the following Question based on the Context only. Only answer from the Context. If you don't know the answer, say 'I don't know'.
    Question: {question}\n\n
    Context: {knowledge}\n\n
    Answer:\n""",
        },
    ]

def api_call(messages, model):
    return client.chat.completions.create(
        model=model,
        messages=messages,
        stop=["\n\n"],
        max_tokens=1000,
        temperature=0.0,
    )

def run_conversation(question):
    # Step 1: send the conversation and available functions to the model
    messages = []
    messages.append({"role": "system", "content": "RFO assistant who can answe questions by generating SQL queries against RFO table_constraints and run sql in db"})
    messages.append({"role": "user", "content": question})
    
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_ora_error",
                "description": "Get the error description for given error code",
                "parameters": { "type": "object",
                    "properties": {     
                        "ora_err_code": {     "type": "string", "description": "error code ",  },
                        "desc":  {
                            "type": "string",
                            "description": "error code description",
                        },
                    },
                    "required": ["ora_err_code"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_plsql",
                "description": "run plsql and return task id",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "plsql": {
                            "type": "string",
                            "description": "plsql command",
                        }
                    },
                    "required": ["plsql"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "check_knowledge",
                "description": "check internal vector database to return respond for the question",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "question for to check in the vector database ",
                        }
                    },
                    "required": ["question"],
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
           # "get_ora_error": get_ora_error, 
            "run_plsql": run_plsql,
            "check_knowledge": check_knowledge,
        }  # only one function in this example, but you can have multiple
        messages.append(response_message)  # extend conversation with assistant's reply
        # Step 4: send the info for each function call and function response to the model
        for tool_call in tool_calls:
            function_name = tool_call.function.name
            function_to_call = available_functions[function_name]
            function_args = json.loads(tool_call.function.arguments)
            #print(function_args)
            function_response = function_to_call(
                #ora_err_code=function_args.get("ora_err_code") 
                **function_args
            )
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": function_response,
                }
            )  # extend conversation with function response
      

        
        kg=get_prompt(question,messages[3])
        second_response = api_call(kg, "gpt-3.5-turbo-1106") 
        return second_response
"""
        second_response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages,
        )  # get a new response from the model where it can see the function response
        print(" \n " )  
        print(second_response )
        return second_response.choices[0].message.content
"""
    
#run_conversation("why insert account hit fk_account_001 error")
#run_conversation(" help me create new context vi plsql on reporting date 20240101 sample pack_context.create_context(v_reporting_date ?);")
aa=run_conversation(" how to create oracle instance ")
print(aa)