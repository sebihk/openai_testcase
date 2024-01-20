 
import json 
from openai import OpenAI
import  oracledb
import json
import os 
#client = OpenAI(api_key='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')
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
#app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)

 

def get_col_list( ):
    """Get the error description for ora error code""" 
    connection =  oracledb.connect(user="rco", password="a",host="ahd-wxapancb601", port=1521, service_name="orapdb193")
    
    with connection.cursor() as cursor:
            sql = "SELECT TABLE_NAME,COLUMN_NAME FROM TABLE_COLUMNS where table_name in ('LOANDEPO')  AND COLUMN_NAME NOT LIKE 'CHECK%' AND COLUMN_NAME NOT LIKE 'PARTITION%' \
            AND COLUMN_NAME NOT LIKE '%DIM%' \
             AND COLUMN_NAME NOT LIKE '%ATTRIBUT%' \
           and ( column_name in ('NOMINAL','AMOUNT_1','COUNTRY_CODE') OR column_name LIKE '%COUNTRY%') "
            #,'COMPANY','CURRENCY','CONTRACT_TYPES','FAMILY'
            cursor.execute(sql)
            result_set = cursor.fetchall()

            # Extract the column names from the cursor description
            column_names = [column[0] for column in cursor.description]

            # Extract the column names from each row and convert to dictionary
            result_list = [dict(zip(column_names, row)) for row in result_set]

        # Format the result set as a JSON string
    result_set_json = json.dumps(result_list)
    return result_set_json



def run_conversation(question):
    # Step 1: send the conversation and available functions to the model
    result_set_json=get_col_list()
    prompt = f"Here are the columns in your database:\n{result_set_json}\nGenerate me SQL code for the following question using the information about my database. {question}" 
    messages = []
    #messages.append({"role": "system", "content": "generate sql base on table_columns and user input"})
    messages.append({"role": "user", "content": prompt})
    print(prompt)
     
    
    response = client.completions.create(
      model="gpt-3.5-turbo-instruct",
      prompt=prompt
    )
    print(response.choices[0].text)
    SQLQuery = response.choices[0].text
    return SQLQuery
    
print(run_conversation('What is the total NOMINAL from LOANDEPO for country USA'))