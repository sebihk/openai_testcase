# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 23:48:37 2024

@author: WuJia
"""
import pandas as pd
import time
from itertools import chain, combinations
import chromadb 
import json
from openai import OpenAI
import os
import chromadb.utils.embedding_functions as embedding_functions
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key=os.environ.get('OPENAI_API_KEY'),
                model_name="text-embedding-ada-002"
            )
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
#c = chromadb.HttpClient(host='localhost', port=8000)
#chromadb.PersistentClient(path="c:/ma/openai/db/")
#kb_model="ETL"
#gpt_model= "gpt-3.5-turbo-0125"
gpt_model= "gpt-4-32k" 
filename="C:/ma/openai/upload/OALM_CS_20231231_Input/loans.csv"
working="C:/ma/openai/"
os.chdir(working)
current = os.getcwd()


def load_info(df_excel):
    col_def=[]
    for index, row in df_excel.iterrows():
        print(row[0],":", row[1])
        a="'"+str(row[0])+"':'"+ str(row[1])+"'"
        col_def.append(a.lower())
    return col_def

def load_excel_schema(excel_file,sheet ):
    df_excel=pd.read_excel( excel_file,
             sheet_name=sheet ,header=1) 
    col_def=load_info(df_excel)
    a=",".join(col_def)
    return a

def get_prompt(question,schema_def):
    return [
        {"role": "system", "content": "you are a ETL specialist . user will provide the source column name as input, please provide the column mapping recommendation base on the given  column name and column definition definition below:"+schema_def},
        {
            "role": "user",
          #  "content": f"""Answer the following Question based on the Context only. Only answer from the Context. If the document didnt have enough infomrmation, say 'I don't know the answer for {question}'.
          "content": f"""which column should "{question.lower()}" map to?""",
        },
    ]


def get_assistant_instruction(schema_def):
 
    return "You are an assistant that helps to perform data mapping for finanicaldata.\
      You will be given 'source column_name:purpose of source column_name' as input \
      targe schema definition is in csv format with Field and Description column. Field is the column name, description is the column description.\
      Schema definition are listed below: "  +schema_def+ "\
      please provide the mapping recommendation in this format 'source_column: source column name, target_column : explain' and try to limit the respond within 100 words base on the provided information."

"""
#df_excel=pd.read_excel( "C:/ma/openai/upload/Consolidated Data Requirements ZMdesk v4.90_Training.xlsx",
#              sheet_name='Loan' ,header=1) 
result=[]
#load_info(df_excel)
for col in df.columns:
    if (col.replace("*","").upper()).startswith("N/A"):
        print("skip")
    else:
        a=get_col(col.replace("*",""))
        result.append(col +" map to "+a["ids"][0][0])

df3=pd.DataFrame(result)     
  
print(df3)
"""
def get_col(column_name ,schema_def):
    message=get_prompt(column_name,schema_def)
    print(message)
    #respond =api_call(message)
    #return respond.choices[0].message.content
    return column_name
    
   

def rerun_col_map(csv_filename,excel_mapping_filename):  
    file_name = os.path.basename(csv_filename)
    df_excel=pd.read_excel( excel_mapping_filename,
                  sheet_name=file_name ,header=1) 
    for index, row in df_excel.iterrows():
        print(index )
        print(row[2])
        
 
def api_call(messages, model=gpt_model):
    return client.chat.completions.create(
        model=model,
        messages=messages,
        #stop=["\n\n"],
        #max_tokens=1000,
        temperature=0.1
    )





#schema_def=load_excel_schema("C:/ma/openai/upload/Consolidated Data Requirements ZMdesk v4.90_Training.xlsx","Loan")

#b=get_prompt("Loan number-unique id",schema_def)
#c=api_call (b)
def wait_on_run(run, thread):
    while run.status == "queued" or run.status == "in_progress":
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id,
        )
        time.sleep(0.5)
    return run

def submit_message(assistant_id, thread, user_message):
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=user_message
    )
    return client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
    )


def create_thread_and_run(user_input,MATH_ASSISTANT_ID):
    thread = client.beta.threads.create()
    run = submit_message(MATH_ASSISTANT_ID, thread, user_input)
    return thread, run

def pretty_print(messages):
    print("# Messages")
    for m in messages:
        print(f"{m.role}: {m.content[0].text.value}")
    print()

def format_message(messages):
    result=""
    for m in messages:
        result=result+f"{m.role}: {m.content[0].text.value}"
    return result

def get_response(thread):
    return client.beta.threads.messages.list(thread_id=thread.id)

 
def init_col_map(csv_filename,assistant):
    file_name = os.path.basename(csv_filename)
    print(file_name)
    df=pd.read_csv(csv_filename)
    columns = ['N/A','N/A.1' ]
    df.drop(columns, inplace=True, axis=1)

    row_count=len(df)
    distinct_array=df.nunique()
    
    df1=df.T.copy()
    src_col=[]
    src_distinct=[]
    src_desc=[]
    trg_col_recommend=[]
    trg_desc=[]
    
    for rec in df1.index:
        
        if distinct_array[rec]==row_count: 
            col_name=rec+ " unique id" 
        else:
            col_name=rec
            
        print("checking "+col_name)
        thread,run=create_thread_and_run(col_name.lower().replace("*",""),assistant.id)
        src_col.append(col_name)
        src_desc.append("")
        wait_on_run(run,thread)
        trg_col_recommend.append(format_message(get_response(thread)))
        trg_desc.append("  ")
        
        
    data = pd.DataFrame({ 
        "src_col": src_col, 
        "src_desc": src_desc, 
        "trg_col_recommend": trg_col_recommend,
        "trg_desc": trg_desc,
    } 
    ) 
    
    data.to_excel("C:/ma/openai/upload/"+file_name+"_mapping.xlsx",sheet_name=file_name)
 
def remove_cr(value):
    return value.lower().replace( '\r\n'," ").replace( '\n'," ").replace( '\r'," ")

def mapping(column_name,meaning ,assistant_id):
    input =  column_name+":"+meaning.replace("\n"," ")
    print("processing "+input)
    thread,run=create_thread_and_run(input,assistant_id)
    wait_on_run(run,thread)
    return format_message(get_response(thread))
    #return input



df_excel=pd.read_excel( "C:/ma/openai/upload/Consolidated Data Requirements ZMdesk v4.90_Training.xlsx",
             sheet_name='Loan' ,header=1) 

df3=df_excel[['Field','Description']] 
df3['Field'] = df3['Field'].str.lower()
df3['Description'] = df3.apply(lambda x: remove_cr(str(x.Description) ), axis=1)

csv_data=df3.to_csv( index=False)

assistant_instruction=get_assistant_instruction(csv_data)

assistant = client.beta.assistants.create(
    name="ETL specialist on ALM( asset liability management) fincial data",
    instructions=assistant_instruction,
    model=gpt_model)
 
assistant_id=assistant.id
df_excel1=pd.read_excel( "C:/ma/openai/explained_col_phase1.xlsx",
             sheet_name='loans') 
 
#df_excel1["MAPPING_RECOMMEND"] = None   
#df_excel1["MAPPING_RECOMMEND"] = df_excel1.apply(lambda x: mapping(x.COLUMN_NAME, x.ELABORATION,assistant_id), axis=1)

#df_excel1.to_excel(os.path.join(current,"explained_col_phase2.xlsx"),sheet_name="loans")


"""     
thread,run=create_thread_and_run("Principal/int payment amount",assistant.id)

wait_on_run(run,thread)

print(get_response(thread))
"""

#init_col_map(filename,assistant)
 