
from openai import OpenAI
import os
import chromadb.utils.embedding_functions as embedding_functions
import pandas as pd
import time
import numpy as np
working="C:/ma/openai/"
os.chdir(working)
current = os.getcwd()
location = os.path.join(current,"upload")

chromadb_host="localhost"

gpt_model= "gpt-4-turbo-preview" 
tabname = "loans"
filename=os.path.join(location,tabname + ".csv")

client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

explain_assistant_instruction = f"You are an assistant that helps to perform data mapping for finanical data.\
  You are very familar about how different financial instruments work in the market. \
  User prompt will be in following format financial_instrument_type:column_name:sample data \
  Your goal is to elaborate the purpose of the column_name and the sample data based on your financial knowledge about financial_instrument_type.\
  Limit your output in 100 words."
  
    

explain_assistant = client.beta.assistants.create(
    name="Financial Data specialist",
    instructions=explain_assistant_instruction,
    model=gpt_model)

explain_assistant_id=explain_assistant.id

guess_assistant_instruction = f"You are an assistant that helps to perform data mapping for finanical data.\
  You are very familar about how different financial instruments work in the market. \
  You will be given instrument name:list_of_raw_data representing possible attributes of that instrument. \
  You need to guess what does these data mean for the input instrument name  \
  Return your theory in 50 words"
  
guess_assistant = client.beta.assistants.create(
    name="Financial Data specialist",
    instructions=guess_assistant_instruction,
    model=gpt_model)  

guess_assistant_id=guess_assistant.id

def wait_on_run(run, thread):
    while run.status == "queued" or run.status == "in_progress":
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id,
        )
        time.sleep(2)
        #print("Waiting")
    return run

def get_response(thread):
    return client.beta.threads.messages.list(thread_id=thread.id)

def format_message(messages):
    result=""
    for m in messages:
        result=result+m.content[0].text.value
    return result

def submit_message(assistant_id, thread, user_message):
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=user_message
    )
    return client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
    )

def create_thread_and_run(user_input,assistant_id):
    thread = client.beta.threads.create()
    run = submit_message(assistant_id, thread, user_input)
    return thread, run

def explain(prompt,distinct,table_name=tabname):
    input = table_name + ':' + prompt +":" 
    input = input+" no sample value" if distinct is None else input + distinct
    print( input.replace("\n",""))
    thread,run=create_thread_and_run(input,explain_assistant_id)
    wait_on_run(run,thread)
    return format_message(get_response(thread))
    #return input



def guess(prompt, table_name=tabname):
    input = table_name + ':' + prompt
    print("guessing "+input)
    thread,run=create_thread_and_run(prompt,guess_assistant_id)
    wait_on_run(run,thread)
    return format_message(get_response(thread))

 


# read csv
df=pd.read_csv(filename)
# drop NA COLUMN  
df = df[df.columns.drop(list(df.filter(regex='N/A')))]  

# build df for mapping
columns_df = pd.DataFrame(df.columns,columns=["COLUMN_NAME"]) 
columns_df.to_csv(os.path.join(current,"explained_col.csv"), encoding='utf-8',index=False)
columns_df = pd.read_csv(os.path.join(current,"explained_col.csv"),header=0,index_col=["COLUMN_NAME"])
# gen temp file
print (os.path.join(current,"explained_col.csv"))
# add unique id column
columns_df["UNIQUE"] = None
columns_df["SAMPLE_UNIQUE"] = None

for strcol in df.select_dtypes(include=['object']).columns:
    #print(strcol)
   
    temp_df=df[strcol].dropna().unique() 
    temp_df_top_10=temp_df[np.argsort(temp_df)[-10:]]
    columns_df["UNIQUE"][columns_df.index.get_loc(strcol)] = ",".join( temp_df  )
    columns_df["SAMPLE_UNIQUE"][columns_df.index.get_loc(strcol)] = ",".join( temp_df_top_10  )


# add ELABORATION column
columns_df["ELABORATION"] = None  
#columns_df['ELABORATION'] = columns_df["COLUMN_NAME"].apply(explain) # apply to series does not need axis 
columns_df["ELABORATION"] = columns_df.apply(lambda x: explain(x.name, x.SAMPLE_UNIQUE), axis=1)
columns_df.to_excel(os.path.join(current,"explained_col_phase1.xlsx"),sheet_name="loans")

