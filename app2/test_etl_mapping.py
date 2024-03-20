
from openai import OpenAI
import os
import chromadb.utils.embedding_functions as embedding_functions
import pandas as pd
import time
working="C:/ma/openai/"
os.chdir(working)
current = os.getcwd()
location = os.path.join(current,"upload")
chromadb_host="localhost"

gpt_model= "gpt-4-turbo-preview" 
tabname = "loans"
filename=os.path.join(location,tabname + ".csv")


explain_assistant_instruction = f"You are an assistant that helps to perform data mapping for finanical data.\
  You are very familar about how different financial instruments work in the market. \
  User prompt will be in following format financial_instrument_type:column_name \
  Your goal is to elaborate the purpose of the column_name based on your financial knowledge about financial_instrument_type.\
  Limit your output in 50 words"
    
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
assistant = client.beta.assistants.create(
    name="Financial Data specialist",
    instructions=explain_assistant_instruction,
    model=gpt_model)


df=pd.read_csv(filename)

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

def explain(prompt,table_name=tabname):
    input = table_name + ':' + prompt
    #print(input)
    thread,run=create_thread_and_run(input,assistant.id)
    wait_on_run(run,thread)
    return format_message(get_response(thread))

def guess(prompt):
    thread,run=create_thread_and_run(prompt,assistant.id)
    wait_on_run(run,thread)
    return format_message(get_response(thread))



#print(df.columns)
#print(type(df.columns))

#columns_df = pd.DataFrame(df.columns,columns=["COLUMN_NAME"])
#print(columns_df)
#columns_df['ELABORATION'] = columns_df["COLUMN_NAME"].apply(explain) # apply to series does not need axis
#columns_df.to_csv("explained_col.csv", encoding='utf-8',index=False)

columns_df = pd.read_csv(os.path.join(current,"explained_col.csv"),header=0,index_col=["COLUMN_NAME"])

columns_df["UNIQUE"] = None

for strcol in df.select_dtypes(include=['object']).columns:
    #print(strcol)
    columns_df["UNIQUE"][columns_df.index.get_loc(strcol)] = str(df[strcol].unique())

#print(columns_df)
#columns_df.to_csv("explained_col_unique.csv", encoding='utf-8')


guess_assistant_instruction = f"You are an assistant that helps to perform data mapping for finanical data.\
  You are very familar about how different financial instruments work in the market. \
  You will be given a list of raw data representing the same attribute of loan but we do not know what it is. \
  You need to guess what does these data mean in the setting of a financial loan. \
  Return your theory in 50 words"

assistant = client.beta.assistants.create(
    name="Financial Data specialist",
    instructions=guess_assistant_instruction,
    model=gpt_model)

#print(columns_df["UNIQUE"])
columns_df['GUESS_BASED_ON_UNIQUE']  = None
columns_df['GUESS_BASED_ON_UNIQUE'] = columns_df[columns_df["UNIQUE"].notnull()]["UNIQUE"].apply(guess) # apply to series does not need axis
#print(columns_df['GUESS_BASED_ON_UNIQUE'] )


#print(columns_df)
import chromadb
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key=os.environ.get("OPENAI_API_KEY"),
                model_name="text-embedding-3-small"
)
db = chromadb.HttpClient(host=chromadb_host, port=8000)
try:
    db.delete_collection("SCHEMA")
except:
    pass
schema_collection =  db.get_or_create_collection(name="SCHEMA",embedding_function=openai_ef)

df_excel=pd.read_excel( os.path.join(location,"Consolidated Data Requirements ZMdesk v4.90_Training.xlsx"),
             sheet_name='Loan' ,header=1) 

fields_only_df=df_excel[['Field','Description']] 
fields_only_df.columns = ["COLUMN_NAME","COLUMN_DESCRIPTION"]
fields_only_df['COLUMN_NAME'] = fields_only_df['COLUMN_NAME'].str.lower()
fields_only_df['COLUMN_DESCRIPTION'] = fields_only_df['COLUMN_DESCRIPTION'].str.lower()

def db_rm_pdf(file_name):
    target = []
    target = schema_collection.get(where={"source" :file_name })["ids"]
    if target:
        schema_collection.delete(ids=target)

def db_add_pdf(text,idx,metadata,embedding_function=openai_ef):
    schema_collection.add(
        embeddings = None,
        documents = text,
        metadatas = metadata,
        ids =idx
    )

#print(fields_only_df["COLUMN_DESCRIPTION"].tolist())

db_add_pdf([str(x) for x in fields_only_df["COLUMN_DESCRIPTION"]],[str(x) for x in fields_only_df.index.tolist()],[{"source":tabname,"column_name":column} for column in fields_only_df["COLUMN_NAME"]])

import json

def get(prompt):
    output = []
    db = chromadb.HttpClient(host=chromadb_host, port=8000)
    results = schema_collection.query(
        query_texts=[prompt],
        n_results=3
    )
    for document,metadata in zip(results["documents"][0],results["metadatas"][0]):
        output.append({metadata["column_name"]:document})
    #print(output)
    return json.dumps(output)

columns_df["POSSIBILITIES_1"] = columns_df["ELABORATION"].apply(get)
columns_df["POSSIBILITIES_2"] = columns_df[columns_df["UNIQUE"].notnull()]["GUESS_BASED_ON_UNIQUE"].apply(get)
columns_df.to_csv("explained_col.csv", encoding='utf-8',index=False)

