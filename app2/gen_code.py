
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

explain_assistant_instruction = f"You are an assistant that helps to perform data mapping for finanical data with python pandas .\
  You are very familar about how different financial instruments python mapping logic. \
  User will provide the mapping login description. \
  Uou will generate the python code to map the data from source data frame src_df to target data dataframe trg_df"
  
    

explain_assistant = client.beta.assistants.create(
    name="Financial Data specialist",
    instructions=explain_assistant_instruction,
    model=gpt_model)

explain_assistant_id=explain_assistant.id

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

def submit_message(assistant_id, thread, user_message):
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=user_message
    )
    return client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
        tools=[{"type": "code_interpreter"}, {"type": "retrieval"}]
    )

def create_thread_and_run(user_input,assistant_id):
    thread = client.beta.threads.create()
    run = submit_message(assistant_id, thread, user_input)
    return thread, run



mapping_desc="assistant: Based on your description, the 'Interest rate over split' seems to mean the interest rate applicable to a specific portion of the loan after it has been split into different parts. In the target schema, this might map to the field 'cpn (%)' which represents the current coupon, which is the interest rate on a bond. However, we have to be caution given that 'Interest rate over split' refers to a portion of a loan while 'cpn (%)' is on the whole bond. \
    If we could assume they work on the same principle, then it could be:\
Source Column: Interest rate over split\
Target Column: cpn (%)user: Interest rate over split:The \"Interest rate over split\" column for loans likely refers to the interest rate applicable to a portion of the loan amount after splitting the total loan into different parts, possibly under varying conditions or for different purposes. For instance, a loan may be split into two parts—one part may have a fixed interest rate while the other has a variable rate.\
This column would specify the interest rate for the portion of the loan after such a split. Since there's no sample value, it's crucial to know the specific interest rate to fully understand the loan's cost structure for that part.loans:Interest rate over split: no sample value"

thread,run=create_thread_and_run(mapping_desc,explain_assistant_id)

wait_on_run(run, thread)

print(get_response(thread))