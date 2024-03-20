 
import json 
from openai import OpenAI
import  oracledb
import json
import os  
import chromadb 
import pandas as pd 
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
#app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)

#gpt_model= "gpt-3.5-turbo-0125" 

gpt_model= "gpt-4-1106-preview" #"gpt-4-turbo-preview"

kb_model="model3"


def check_knowledge(question ):
    #r=vdb.get_knowledge(question,"model3")
    result_number=10
    chromadb.PersistentClient(path="c:/ma/openai/db/")
    c = chromadb.HttpClient(host='localhost', port=8000)
    collection = c.get_or_create_collection(kb_model)
    r = collection.query(
        query_texts=[ question.lower()],
        n_results=result_number#,
        # where={"kk": "notion"}, # optional filter
        # where_document={"$contains":"search_string"}  # optional filter
    ) 
    
    content=r['documents'][0]
    docname=r['ids'][0]
    dist=r['distances'][0]

    data = pd.DataFrame({ 
        "docname": docname, 
        "content": content
      #  "distances": dist 
    } )
    return data 
    #return json.dumps(dict)


def get_prompt(question,knowledge):
    #print ("knowledge:" )
    #print (knowledge)
    return [
        {"role": "system", "content": "You are a helpful product assistant familar different financial instruments work in the market and Moodys system.\
         Please organize answer for the question base on the Context. \
         Context: {"+knowledge+"}\n\n"},
        {
        "role": "user",
        "content": f"""Answer the following Question based on the Context only. Only answer from the Context. If the context didn't have enough infomrmation, say 'I don't know the answer'.
    Question: {question}\n\n
        Answer:\n""",
        },
    ]
        # "content": f""" Try your best to answer the following Question based on the context only. please limit the respond within 600 words'.

 
 
def api_call(messages, model):
    return client.chat.completions.create(
        model=model,
        messages=messages,
        #stop=["\n\n"],
        max_tokens=1500,
        temperature=0.5
    )

def run_conversation_dql2(question):
    # Step 1: send the conversation and available functions to the model
  
        message=check_knowledge(question)
        print("a22:" ) 
        #kg=get_prompt(question,message.to_json(orient='table',index=False))
        kg=get_prompt(question,message.to_csv(index=False))
          
        print(kg)
     
        second_response = api_call(kg, gpt_model) 
        
        print("a25:")
        print(second_response.usage)
        return second_response.choices[0].message.content
 
#aa=run_conversation_dql2("how to do data mapping and loading for ZM/ALM?")
#print(aa)
question="What are the main new features in RFO 7.2.2?"

c=check_knowledge(question)
aa=run_conversation_dql2(question)
print(aa)


#aa=check_knowledge("how to create process profile?" )
#print(aa)