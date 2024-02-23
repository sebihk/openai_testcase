# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 16:21:26 2024

@author: WuJia
"""
import chromadb 
import json  
import os
from langchain.document_loaders import PyMuPDFLoader 
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Chroma 
from langchain.embeddings import SentenceTransformerEmbeddings  

from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from langchain.text_splitter import (
    Language,
    RecursiveCharacterTextSplitter,
)

c = chromadb.HttpClient(host='localhost', port=8000)
chromadb.PersistentClient(path="c:/ma/openai/db/")


 

persist_directory = "c:/ma/openai/db/"
model_name = "sentence-transformers/all-MiniLM-L6-v2"
model_kwargs = {'device': 'cpu'}

kb_model="model3"

#ef1 = SentenceTransformerEmbeddingFunction("sentence-transformers/all-MiniLM-L6-v2")
#client.delete_collection("test")    
#collection = client.get_or_create_collection("test",embedding_function=ef1)
"""
id=1
for rec in (all_splits):
    #print(rec)
    
    
    if rec.page_content.count('.')>500 :
        print("-skip")
    else:
        #print(ef1(rec.page_content))
        collection.add(
            documents=rec.page_content, 
            metadatas=[{"type": 'rfo document', "page":rec.metadata["page"]}],
            ids=[str(id )]  # unique for each docui
            )   
    print("add id "+str(rec.metadata["page"])+"-"+str(id))
    id=id+1
"""


# setup Chroma in-memory, for easy prototyping. Can add persistence easily!
#chromadb(persist_directory="c:/ma/openai/db/")
#client = chromadb.Client()



# Create collection. get_collection, get_or_create_collection, delete_collection also available!
# collection = client.create_collection("all-my-documents")

# collection=get_knowledge()

# for rec in get_knowledge():

# Add docs to the collection. Can also update and delete. Row-based API coming soon!
    # print()
"""
    collection.add(
        # we handle tokenization, embedding, and indexing automatically. You can skip that and add your own embeddings as well
        documents=["This is document1", "This is document2"],
        metadatas=[{"source": "notion"}, {
            "source": "google-docs"}], # filter on these!
        ids=["doc1", "doc2"], # unique for each doc
    )

collection.add(
    # we handle tokenization, embedding, and indexing automatically. You can skip that and add your own embeddings as well
    documents=["hello world2", "This is document2"],
    metadatas=[{"kk": "notion"}, {"kk": "google-docs"}], # filter on these!
    ids=["doc3", "doc4"], # unique for each doc
)
# Query/search 2 most similar results. You can also .get by id
results = collection.query(
    query_texts=[ " document2"],
    n_results=5 #,
    # where={"kk": "notion"}, # optional filter
    # where_document={"$contains":"search_string"}  # optional filter
)

print(results)
"""
def findAllFile(base,extention):
    file_list=[]
    for root, ds, fs in os.walk(base):
        for f in fs:
            if f.endswith(extention):
                fullname = os.path.join(root, f)
                file_list.append(fullname)
    return file_list
  

def  add_java(code,file_name, model="maui_model"):
    
    java_splitter = RecursiveCharacterTextSplitter.from_language(
        language=Language.JAVA, chunk_size=1000, chunk_overlap=300
    )
    ts_docs = java_splitter.create_documents([code])
    c = chromadb.HttpClient(host='localhost', port=8000)
    chromadb.PersistentClient(path="c:/ma/openai/db/")
    collection = c.get_or_create_collection(model)
    id=1
    for rec in (ts_docs):
        
       collection.add(
           documents=rec.page_content,
      # filter on these!
          metadatas=[{"type": 'maui src', "name": file_name}],
          ids=[file_name+'-' + str(id) ]  # unique for each docui
          )
       id=id +1
       #print(rec)

          
def  add_txt(file_name):
    with open(file_name) as f:
        contents = f.read()
        add_java(contents,file_name,"maui_model")
        f.close    


def read_pdf(full_pathname):  

    loader = PyMuPDFLoader(full_pathname)              
    #"C:/ma/openai/upload/7.3.2_RiskConfidence_User_Guide.pdf")
    PDF_data = loader.load()  
    
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=200)
    all_splits = text_splitter.split_documents(PDF_data)
    return all_splits
 

def add_document(pdf_documents,file_name,model=kb_model):
    print("adding "+file_name+" to "+model) 
    c = chromadb.HttpClient(host='localhost', port=8000)
    chromadb.PersistentClient(path="c:/ma/openai/db/")

    collection = c.get_or_create_collection(model)
    id=1
    print("processing "+file_name)
    for rec in (pdf_documents):
        #print(rec)
        
        
        if rec.page_content.count('.')>500 :
            print("-skip")
        else:
            
            print("adding "+str(rec.metadata["page"]))
            collection.add(
                documents=rec.page_content,
           # filter on these!
               metadatas=[{"type": 'rfo document', "name": file_name, "section":rec.metadata["page"]}],
               ids=[file_name+'-' + str(id) ]  # unique for each docui
               )    
            id=id+1
           

def get_knowledge(question ,model=kb_model ):
    
    chromadb.PersistentClient(path="c:/ma/openai/db/")
    c = chromadb.HttpClient(host='localhost', port=8000)

    print("checking on "+model) 
    collection = c.get_or_create_collection(model)
    results = collection.query(
        query_texts=[ question],
        n_results=3#,
        # where={"kk": "notion"}, # optional filter
        # where_document={"$contains":"search_string"}  # optional filter
    )
    return json.dumps({" reference": results, "desc":question})


"""       
filename="C:/ma/openai/upload/7.1.6_RiskFoundation_Security_AdminConf_Guide.pdf"
doc=read_pdf("C:/ma/openai/upload/7.1.6_RiskFoundation_Security_AdminConf_Guide.pdf")
add_document(doc,filename)

results = collection.query(
    query_texts=[ "how to create task server pool "],
    n_results=3#,
    # where={"kk": "notion"}, # optional filter
    # where_document={"$contains":"search_string"}  # optional filter
)

print(results)

"""

""" 
a=findAllFile("C:/work73/warsso","java")
for rec in a:
    print(rec)
    add_txt(rec)
    """
c = chromadb.HttpClient(host='localhost', port=8000)
chromadb.PersistentClient(path="c:/ma/openai/db/")
collection = c.get_or_create_collection("maui_model")
results = collection.query(
    query_texts=[ "curityContextPersistenceFilter"],
    n_results=10#,
    # where={"kk": "notion"}, # optional filter
    # where_document={"$contains":"search_string"}  # optional filter
)

for rec in results["documents"]:
    print(rec)
    print("\n")