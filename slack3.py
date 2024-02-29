import os
import json
# Use the package we installed

import time
from slack_sdk import  WebClient
import app2.dql as dql
import app2.vectordb2 as vdb1
# Initialize your app with your bot token and signing secret
import pandas as pd
import datetime 
import requests
import io

import speech_recognition as sr 
from ydata_profiling import ProfileReport, compare

test_channel_id="C06GRCN1FA8"
test_channel_name="help-ers-assistant" 
token = os.environ.get("SLACK_MCO_TEST_TOKEN")


 
r = sr.Recognizer() 
## get current time stamp
now = datetime.datetime.now()
ts=int(now.timestamp())
#ts=1706783786
print(ts)
# Add functionality here later
# @app.event("app_home_opened") etc.
conversation_history = []

client = WebClient(token=token)
conversation_history=  client.conversations_history(channel=test_channel_id,limit=10,oldest=ts)
result = conversation_history["messages"]
a2=conversation_history["messages"]
auth_test = client.auth_test()
bot_user_id = auth_test["user_id"]
print(bot_user_id)
kb_model="model3"
for i in range(6000):
    conversation_history=  client.conversations_history(channel=test_channel_id,limit=100,oldest=ts)
    #print(conversation_history)
    result = conversation_history["messages"] 
   
    if len(result)>0:
       for rec in  result:
        if ts<float(rec["ts"]) and ( "client_msg_id" in rec.keys() ):
            ts=float(rec["ts"])
            message=rec["text"]
            if "files" in rec :
                a2=rec
                rec_file_name=a2["files"][0]["name"]
                print(rec_file_name)
                url=a2["files"][0]["url_private_download"]
                print(url)
                if message.upper()=="DATA PROFILE"  :   

                    data=requests.get(url, headers={'Authorization': 'Bearer %s' % token}).content
                    df = pd.read_csv(io.StringIO(data.decode('utf-8')))
                    print(df.count()) 
                    #profile.to_file(profile_name)
                    profile = ProfileReport(df, title=rec_file_name+"Profiling Report")
                    
                    new_file = client.files_upload_v2(
                                           title=rec_file_name+"Profiling Report",
                                           filename=rec_file_name+".html",
                                           content=profile.to_html(),
                                       )   
                    
                    files = client.files_list(user=bot_user_id)
                    file_url = new_file.get("file").get("permalink")
                    response = client.chat_postMessage(
                       channel=test_channel_name,
                       text=f"RFO assistant :Here is the file: {file_url}",
                   )
                    print("send profile")
                    
                elif message.upper()=="LEARN" :
                    full_path="c:/ma/openai/upload/"+rec_file_name
                    data=requests.get(url, headers={'Authorization': 'Bearer %s' % token}).content
                    with open(full_path, "wb") as file:
                        file.write(data)
                    print("load "+full_path)
                    doc=vdb1.read_pdf(full_path  )
                    vdb1.add_document(doc,rec_file_name,kb_model)
                    response = client.chat_postMessage(channel=test_channel_name, text=rec_file_name+" is loaded into knowledge base")
            elif len(message)>3:
                
                ai_feedback=dql.run_conversation_dql2(message)
                time.sleep(1)
                if type(ai_feedback) is type(None)  :
                    response = client.chat_postMessage(channel=test_channel_name, text=" What can i do for you?")
                else:
                    response = client.chat_postMessage(channel=test_channel_name, text=" "+ai_feedback)
            elif len(message)<3:
                response = client.chat_postMessage(channel=test_channel_name, text=" What can i do for you?")
        else: #speech to text as input
            	try:
            		
            		# use the microphone as source for input.
            		with sr.Microphone() as source2:
            			
            			# wait for a second to let the recognizer
            			# adjust the energy threshold based on
            			# the surrounding noise level 
            			r.adjust_for_ambient_noise(source2,duration=2)
            			
            			#listens for the user's input 
            			audio2 = r.listen(source2)
            			
            			# Using google to recognize audio
            			MyText = r.recognize_google(audio2)
            			text=MyText.upper()
            			print(text)
            			respond=client.chat_postMessage(channel=test_channel_name, text=" " +text)
            			ai_feedback=dql.run_conversation_dql2(text) 
            			if type(ai_feedback) is type(None):
            			  print("no feedback")
            			else:
            			  response = client.chat_postMessage(channel=test_channel_name, text=" "+ai_feedback)
            			 
            	except sr.RequestError as e:
            		print("Could not request results; {0}".format(e))
            		
            	except sr.UnknownValueError:
            		print("unknown error occurred")

           
    time.sleep(2)

 