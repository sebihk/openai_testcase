from flask import Flask, request
from flask import   render_template
import json
from flask_cors import CORS 
from openai import OpenAI
import  oracledb
import json
import os
import sys
from flask import Flask, render_template, request, url_for, flash, redirect
sys.path.append('./')
from  dql import *

#client = OpenAI(api_key='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')
client= OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
#app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)


app = Flask(__name__)

messages = [ 
            ]

@app.route('/')
def index():
    return render_template('index.html', messages=messages)

@app.route('/create/', methods=('GET', 'POST'))
def create():
    if request.method == 'POST':
        title = request.form['title']
        print(title)
        content = request.form['content'] 
        if title=='dql':
            messages.append({'title': content, 'content': run_conversation(content)})
            print(title)
        elif  title=='nlpsql':
            messages.append({'title': content, 'content': 'not ready yet'})
        return redirect(url_for('index'))

    return render_template('create.html')
    
if __name__=="__main__":
    print(app.root_path)
    app.run(host="0.0.0.0",port=5000,debug=False)
   