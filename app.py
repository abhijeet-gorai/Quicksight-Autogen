# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 22:35:56 2020

@author: Abhijeet
"""


from flask import Flask, render_template, request, redirect
import Quicksight

app = Flask(__name__)

@app.route('/')
def upload():
    return render_template("upload.html")

@app.route('/dashboard',methods=['POST'])
def dashboard():
    if request.method == 'POST':
        f = request.files['file']
        fn = f.filename
        f.save(fn)
        result = request.form
        dstype = result['dstype']
        path = Quicksight.make_dashboard(fn, dstype = dstype)
        return redirect(path)
    
if __name__ == '__main__':
    #app.debug = True
    app.run(host='127.0.0.1', port=5000)