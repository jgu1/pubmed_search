#all the imports

import pdb
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash,send_file
import esearch_fetch_parse
from contextlib import closing
from time import sleep
from flask.ext.paginate import Pagination
import re
import socket
from datetime import datetime,timedelta
from db_classes import *
from wordcloud import WordCloud
import os
import tempfile
# configuration
DATABASE = '/Users/jialianggu/WorkSpace/job_10_19/pubmed_search/pubmed_cache.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME_PASSWORD_DICT={'hao':'genome','jiashun':'genome','erxin':'genome','jun':'genome','yanqiu':'genome','jialiang':'genome'}
PAPER_PER_PAGE=10


app = Flask(__name__)
app.config.from_object(__name__)

#app = Blueprint('papers',__name__)

def connect_db():
    db =  sqlite3.connect(app.config['DATABASE'])
    #db.text_factory = str
    return db

@app.route('/get_wordCloud_img')
def get_wordCloud_img():
    temp_img_name = session['temp_img_name'] 
    #return send_file(filename, mimetype='image/png')
    response = app.make_response(send_file(temp_img_name, mimetype='image/png'))

    response.headers.add('Last-Modified', datetime.now())
    response.headers.add('Cache-Control', 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0')
    response.headers.add('Pragma', 'no-cache')
    return response
'''
def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('create_pubmed_cache.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()
'''
@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

def highlight_search_terms(abstract, search_term):
    terms = re.split('\+|AND|OR',search_term)
    terms = filter(bool,terms)
    for term in terms:
        abstract = abstract.replace(term,'<mark>'+term+'</mark>')
    return abstract

@app.route('/')
def show_papers():
   
    if 'disease' not in session or 'genes_included' not in session:
        return render_template('show_papers.html')

 
    disease = session['disease']
    genes_included = session['genes_included']
    genes_excluded = session['genes_excluded']

    dao = DAO(DATABASE)
    all_papers,count_dict = dao.fetch_search_terms(disease,genes_included,genes_excluded)
  
    #begin pagination 
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1

    PAPER_PER_PAGE= app.config['PAPER_PER_PAGE'] 
    papers_for_this_page = all_papers[(page-1)*PAPER_PER_PAGE:page*PAPER_PER_PAGE] 
   
    abstract_txt = '' 
    for paper in papers_for_this_page:
        abstract = paper['abstract']
        if abstract is not None:
            paper['abstract'] = highlight_search_terms(abstract,paper['search_term'])
            abstract_txt = abstract_txt + abstract
    
    if 'temp_img_name' in session:  # each user session will occupy only one random file, not leaking disk space
        old_img_name = session['temp_img_name']
        os.remove(old_img_name)

    wordCloud = WordCloud(max_font_size=60).generate(abstract_txt)
    temp_img_name = tempfile.NamedTemporaryFile().name  + '.png'
    wordCloud.to_file(temp_img_name)
    session['temp_img_name'] = temp_img_name
     
    pagination = Pagination(page=page, total=len(all_papers), per_page=PAPER_PER_PAGE, record_name='papers')
    #end pagination
    return render_template('show_papers.html',papers=papers_for_this_page,count_dict=count_dict,pagination=pagination)


def parse_web_search_term(web_search_term_disease,web_search_term_genes_included,web_search_term_genes_excluded):
    disease = web_search_term_disease.strip()
    genes_included = web_search_term_genes_included.strip().split()
    genes_excluded = web_search_term_genes_excluded.strip().split()
    return disease,genes_included,genes_excluded

@app.route('/search', methods=['POST'])
def search():
 
    if not session.get('logged_in'):
        abort(401)
    
    web_search_term_disease = request.form['disease']
    web_search_term_genes_included  = request.form['genes_included']
    web_search_term_genes_excluded = request.form['genes_excluded']
    
    if web_search_term_disease is None or web_search_term_genes_included is None:
        return render_template('show_papers.html')
   
    disease,genes_included,genes_excluded = parse_web_search_term(web_search_term_disease,web_search_term_genes_included,web_search_term_genes_excluded)

    dao = DAO(DATABASE)
    dao.pop_db(disease,genes_included, genes_excluded)
 
    session['disease']=disease
    session['genes_included']=genes_included
    session['genes_excluded']=genes_excluded

    return redirect(url_for('show_papers'))

@app.route('/choose_term', methods=['GET'])
def choose_term():
    gene=request.args.get('gene', 1)
    session['genes_included']=[gene]
    return redirect(url_for('show_papers'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] not in app.config['USERNAME_PASSWORD_DICT']:
            error = 'Invalid username'
        else:
            session['logged_in'] = True
            flash('You were logged in')
            return redirect(url_for('show_papers'))
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_papers'))

if __name__ == '__main__':
    ip_for_current_machine = socket.gethostbyname(socket.gethostname())
    app.run(host='localhost',port=15213,threaded=True)
    #app.run(host=ip_for_current_machine,port=15213,threaded=True)


