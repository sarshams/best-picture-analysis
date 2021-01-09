# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 17:32:33 2019

@author: sshamsie
"""

import pandas as pd
import glob
import requests
import json
import sqlite3
from sqlite3 import Error

def create_df(response, cols):
#    cols = ['id', 'runningTimeInMinutes', 'title', 'titleType', 'year']
    r = {k: response[k] for k in response.keys() & cols}
    r_df = pd.DataFrame([r])
    return r_df

def search_movie(url, key, host, title):
    headers = {
            'x-rapidapi-host': host,
            'x-rapidapi-key': key,
        }
    
    #create empty dataframe that we will add new movies to
    cols = ['id', 'runningTimeInMinutes', 'title', 'titleType', 'year']
    df = pd.DataFrame(columns=cols)
    
    #create list to hold missing titles
    miss = []

    for t in range(len(title)):
        querystring = {'q':title[t]}
        response = requests.request('GET', url, headers=headers, params=querystring)
        
        #find the best match returned
        if(response.status_code == 200):
            j = json.loads(response.content)
            if 'results' in j:
                best_match = j['results'][0]
#                res_check = j['results']
#                for i in res_check:
##                    if i['title'] == title[t] and i['titleType'] == 'movie':
#                    if i['titleType'] == 'movie':
#                        best_match = i
#                        break     
#                        print(best_match)
                #create dataframe row from best_match and append to running df
                r_df = create_df(best_match, cols)
                df = df.append(r_df, sort='True')
            else:
                miss.append(title[t])
                
        else:
            miss.append(title[t])
    return df, miss

def get_release(url, key, host, mov_id):
    headers = {
            'x-rapidapi-host': host,
            'x-rapidapi-key': key,
        }
    
    #create empty dataframe that we will add new movies to
    cols = ['movie_id', 'date', 'premiere', 'region', 'wide']
    df = pd.DataFrame(columns=cols)
    
    #create list to hold missing titles
    miss = []

    for i in range(len(mov_id)):
        querystring = {'tconst':mov_id[i].split('/')[2]}
        response = requests.request("GET", url, headers=headers, params=querystring)
        
        if(response.status_code == 200):
            j = json.loads(response.content)
            for r in j:
                if r['region'] == 'US' and r['wide'] == True:
                    rel_match = r
                    break
                    print(rel_match)
            
            #create dataframe row from rel_match and append to running df
            r_df = create_df(rel_match, cols)
            r_df['movie_id'] = mov_id[i]
            df = df.append(r_df, sort='True')
            
        else:
            miss.append(mov_id[i])           
    return df, miss

def get_config_params(src_dir ='./assets/', config_file='config.json'):
    with open(src_dir + config_file) as f:
        return json.load(f)

def create_db(db_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


#read in local data
files = glob.glob('data/*.csv')
movs = pd.concat([pd.read_csv(f) for f in files])
mov_date = movs[(movs['year_film']>=2000) & (movs['year_film']<=2018)]
bp = list(mov_date[mov_date['category'].isin(['BEST PICTURE'])]['film'])

#read from API
id_url = 'https://imdb8.p.rapidapi.com/title/find'
rel_url = 'https://imdb8.p.rapidapi.com/title/get-releases'
RAPIDAPI_KEY  = '2d15c8a065msh1412c1ad1e06ff2p115d9ajsn814f3beb14cd'
RAPIDAPI_HOST = 'imdb8.p.rapidapi.com'

#initialize database
config_params = get_config_params()
db_filepath = config_params['database path']

#get movie ids and any ids that failed the api run
#mov_tup = search_movie(id_url, RAPIDAPI_KEY, RAPIDAPI_HOST, bp)
id_df = mov_tup[0]

#write movie ids to the database
with sqlite3.connect(db_filepath) as conn:
#    id_df.to_sql('movies', conn, if_exists='append', index=False)
    mov_ids = list(pd.read_sql('SELECT id FROM movies WHERE year <2010', conn)['id'])
    
#get movie release information
#rel_tup = get_release(rel_url, RAPIDAPI_KEY, RAPIDAPI_HOST, mov_ids)
rel_df = rel_tup[0]
rel_df = rel_df.reset_index().drop(columns='index', axis=1)
rel_df['date'] = pd.to_datetime(rel_df['date']).dt.date

with sqlite3.connect(db_filepath) as conn:
#    rel_df.to_sql('release_info', conn, if_exists='append', index=False)
    movies = pd.read_sql('SELECT * from movies', conn)

#get oscar date
bp_df = mov_date[mov_date['category']=='BEST PICTURE']
bp_df = pd.merge(movies,bp_df, left_on='title', right_on='film')
bp_df = bp_df.drop(columns=['year_film', 'film'], axis=1)

with sqlite3.connect(db_filepath) as conn:
    bp_df.to_sql('movies', conn, if_exists='replace', index=False)


##edge cases
#miss = ['Moulin Rouge', 'The Queen', 'Precious']
#mov_tup_e1 = search_movie(id_url, RAPIDAPI_KEY, RAPIDAPI_HOST, ['The Queen'])
    
