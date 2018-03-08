# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 11:22:56 2017

@author: ABHAY RANJAN
"""
from alerts import alerts
import collections
import csv
from csv import reader
import datetime
import filter_words
import json
import logging
import numpy as np
import os
import pandas as pd
import paramiko
import pymysql
import re
import sqlalchemy
from sqlalchemy import create_engine
import sys
from stop_words import get_stop_words  


try:
    start_directory = os.getcwd()
except Exception as e:
    clean_environment([],e,"Job stopped because unable to get the current working directory",send_mail,True,True,file_path,error_path)

## Define the possible paths
util_directory = start_directory.replace("wordcloud_dataset","master")

##Load Utility Functions
try:
        logging.basicConfig(filename = "word_cloud.log",level = logging.DEBUG)
        os.chdir(util_directory)
        cfg = json.loads(open('configure.json').read())
        exec(open("automation_utils.py").read())
        os.chdir(start_directory)
        send_mail = alerts(cfg)
        job_path = sys.argv[1]
        input_path = job_path+"INPUT"
        files = [f for f in os.listdir(input_path)]
        data_name = files[0]
        txt = open(input_path + "/" + data_name)
        data = txt.read()
        input_dict = json.loads(data)
        p = re.compile('tra/(.*).filepart')
        inputFilePath = re.search(p,input_dict['filePath'])
        logging.info("inputFilePath: "+str(inputFilePath))
        if inputFilePath is None:
            p = re.compile('tra/(.*)')
            inputFilePath = re.search(p,input_dict['filePath'])
            logging.info(inputFilePath)
            file_path=inputFilePath.group(1)
            file_name = file_path.split('/')[1]
            logging.info(file_path)
            tenant_name = file_path.split('/')[0]
except Exception as e:
        clean_environment([],e,"Unable to initialize the utility modules for the job to run",send_mail)
        
# Archive path: move file afer the flow ran successfully
# Error path: move file whenever the flow breaks
archive_path="/"+tenant_name+"/processed"+'/'+file_name+'_'+datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S") 
error_path="/"+tenant_name+"/failed"+'/'+file_name+'_'+datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")

#------------------------------#
# -*- SFTP & DB connection -*- #
#------------------------------#
# SFTP connection
try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(cfg['SFTP_HOST'],username= cfg['SFTP_USER'], password = cfg['SFTP_PASS'], timeout=10000)
        sftp = ssh.open_sftp()
except Exception as e:
        clean_environment([connection],e,"Unable to connect to sftp",send_mail,True,True,file_path,error_path)  


def create_dataframe_from_file(file_name):
    try:
        #remote_file = sftp.open(file_name)
        sftp.get(file_name,job_path+file_name)
        os.chdir(job_path)
        raw_file_df = robust_data_loader(file_name)
        os.chdir(start_directory)
        return raw_file_df
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to create an engine to the database",send_mail,True,True,file_path,error_path)

        ## Robust Data Loaded of file to Pandas Dataframe
def robust_data_loader(input_file_name):
    try:
        data = pd.read_csv(input_file_name, encoding = 'utf-8')
        logging.info(input_file_name + "read successfully!") 
    except:
        try:
            data = pd.read_csv(input_file_name, encoding = 'latin1')
            logging.info(input_file_name + " read successfully!") 
        except:
            try:
                data = pd.read_csv(input_file_name, encoding = 'iso-8859-1')
                logging.info(input_file_name + " read successfully!") 
            except:
                try:
                    data = pd.read_csv(input_file_name, encoding = 'cp1252')
                    logging.info(input_file_name + "read successfully!") 
                except:
                    try:
                        data = pd.read_csv(input_file_name, encoding = 'Windows-1252')
                        logging.info(input_file_name + "read successfully!")
                    except Exception as e:
                        clean_environment([],e,"Job stopped because unable to load the dataframe",send_mail,True,True,file_path,error_path)          
    return data
def create_word_dataframe(cleaned_dataframe):
    try:
        thekey = cleaned_dataframe.composite_key_index.unique()
        word_df = pd.DataFrame(columns=['Word','Count','hospital_id','record_type','encounter_type','coc_1','coc_2','composite_key_index'])
        for keyit in thekey:
            cleaned_dataframe_with_matching_key = cleaned_dataframe[cleaned_dataframe["composite_key_index"] == keyit]
            comp_key = keyit
            word_lst = collections.Counter(" ".join(cleaned_dataframe_with_matching_key[cfg['DESCRIPTION_COLS'][0]]).split()).most_common(200)
            composite_srl = comp_key.split("_")
            for word_index in range(len(word_lst)):
                most_common_word_and_occurence = list(word_lst[word_index])
                for pos in range(len(composite_srl)):
                    most_common_word_and_occurence.insert(pos+2,composite_srl[pos])
                most_common_word_and_occurence.insert(len(composite_srl)+2,comp_key)
                most_common_word_and_occurence = tuple(most_common_word_and_occurence)
                word_lst[word_index] = most_common_word_and_occurence
            intermediate_df = pd.DataFrame(word_lst,columns=['Word','Count','hospital_id','record_type','encounter_type','coc_1','coc_2','composite_key_index'])
            word_df = pd.concat([word_df,intermediate_df])
        return word_df
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to create a word dataframe",send_mail,True,True,file_path,error_path)
        
def create_engine_using_sqlalchemy():
    try:
        db_host_name = "riskid_"+tenant_name
        mysql_engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(cfg['DB_USER'],cfg['DB_PASSWORD'],\
                                     str(cfg['DB_HOST'])+":"+str(cfg['DB_PORT']),db_host_name),echo=False)
        return mysql_engine
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to create an engine to the database",send_mail,True,True,file_path,error_path)
        

def insert_into_database(final_df):
    try:
        engine = create_engine_using_sqlalchemy()
        final_df.to_sql(name=cfg['EVENTS_WORD_CLOUD'],con=engine,if_exists='append',index=False)
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to insert into the database",send_mail,True,True,file_path,error_path)
        

def dataframe_cleaner(df, special_characters):
    try:
        df[cfg['DESCRIPTION_COLS'][0]] = df[cfg['DESCRIPTION_COLS'][0]].apply(lambda x: string_cleaner(x,[]))
        return df
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to clean the description field of dataframe",send_mail,True,True,file_path,error_path)
    
     
     
def string_cleaner(text, special_characters):
    try:
       if isinstance(text,(str,list,dict,tuple)):
           for k, v in spec_char:
               text = text.replace(k, v)
           text = text.upper()
           text = re.sub(' +',' ',text)  
           return text.strip('\t\n\r -')
       else:
          return text
       
    except Exception as e:  
       clean_environment([],e,"Job stopped because unable to clean the description text in string cleaner function",send_mail,True,True,file_path,error_path)
    
   
   
       
spec_char = [('.',' '), ('!',' '), ('?',' '), (',',' '), ("'",' '), ("@",' '), ("#",' '), 
             ("$",' '), ("%",' '), ("\n",' '), ("^",' '), ("&",' '), ("*",' '), ("(",' '), 
             (")",' '), ("=",' '), ("_",' '), ("<",' '), (">",' '), (":",' '), (";",' '), 
              ("[",' '), ("]",' '), ("\t",' '), ("{",' '), ("}",' '), ("|",' '), ('"',' '), 
             ]
 ## For the entire dataframe    
def df_mapper(df_pre_map,col_name):
    try:
        df = df_pre_map.copy() 
        num_rows = df.shape[0]
        df['Description'] = df['Description'].apply(update_description)
        return df
    except Exception as e:
        clean_environment([],e,"Job stopped because unable to clean the description text in df_mapper function",send_mail,True,True,file_path,error_path)


def update_description(text_to_examine):
    try:
        if ' ' in str(text_to_examine):
                mapped_text = string_mapper(text_to_examine)
        return mapped_text
    except Exception as e:
         clean_environment([],e,"Update Description:Job stopped because unable to clean the description text in string mapper",send_mail,True,True,file_path,error_path)
        
## For arbitrary strings
def string_mapper(input_string):
    try:
        input_string = input_string.rstrip().lstrip()
        input_string = re.sub(' +',' ',input_string)
        input_string = ' '.join(input_string.split())
        tokens = input_string.split(' ')
        return ' '.join([token_mapper(x) for x in tokens])
    except Exception as e:
         clean_environment([],e,"Job stopped because unable to clean the description text in string mapper",send_mail,True,True,file_path,error_path)
     


 ## For Tokens
def token_mapper(token):
  been_mapped = 0
  try:
      if cfg['USE_ABBREV_MAPPING'] and been_mapped == 0:
          in_abbrev_table = token in list(abbreviated_raw_file_df['Abbrev'])
          if in_abbrev_table:
              been_mapped = 1      
              return str(abbreviated_raw_file_df[abbreviated_raw_file_df['Abbrev'] == token]['FullWord'].iloc[0])
      if been_mapped == 0:
         return token
  except Exception as e:
      clean_environment([],e,"Job stopped because unable to clean the description text in token mapper function",send_mail,True,True,file_path,error_path)
  
#-------------------------------#
# -*- Data Loading Function -*- #
#-------------------------------#
try:
    sys_dir = sftp.listdir()
    new_dir = "/"+tenant_name+"/"
    sftp.chdir(new_dir)
    raw_file_df = create_dataframe_from_file(file_name)
    ## Creating a dataframe of abbreviated words
    abbreviated_raw_file_df = create_dataframe_from_file(cfg['ABBREVIATIION_FILE_NAME'])
    ##Creating a dataframe of curse words
    cursewords_raw_file_df = create_dataframe_from_file(cfg['CURSEWORDS_FILE_NAME'])
    curse_words = list(cursewords_raw_file_df['Curse_Words'])
    cleaned_dataframe = dataframe_cleaner(raw_file_df, [])
    cleaned_dataframe = df_mapper(cleaned_dataframe,cfg['DESCRIPTION_COLS'][0])
    cleaned_dataframe['composite_key_index'] = cleaned_dataframe.apply(lambda x: str(x[cfg['KEY_COLUMNS'][0]])+"_"+\
    str(x[cfg['KEY_COLUMNS'][1]])+"_"+str(x[cfg['KEY_COLUMNS'][2]])+"_"+str(x[cfg['KEY_COLUMNS'][3]])+"_"+\
    str(x[cfg['KEY_COLUMNS'][4]]),axis=1)
    word_df = create_word_dataframe(cleaned_dataframe)
    ## Processing of stop words start from here
    stop_words = get_stop_words('en')
    stop_words = stop_words + list(filter_words.MORE_WORDS) + list(filter_words.CURSE_WORDS)[0]
    stop_words = [x.upper() for x in stop_words]
    word_df_filtered = word_df[~word_df['Word'].isin(stop_words)]     
    ##Remove words with no alpha characters    
    word_df_filtered = word_df_filtered[word_df_filtered['Word'].str.contains('|'.join(cfg['ALPHASET']))] 
    final_df = word_df_filtered.groupby('composite_key_index').head(cfg['NUMBER_OF_WORDS']).reset_index(drop=True)
    insert_into_database(final_df)
    send_mail.sendSuccessAlerts("Successfully updated the word cloud data table")
except Exception as e:
    clean_environment([ssh,sftp],e,"Unable to insert word cloud data into the database",send_mail,True,True,file_path,error_path)   
    
    
    