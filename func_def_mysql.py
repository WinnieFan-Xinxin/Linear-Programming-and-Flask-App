import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import xml
import pymysql
host   = 'nemodb.cnlzucvh0tgy.us-east-1.rds.amazonaws.com'
port   = 3306
user   = 'root'
passwd = 'fanxin521'
db     = 'NEMO'


# read data from db table to dataframe
# if you need 2+ variables as '*theRest', use (var1, var2,etc.)  
def DB_table_data(conn, sql, *theRest):
	cursor = conn.cursor()
	result = cursor.execute(sql,*theRest)
	fetchResult = cursor.fetchall()
	column = [x[0] for x in cursor.description]
	data=[]
	for row in fetchResult: data.append(tuple(row))
	array = np.array(data)
	df = pd.DataFrame.from_records(array, columns = column)
	return df
# insert df into MySQL database
def insert_df_into_db(col_name, tbl_name, df):
    data_tuple = [tuple(x) for x in df.to_records(index=False)]
    n = 16700
    sql_insert = "INSERT INTO "+ tbl_name + " (" + ','.join('`' + item + '`' for item in col_name) + \
            ") VALUES (" + ','.join(['%s'] * len(col_name)) + ")"
    print(sql_insert)
    pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
    pymysql.converters.conversions = pymysql.converters.encoders.copy()
    pymysql.converters.conversions.update(pymysql.converters.decoders)
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    cur = conn.cursor()
    if len(data_tuple) > n:
        for x in range(0, len(data_tuple), n):
            sub_data_tuple = data_tuple[x:x + n]
            cur.executemany(sql_insert,sub_data_tuple)
            conn.commit()
    else:
        cur.executemany(sql_insert,data_tuple)
        conn.commit()




