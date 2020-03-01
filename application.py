from flask import Flask, render_template, flash, redirect, url_for, session, request, logging,jsonify
from wtforms import Form, StringField, TextAreaField, PasswordField, validators, SelectField,SubmitField,DateTimeField,HiddenField,RadioField
from wtforms.validators import DataRequired
from wtforms.validators import DataRequired
from flask_wtf.file import FileField,FileAllowed, FileRequired
from flask_wtf import FlaskForm
from flask_bootstrap import Bootstrap
import numpy as np
import pandas as pd
import datetime
from werkzeug.utils import secure_filename
import os, sys
import xml.etree.ElementTree as ET
import xml
from pandas import ExcelWriter
# import xlwings as xw
import socket
import ipaddress
import pymysql
import pythoncom


from functools import reduce

from func_def_mysql import *

from nemo_basic import run_model
from nemo_with_max_flow import run_model_max

from Yearly_model_with_sto_noMaxflow import run_model_sto
from Yearly_model_with_sto_Maxflow import run_model_sto_max

from Yearly_model_with_exp_noMaxflow import run_model_exp
from Yearly_model_with_exp_Maxflow import run_model_exp_max

from Yearly_model_with_StoExp_noMaxflow import run_model_StoExp
from Yearly_model_with_StoExp_Maxflow import run_model_StoExp_max


host_name = socket.gethostname()

ip = socket.gethostbyname(socket.gethostname())


application = app = Flask(__name__)

#---------------------------------------------------------------
# Connect to database
host   = 'nemodb.cnlzucvh0tgy.us-east-1.rds.amazonaws.com'
port   = 3306
user   = 'root'
passwd = 'fanxin521'
db     = 'NEMO'

conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

#---------------------------------------------------------------
#- Path to save files in order to check the possible issues
path = '//hou-file1/woodmac$/Winnie/NeMo_v1/'
file_path = '//hou-file1/woodmac$/Winnie/NeMo_v1/uploaded_files/'
path_save_delete = '//hou-file1/woodmac$/Latin America Markets/MeMo_Saved_Inputs_Outputs_for_Deleted_Cases/'
#---------------------------------------
# Home page
@app.route('/', methods=['GET', 'POST'])
def index():

    return render_template('home_case.html')
#-------------------------------------
# New case page
## new case form
class newcase(FlaskForm):
        # Form fields
        choices = [("DEV","DEV")]
        
        branchfromcase = SelectField('Branch from Existing Case:', choices=choices)
        casename = StringField('Case Name:')
        date = DateTimeField('Date:', format="%Y-%m-%d-T%H:%M:%S",default=datetime.datetime.now,validators=[validators.DataRequired()])
        comment = TextAreaField ('Memo:')

## page setup
@app.route('/new_case', methods=['GET', 'POST'])
def new_case():
    """
    #Add a new case
    """
    form = newcase()
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    # dynamic existing case
    cur = conn.cursor()
    caseinfo = cur.execute("SELECT * FROM case_info ")
    caseinfos = cur.fetchall()
    case_id_list = list()

    for i in range(len(caseinfos)):
        element = caseinfos[i][-3]
        if element is not None:
            case_id_list.append(caseinfos[i][-3])

    unique_case_id = sorted(list(set(case_id_list)))

    choices=[(i, i) for i in unique_case_id] 

    form.branchfromcase.choices = choices

    if request.method == 'POST':
        if form.validate_on_submit():
            # get Form Fields
            name = form.casename.data
            date = form.date.data
            comment = form.comment.data
            branch_case = form.branchfromcase.data

            if name:
                # generate a name for new case
                date1 = date.strftime('%Y-%m-%d-%H:%M:%S')
                new_case_name = str(date1)+'-'+str(name)
                print(new_case_name)
                # create cursor
                cursor=conn.cursor()
                # get existing case information
                result =cursor.execute("SELECT * FROM case_info WHERE case_id =%s", new_case_name)
                results=cursor.fetchall()

                if results:
                    flash ('This case is existing, please refresh to create a new one or go to update the existing case','warning')
                else:
                    # This case does not exist in database and then copy input data from existing case
                    # Insert new case into information table of db
                    startTime = datetime.datetime.now()
                    print(branch_case)
                    name = str(name)
                    time = str(date)
                    info = str(comment)
                    cursor.execute("INSERT INTO case_info (name, comment, case_id) VALUES(%s, %s, %s)", (name, info, new_case_name))
                    conn.commit()
                    cursor = conn.cursor()
                    cursor.execute("UPDATE case_info as t, (SELECT start_year,end_year FROM case_info WHERE case_id=%s) AS t1 SET t.start_year = t1.start_year, t.end_year = t1.end_year  where t.case_id = %s",(str(branch_case),new_case_name))
                    conn.commit()
                    flash ('New case created'+':'+' '+new_case_name,"info")
     
                    # grab data from an existing case
                    old_case = str(branch_case)
                    # create cursor
                    cursor = conn.cursor()
                    #-----------------------------------------------------------------------------
                    #- New Supply input data
                    # execute query: insert into supply capacity table
                    cursor.execute("INSERT INTO tbl_NEMOI_Supply_Capacity (Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, capacity, date, case_id, topology) SELECT Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, capacity, date,%s, topology FROM tbl_NEMOI_Supply_Capacity WHERE case_id=%s ",(new_case_name,old_case))
                    #cursor.close()
                    conn.commit()

                    # execute query: insert into supply cost table 
                    cursor.execute("INSERT INTO tbl_NEMOI_Supply_Cost (Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, cost, date, case_id, topology) SELECT Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, cost, date,%s, topology FROM tbl_NEMOI_Supply_Cost WHERE case_id=%s ",(new_case_name,old_case))
                    #cursor.close()
                    conn.commit()
                    #-----------------------------------------------------------------------
                    #- for arc input data, just duplicate everything from the existing case
                    #cursor=conn.cursor()
                    # execute query: insert into arc pipeline infrastructure table
                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Pipeline_Infrastructure (Unique_From_Hub_ID,Unique_To_Hub_ID, from_hub, to_hub, arc_name, online_date, ramp_up_months, capacity, comments, topology,case_id ) SELECT Unique_From_Hub_ID,Unique_To_Hub_ID, from_hub, to_hub, arc_name, online_date, ramp_up_months, capacity, comments, topology,%s FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id=%s ",(new_case_name,old_case))
                    #cursor.close()
                    conn.commit()

                    #cursor=conn.cursor()
                    # execute query: insert into arc tariffs table
                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Tariffs (Unique_From_Hub_ID, Unique_To_Hub_ID, from_hub, to_hub, cost_pesoGJ, date, topology, case_id, arc_name) SELECT Unique_From_Hub_ID, Unique_To_Hub_ID, from_hub, to_hub, cost_pesoGJ, date, topology, %s, arc_name FROM tbl_NEMOI_Arc_Tariffs WHERE case_id=%s ",(new_case_name,old_case))
                    #cursor.close()
                    conn.commit()

                    #cursor=conn.cursor()
                    # execute query: insert into arc constraints (min) table
                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Constraints (Unique_From_Hub_ID,Unique_To_Hub_ID,from_hub,to_hub,arc_name,data_type,comments,case_id,topology,`year`,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,`Dec`) SELECT Unique_From_Hub_ID,Unique_To_Hub_ID,from_hub,to_hub,arc_name,data_type,comments,%s,topology,`year`,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,`Dec` FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s ",(new_case_name,old_case))
                    #cursor.close()
                    conn.commit()

                    #-------------------------------------------------------------------------------
                    #- New Demand
                    cursor.execute("INSERT INTO tbl_NEMOI_Demand (Unique_Node_ID,Unique_Hub_ID,node,hub,demand,date,case_id, topology,state) SELECT Unique_Node_ID,Unique_Hub_ID,node,hub,demand,date,%s, topology,state FROM tbl_NEMOI_Demand WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    #- storage constraints
                    cursor.execute("INSERT INTO tbl_NEMOI_Storage_Constraints (Unique_Sto_ID,sto_facility ,storing_cost,max_injection,max_extraction,max_sto_capacity,min_sto_capacity,date,case_id,topology) SELECT Unique_Sto_ID,sto_facility ,storing_cost,max_injection,max_extraction,max_sto_capacity,min_sto_capacity,date,%s,topology FROM tbl_NEMOI_Storage_Constraints WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    #- storage injection

                    cursor.execute("INSERT INTO tbl_NEMOI_Storage_Injection (Unique_Hub_ID,Unique_Sto_ID ,hub,sto_facility,inj_cost ,date ,topology,case_id) SELECT Unique_Hub_ID,Unique_Sto_ID ,hub,sto_facility,inj_cost ,date ,topology,%s FROM tbl_NEMOI_Storage_Injection WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    #- storage extraction

                    cursor.execute("INSERT INTO tbl_NEMOI_Storage_Extraction (Unique_Sto_ID,Unique_Hub_ID,sto_facility,hub,ext_cost,date,topology,case_id) SELECT Unique_Sto_ID,Unique_Hub_ID,sto_facility,hub,ext_cost,date,topology,%s FROM tbl_NEMOI_Storage_Extraction WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    #- export price
                    cursor.execute("INSERT INTO tbl_NEMOI_Export_Price (Unique_Hub_ID, Unique_ExpNode_ID, hub,node,FOB_price,date,case_id,topology) SELECT Unique_Hub_ID, Unique_ExpNode_ID, hub,node,FOB_price,date,%s,topology FROM tbl_NEMOI_Export_Price WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    #- export capacity
                    cursor.execute("INSERT INTO tbl_NEMOI_Export_Capacity (Unique_Hub_ID, Unique_ExpNode_ID, hub,node,capacity,date,case_id,topology) SELECT Unique_Hub_ID, Unique_ExpNode_ID, hub,node,capacity,date,%s,topology FROM tbl_NEMOI_Export_Capacity WHERE case_id=%s",(new_case_name,old_case))
                    conn.commit()

                    timeElapsed = datetime.datetime.now()-startTime
                    flash('Time elapsed for creating a new case (hh:mm:ss.ms):'+ ' '+ str(timeElapsed),'info')

                    
            else:
                flash("Case name is needed",'danger')


    return render_template('new_case.html',form=form)
#-------------------------------------------------------
# Upload input files to a case
class selectcase(FlaskForm):

        #choices = [("","---")]
        # Form fields
        select = SelectField('Select Case to Update:')

        topologies = [
                 ("Mexico","Mexico"),
                 ("Southern Cone","Southern Cone")
                ]
        topology = RadioField('Select Topology', choices = topologies)
        file_hub = FileField('Upload HUB DEFINITION File:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])
        file_sup = FileField('Upload SUPPLY File:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])
        file_arc = FileField('Upload INFRASTRUCTURE file:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])
        file_demand = FileField('Upload DEMAND File:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])
        file_sto = FileField('Upload STORAGE File:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])
        file_exp = FileField('Upload EXPORT File:', validators=[FileAllowed(['xlsx','csv','xlsm','xls'])])

@app.route('/update_input', methods=['GET', 'POST'])
def updateinput():
    form = selectcase()
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    # create cursor
    cursor = conn.cursor()

    # get all data from a table
    results = cursor.execute("SELECT * FROM case_info ")

    alldata = cursor.fetchall()

    # create a list for case_id
    case_id_list=list()

    for i in range(len(alldata)):
        element = alldata[i][-3]
        if element is not None:
            case_id_list.append(alldata[i][-3])

    #case_id_list.append(tuple(("","---")))

    unique_case_id=sorted(list(set(case_id_list)))
    #unique_case_id.remove('DEV')

    choices=[(i, i) for i in unique_case_id] 
    
    form.select.choices = choices

    if request.method == 'POST' and form.validate_on_submit():
        # get Form Fields
        
        case_id = form.select.data

        topology = form.topology.data

        file_hub = form.file_hub.data

        file_sup = form.file_sup.data

        file_arc = form.file_arc.data
 
        file_demand = form.file_demand.data
     
        file_sto = form.file_sto.data

        file_exp = form.file_exp.data

        if case_id != "":

            sql = """SELECT * FROM case_info WHERE case_id =%s"""
            data_for_case_df = DB_table_data(conn, sql, str(case_id))
            #supplycap_df.to_excel('supplycapdf.xlsx',sheet_name='supplycap',index=False)
            # check the number of records for this case id
            nrecords=len(data_for_case_df)
            if nrecords>1:
                flash ('There are:'+nrecords+' records for this case in database','danger')

            else:
                case_name = data_for_case_df.iloc[0,0]
                comment = data_for_case_df.iloc[0,1]
                case_id = data_for_case_df.iloc[0,2]
                start = data_for_case_df.iloc[0,3]
                end = data_for_case_df.iloc[0,4]
                #flash('Case Name'+':'+case_name,"info")
                #flash('Comment'+':'+comment,"info")
                #flash('Case ID'+':'+case_id,"info")  
                startTime = datetime.datetime.now()
                #-
                
                if case_id !='DEV' and '2019' in case_id:

                    case_sp = case_id.split('-')
                    year = case_sp[0]
                    month = case_sp[1]
                    day = case_sp[2]
                    caseName = case_sp[4]
                    case_id1 = year + '-' + month + '-' + day + '_' + caseName

                elif case_id !='DEV' and '2019' not in case_id:
                    
                    case_sp = case_id.split(' ')
                    case_id1 = case_sp[0]
                    case_id2 = case_sp[1]
                    caseName = case_id1
                else:
                    case_id1 = case_id
                    caseName = case_id
                
                writer_upload = pd.ExcelWriter(path + case_id1 +'-FileUpload_Info.xlsx', engine='xlsxwriter')

                if file_hub:
                    temp_hub = str(caseName) + '_' + str(topology) + '_hub_temp.xlsx'
                    if os.path.exists(file_path + temp_hub):

                        os.remove(file_path + temp_hub)
                    
                    #- save the file uploaded by user to remote drive 
                    filename_hub = secure_filename(file_hub.filename)
                    file_hub.save(os.path.join(file_path, filename_hub))

                    #- read workbook, change the calculation status
                    #- and save and close
                    pythoncom.CoInitialize()
                    app_hub = xw.App(visible=False)
                    wb_hub = xw.Book(file_path + filename_hub)
                    wb_hub.app.display_alerts = False
                    wb_hub.app.screen_updating = False
                    wb_hub.app.calculation = 'automatic'
                    wb_hub.save()
                    wb_hub.close()
                    app_hub.kill()

                    #- rename the file in remote drive
                    os.rename(file_path + filename_hub, file_path + temp_hub)
                    
                    #- read excel file into dataframe format
                    xlsx_hub = pd.ExcelFile(file_path + temp_hub)

                    hub_def_file = pd.read_excel(xlsx_hub, sheet_name = 'nemo_hub_defs')
                    hub_def_file = hub_def_file.replace("'","_", regex=True)

                    hub_def_file.columns = [col.strip() for col in hub_def_file.columns]
                    hub_def_file['hub'] = hub_def_file['hub'].str.strip()
                    #- filt the hubs based on selected topology on the web
                    hub_def_file1 = hub_def_file[hub_def_file['topology']==topology]
                    if len(hub_def_file1) >0:

                        #- compare hub file with hub table in db
                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_def_hub_df = DB_table_data(conn, sql, topology)
                        #--
                        if len(db_def_hub_df) == 0:
                            hub_def_file1 = hub_def_file1[['hub', 'hub_report_name', 'ggm_hub', 'country',
                                                      'state','gas_region', 'power_region', 'super_hub', 'topology']]
                            
                            # insert dataframe into db
                            col_name = list(hub_def_file1.columns)
                            tbl_name = 'tbl_NEMOI_Hub_Definition'
                            insert_df_into_db(col_name, tbl_name, hub_def_file1)


                        else:
                            hub_file_db = pd.merge(hub_def_file1[['hub','topology']],db_def_hub_df[['hub','topology']],
                                                  on=['hub','topology'],how='outer',indicator=True)

                            hub_file_only = hub_file_db[hub_file_db['_merge']=='left_only'][['hub','topology']]

                            hub_db_only = hub_file_db[hub_file_db['_merge']=='right_only'][['hub','topology']] 

                            if len(hub_file_only)>0:
                                # if there are new hubs in hub excel file, filter these hubs and insert into db
                                new_hub_in_file = pd.DataFrame()

                                for i in range(len(hub_file_only)):
                                    temp_df = hub_def_file1[hub_def_file1['hub']==str(hub_file_only.iloc[i,0])]
                                    new_hub_in_file = new_hub_in_file.append(temp_df)

                                new_hub_in_file.to_excel(writer_upload, sheet_name = 'Hub_in_H_defs_file_notDB')
                                flash('New hubs found from hub definition file and added into database','warning')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                                
                                new_hub_in_file = new_hub_in_file[['hub', 'hub_report_name', 'ggm_hub', 'country', 
                                                              'state','gas_region', 'power_region', 'super_hub', 'topology']]
                                
                                for idx, row in new_hub_in_file.iterrows():
                                    insertList = row.tolist()
                                    insertString = ""
                                    for item in insertList:
                                        if insertString:
                                            insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                        else:
                                            insertString = "\'" + item + "\'"
                                        print(insertString)
                                    cursor.execute("""insert tbl_NEMOI_Hub_Definition(    
                                                    hub, 
                                                    hub_report_name,
                                                    ggm_hub,
                                                    country,
                                                    state,
                                                    gas_region,
                                                    power_region,
                                                    super_hub,
                                                    topology
                                                    ) values (%s)""" % insertString)
                                    conn.commit()
                            else:
                                flash('No new hub found in hub definition file')
                                # check if there are hubs that needs fully information
                                # if there are, filter these hubs  
                                hub_need_info = pd.DataFrame()
                                db_def_hub_df.hub_report_name = db_def_hub_df.hub_report_name.replace("","NULL").replace(np.nan,"NULL")

                                hub_need_info = db_def_hub_df[db_def_hub_df.hub_report_name == "NULL"]

                                if len(hub_need_info)>0:
                                    hub_info = pd.DataFrame()
                                    for j in range(len(hub_need_info)):
                                        hub_temp_df = hub_def_file1[hub_def_file1['hub']==str(hub_need_info.iloc[j,2])]
                                        hub_info = hub_info.append(hub_temp_df)

                                    if len(hub_info)>0:
                                        flash('More information of hubs found in hub file is added into database')

                                        hub_info = hub_info[['hub', 'hub_report_name', 'ggm_hub', 'country', 
                                                              'state','gas_region', 'power_region', 'super_hub', 'topology']]

                                        for k in range(len(hub_info)):
                                            update=("UPDATE tbl_NEMOI_Hub_Definition SET hub_report_name='%s',ggm_hub='%s',country='%s', state='%s',gas_region='%s',power_region='%s', super_hub='%s',topology='%s' where hub='%s'" %
                                                    (str(hub_info.iloc[k,1]),str(hub_info.iloc[k,2]),str(hub_info.iloc[k,3]),str(hub_info.iloc[k,4]),
                                                    str(hub_info.iloc[k,5]),str(hub_info.iloc[k,6]),str(hub_info.iloc[k,7]),str(hub_info.iloc[k,8]),str(hub_info.iloc[k,0])))
                                            #print(update)
                                            cursor.execute(update)
                                            conn.commit()                            
                    else:
                        flash('No matched data for ' + topology + ' in ' + filename_hub)


                if file_demand:
                    #- check the existence of temporary file
                    #-- if there is, delete it
                    temp_dmd = str(caseName) + '_' +str(topology) + '_dmd_temp.xlsm'
                    if os.path.exists(file_path + temp_dmd): 
                        os.remove(file_path + temp_dmd)

                    #- save the file uploaded by user to remote drive
                    filename_demand = secure_filename(file_demand.filename)
                    file_demand.save(os.path.join(file_path, filename_demand))

                    #- read workbook, change the calculation status
                    #- and save and close
                    
                    pythoncom.CoInitialize()
                    app_dmd = xw.App(visible=False)
                    wb_dmd = xw.Book(file_path + filename_demand)
                    wb_dmd.app.display_alerts = False
                    wb_dmd.app.screen_updating = False
                    wb_dmd.app.calculation = 'automatic'
                    wb_dmd.save()
                    wb_dmd.close()
                    app_dmd.kill()  
                                 

                    #- rename the file in remote drive
                    os.rename(file_path + filename_demand, file_path + temp_dmd)
                    #- read excel file into dataframe format
                    xlsx_dmd = pd.ExcelFile(file_path + temp_dmd)

                    #--- read node def sheet from demand excel file
                    D_node_def_raw = pd.read_excel(xlsx_dmd, sheet_name = 'nemo_demand_node_defs')
                    D_node_def_raw = D_node_def_raw.replace("'","_", regex=True)
                    
                    D_node_def = D_node_def_raw[D_node_def_raw['topology'] == topology]
                    if len(D_node_def) >0:

                        #--- read demand data sheet from demand excel file
                        dmd_horizon = pd.read_excel(xlsx_dmd, sheet_name = 'nemo_demand')
                        dmd_horizon = dmd_horizon.replace("'","_", regex=True)
                        #--- change demand df from the horizontal format to vertical 
                        dmd = pd.melt(dmd_horizon,id_vars=['state','node','hub'],var_name = 'date',value_name = 'demand')
                        
                        dmd['case_id'] = case_id
                        dmd['topology'] = topology
                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells 
                        dtlist=[D_node_def_raw,D_node_def,dmd_horizon,dmd]

                        for dt in dtlist:
                            dt.columns = [col.strip() for col in dt.columns]
                            dt['node'] = dt['node'].str.strip()
                            dt['hub'] = dt['hub'].str.strip()
                        #--- first compare the node+hub combination from demand horizon df and demand node definition df
                        #--- use merge
                        demand_H_N = pd.merge(dmd_horizon[['node','hub']],D_node_def[['node','hub']],
                                          on=['node','hub'],how='outer',indicator=True)
                        #--- new node+hub combination from demand data sheet
                        demand_data_only=[]
                        demand_data_only = demand_H_N[demand_H_N['_merge']=='left_only'][['node','hub']]
                        demand_data_only['topology'] = topology

                        #--- add the new node+hub combine to the node definition df
                        
                        if len(demand_data_only)>0:
                            flash('Demand file: new node and hub combs found in input data not definition sheet ','warning')
                            flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                            flash('Add more information in demand definition sheet and re-upload, if needed','warning')
                            
                            demand_data_only.to_excel(writer_upload, sheet_name ='Dmd_file-NH_in_data_not_def')

                            new_D_node_def = D_node_def.append(demand_data_only,ignore_index=True) 
                        else:
                            
                            new_D_node_def = D_node_def.copy()

                        #- First of all, make sure there are data in hub definition table of database---
                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_hub_def_df = DB_table_data(conn, sql, topology)

                        if len(db_hub_def_df) > 0:
                            
                            #- read demand node definition table in database (named with 'db_Dnode_def') and 
                            #- then compare it with new_D_node_def 
                            #- and add new node&hub to demand node definition table in db

                            sql="""SELECT * FROM tbl_NEMOI_Demand_Node_Definitions where topology=%s"""
                            db_Dnode_def = DB_table_data(conn, sql, topology)

                            if len(db_Dnode_def) == 0:

                                db_Dnode_def = new_D_node_def[['node','hub','ggm_node','sector','topology']]
                                # insert dataframe into db
                                col_name = list(db_Dnode_def.columns)
                                tbl_name = 'tbl_NEMOI_Demand_Node_Definitions'
                                insert_df_into_db(col_name, tbl_name, db_Dnode_def)                                

                            #- compare 
                            two_defs_H_N = pd.merge(new_D_node_def[['node','hub','ggm_node','sector','topology']],
                                            db_Dnode_def[['node','hub']], on=['node','hub'],how='outer',indicator=True)
                            #--- new node+hub combination from demand data sheet
                            file_Dnode_def_only=[]
                            file_Dnode_def_only = two_defs_H_N[two_defs_H_N['_merge']=='left_only'][['node','hub','ggm_node','sector','topology']]
                            
                            if len(file_Dnode_def_only)>0:
                                file_Dnode_def_only.to_excel(writer_upload, sheet_name = 'H&N_in_Dmdfile_notDB')
                                flash('New node and hub combinations found in demand file and added into database','warning')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                                for idx, row in file_Dnode_def_only.iterrows():
                                    insertList = row.tolist()
                                    insertString = ""
                                    for item in insertList:
                                        if insertString:
                                            insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                        else:
                                            insertString = "\'" + item + "\'"
                                        #print(insertString)
                                    cursor.execute("""insert tbl_NEMOI_Demand_Node_Definitions( 
                                                    node, 
                                                    hub, 
                                                    ggm_node,
                                                    sector,
                                                    topology
                                                    ) values (%s)""" % insertString)
                                    conn.commit()

                                new_db_Dnode_def = db_Dnode_def[['node','hub','ggm_node','sector','topology']]

                                nnew_D_node_def = new_db_Dnode_def.append(file_Dnode_def_only,ignore_index=True)
                            else:
                                nnew_D_node_def = db_Dnode_def[['node','hub','ggm_node','sector','topology']]




                            #--- compare the list of hub & topology from demand input data(new node def) 
                            #-- with the list from hub def db
                            
                            hub_Dmd_db = pd.merge(nnew_D_node_def[['hub','topology']],db_hub_def_df[['hub','topology']],
                                                  on=['hub','topology'],how='outer',indicator=True)

                            hub_Dmd_only = hub_Dmd_db[hub_Dmd_db['_merge']=='left_only'][['hub','topology']]

                            hub_db_only = hub_Dmd_db[hub_Dmd_db['_merge']=='right_only'][['hub','topology']] 

                            if len(hub_Dmd_only)>0:
                                hub_Dmd_only.to_excel(writer_upload, sheet_name = 'Hub_from_dmd_not_hubdefDB')

                                flash('New hubs found in demand side and added into hub definition table in database','warning')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                                #flash('Add more information in hub definition sheet and re-upload, if needed','warning')

                                for idx, row in hub_Dmd_only.iterrows():
                                    insertList = row.tolist()
                                    insertString = ""
                                    for item in insertList:
                                        if insertString:
                                            insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                        else:
                                            insertString = "\'" + item + "\'"
                                        print(insertString)
                                    cursor.execute("""insert tbl_NEMOI_Hub_Definition(    
                                                    hub, 
                                                    topology
                                                    ) values (%s)""" % insertString)
                                    conn.commit()

                                #-- re-read hub definition table in db
                                sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                                new_db_def_hub_df = DB_table_data(conn, sql, topology)

                            else:
                                new_db_def_hub_df = db_hub_def_df.copy() 
                                

                            # load demand node definition to database
                             
                            #conn = pyodbc.connect(driver='{SQL Server Native Client 10.0}',server='WMHNAPSTESLA\\NAPSSQL',database='NeMo',Trusted_Connection='Yes')
                            cursor = conn.cursor()
                            ## delete the demand node, hub, demand input data for the selected case_id
                            cursor.execute("DELETE FROM tbl_NEMOI_Demand WHERE case_id = %s and topology=%s",(case_id,topology))
                            conn.commit()
                            
                            ## node definition data

                            #cursor = conn.cursor()
                            ### read node definition table in database and get the node IDs
                            sql = """SELECT * FROM tbl_NEMOI_Demand_Node_Definitions WHERE topology=%s"""
                            db_def_Dnode_df = DB_table_data(conn, sql, topology)
                            #db_def_Dnode_df.to_excel('nodedf.xlsx')
                            #dmd.to_excel('demanddf.xlsx')

                            dmd_with_hubID = pd.merge(dmd, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub'])
                            dmd_add_nodeID = pd.merge(dmd_with_hubID, db_def_Dnode_df[['hub','node','Unique_Node_ID']], how='left', on=['hub','node'])
                            #dmd_add_nodeID.to_excel('finaldemand.xlsx')
                            dmd_add_nodeID = dmd_add_nodeID[['Unique_Hub_ID','Unique_Node_ID','node','hub',
                                                             'demand','date','case_id','topology','state']]
                            dmd_add_nodeID.demand = dmd_add_nodeID.demand.astype(float)
                            dmd_add_nodeID['date'] = pd.to_datetime(dmd_add_nodeID['date'])
                            dmd_add_nodeID['date'] = dmd_add_nodeID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            
                            # insert dataframe into db
                            col_name = list(dmd_add_nodeID.columns)
                            tbl_name = 'tbl_NEMOI_Demand'
                            insert_df_into_db(col_name, tbl_name, dmd_add_nodeID)

                        else:
                            flash('Please upload hub definition file for '+ topology,'warning')
                    else:
                        flash('No matched data for ' + topology + ' in ' + filename_demand)

                
                if file_sup:

                    #- check the existence of temporary file
                    #-- if there is, delete it
                    temp_sup = str(caseName) + '_' +str(topology) + '_sup_temp.xlsx'
                    if os.path.exists(file_path + temp_sup): 
                        os.remove(file_path + temp_sup)
                    
                    #- save the file uploaded by user to remote drive 
                    filename_sup = secure_filename(file_sup.filename)
                    file_sup.save(os.path.join(file_path, filename_sup))

                    #- read workbook, change the calculation status
                    #- and save and close
                    pythoncom.CoInitialize()
                    app_sup = xw.App(visible=False)
                    wb_sup = xw.Book(file_path + filename_sup)
                    wb_sup.app.display_alerts = False
                    wb_sup.app.screen_updating = False
                    wb_sup.app.calculation = 'automatic'
                    wb_sup.save()
                    wb_sup.close()
                    app_sup.kill()
                    

                    #- rename the file in remote drive
                    os.rename(file_path + filename_sup, file_path + temp_sup)
                    
                    #- read excel file into dataframe format
                    xlsx_sup = pd.ExcelFile(file_path + temp_sup)

                    #- supply node defs
                    sup_node_def_raw = pd.read_excel(xlsx_sup,'nemo_supply_node_defs')
                    sup_node_def_raw = sup_node_def_raw.replace("'","_", regex=True)
                    sup_node_def = sup_node_def_raw[sup_node_def_raw['topology']== topology]
                    if len(sup_node_def) > 0:

                        #- raw data from supply capacity
                        sup_cap_raw = pd.read_excel(xlsx_sup,'nemo_supply_capacity')
                        sup_cap_raw = sup_cap_raw.replace("'","_", regex=True)
                        sup_cap = pd.melt(sup_cap_raw,id_vars=['node','hub'],var_name = 'date',value_name = 'capacity')
                        #sup_cap_raw.to_excel('supcap.xlsx')

                        #- raw data from supply cost
                        sup_cost_raw = pd.read_excel(xlsx_sup,'nemo_supply_cost')
                        sup_cost_raw = sup_cost_raw.replace("'","_", regex=True)
                        sup_cost = pd.melt(sup_cost_raw,id_vars=['node','hub'],var_name = 'date',value_name = 'cost')
                        #sup_cost_raw.to_excel('supcost.xlsx')


                        #sup_node_def.to_excel('supndef.xlsx')

                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dtlist=[sup_cap_raw,sup_cost_raw,sup_cap,sup_cost,sup_node_def]

                        for dt in dtlist:
                            dt.columns = [col.strip() for col in dt.columns]
                            dt['node'] = dt['node'].str.strip()
                            dt['hub'] = dt['hub'].str.strip()


                        #- compare the unique list of node + hub comb from supply capacity df and cost df
                        sup_cap_cost_NH = pd.merge(sup_cap_raw[['node','hub']],sup_cost_raw[['node','hub']],
                                                   on=['node','hub'],how='outer',indicator=True)

                        sup_cap_only = sup_cap_cost_NH[sup_cap_cost_NH['_merge']=='left_only'][['node','hub']]
                        sup_cost_only = sup_cap_cost_NH[sup_cap_cost_NH['_merge']=='right_only'][['node','hub']]

                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_def_hub_df = DB_table_data(conn, sql, topology)

                        if len(db_def_hub_df) >0:

                            if len(sup_cap_only)==0 and len(sup_cost_only)==0:
                                #- supply capacity
                                sup_cap['case_id'] = case_id
                                sup_cap['topology'] = topology
                                #- supply cost
                                sup_cost['case_id'] = case_id
                                sup_cost['topology'] = topology
                                #- compare node and hub comb from supply data with supply node and hub definition sheet
                                sup_cost_nodedef_NH = pd.merge(sup_cost_raw[['node','hub']],sup_node_def[['node','hub']],
                                                           on=['node','hub'],how='outer',indicator=True)
                                
                                sup_cost_only1 = sup_cost_nodedef_NH[sup_cost_nodedef_NH['_merge']=='left_only'][['node','hub']]
                                sup_nodedef_only = sup_cost_nodedef_NH[sup_cost_nodedef_NH['_merge']=='right_only'][['node','hub']]
                                
                                if len(sup_cost_only1)==0:
                                    new_S_node_def = sup_node_def.copy()
                                else:
                                    sup_cost_only1['topology'] = topology
                                    flash('Supply file: new node and hub combs found in input data not definition sheet','info')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','info')
                                    flash('Add more information in supply definition sheet and re-upload, if needed','info')
                                    sup_cost_only1.to_excel(writer_upload, sheet_name ='Supfile-N&H_in_data_not_Ndef')

                                    new_S_node_def = sup_node_def.append(sup_cost_only1,ignore_index=True) 
                                #new_S_node_def.to_excel('newndef.xlsx')

                                #- FIRST---
                                #- After comparing supply node defs sheet with supply input data, 
                                #- get the new version supply node def df

                                #- SECOND---
                                #- Then grab the data from supply node def table in db
                                #- compare the new version supply node def df with data from supply node def table in db
                                #- to get the fully version of supply node def df

                                #- THIRD---
                                #- compare the list of hubs from the fully version of supply node def df
                                #- with that from hub def table in db
                                #- to make sure hubs from supply side are all included into hub def table in db
                                sql = """SELECT * FROM tbl_NEMOI_Supply_Node_Definition where topology=%s"""

                                db_Snode_def = DB_table_data(conn, sql, topology)
                                
                                if len(db_Snode_def)==0:

                                    db_Snode_def = new_S_node_def[['node','hub','ggm_node','supply_source','supply_type','topology']]
                                    # insert dataframe into db
                                    col_name = list(db_Snode_def.columns)
                                    tbl_name = 'tbl_NEMOI_Supply_Node_Definition'
                                    insert_df_into_db(col_name, tbl_name, db_Snode_def)

                                #- compare 
                                two_S_defs_H_N = pd.merge(new_S_node_def[['node','hub','ggm_node','supply_source','supply_type','topology']],
                                                        db_Snode_def[['node','hub']], on=['node','hub'],how='outer',indicator=True)

                                file_Snode_def_only = []
                                file_Snode_def_only = two_S_defs_H_N[two_S_defs_H_N['_merge']=='left_only'][['node','hub','ggm_node','supply_source','supply_type','topology']]
                                #db_Snode_def_only = two_S_defs_H_N[two_S_defs_H_N['_merge']=='right_only'][['node','hub']]
                                #file_Snode_def_only.to_excel('filendefonly.xlsx')

                                if len(file_Snode_def_only)>0:

                                    file_Snode_def_only.to_excel(writer_upload, sheet_name = 'H&N_in_Supfile_notDB')
                                    flash('New node and hub combinations found in supply file and added into database','info')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','info')
                                    for idx, row in file_Snode_def_only.iterrows():
                                        insertList = row.tolist()
                                        insertString = ""
                                        for item in insertList:
                                            if insertString:
                                                insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                            else:
                                                insertString = "\'" + item + "\'"
                                            #print(insertString)
                                        cursor.execute("""insert tbl_NEMOI_Supply_Node_Definition( 
                                                        node, 
                                                        hub, 
                                                        ggm_node,
                                                        supply_source,
                                                        supply_type,
                                                        topology
                                                        ) values (%s)""" % insertString)
                                        conn.commit()


                                    new_db_Snode_def = db_Snode_def[['node','hub','ggm_node','supply_source','supply_type','topology']]
                                    nnew_S_node_def = new_db_Snode_def.append(file_Snode_def_only,ignore_index=True)
                                else:
                                    nnew_S_node_def = db_Snode_def[['node','hub','ggm_node','supply_source','supply_type','topology']]
                                #nnew_S_node_def.to_excel('nnew_S_node_def.xlsx')

                                #- compare list of hubs of nnew_S_node_def with that of hub defs table in db
            
                                #- compare
                                hub_Sup_db = pd.merge(nnew_S_node_def[['hub','topology']],db_def_hub_df[['hub','topology']],
                                                      on=['hub','topology'],how='outer',indicator=True)

                                hub_Sup_only = hub_Sup_db[hub_Sup_db['_merge']=='left_only'][['hub','topology']]

                                hub_db_only = hub_Sup_db[hub_Sup_db['_merge']=='right_only'][['hub','topology']]

                                #--
                                if len(hub_Sup_only)>0:
                                    
                                    hub_Sup_only.to_excel(writer_upload, sheet_name ='Hub_in_sup_file_not_db')
                                    flash('New hubs found in supply side and added into hub definition table in database','info')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','info')
                                    
                                    # insert new hubs found in supply node defition sheet into hub def table in db
                                    for idx, row in hub_Sup_only.iterrows():
                                        insertList = row.tolist()
                                        insertString = ""
                                        for item in insertList:
                                            if insertString:
                                                insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                            else:
                                                insertString = "\'" + item + "\'"
                                            print(insertString)
                                        cursor.execute("""insert tbl_NEMOI_Hub_Definition(    
                                                        hub, 
                                                        topology
                                                        ) values (%s)""" % insertString)
                                        conn.commit()
                                    #---read updated hub definition table from database
                                    sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                                    new_db_def_hub_df = DB_table_data(conn, sql, topology)            
                                else:
                                    new_db_def_hub_df = db_def_hub_df.copy()
                                

                                #--------------------------------------------------
                                #--delete data for selected case
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM tbl_NEMOI_Supply_Capacity WHERE case_id=%s and topology=%s",(case_id,topology))
                                cursor.execute("DELETE FROM tbl_NEMOI_Supply_Cost WHERE case_id=%s and topology=%s",(case_id,topology))
                                conn.commit()


                                ### read supply node definition table in database and get the supply node IDs
                                sql = """SELECT * FROM tbl_NEMOI_Supply_Node_Definition WHERE topology=%s"""
                                db_def_Snode_df = DB_table_data(conn, sql, topology)
                                #- merge supply capacity and cost with hub IDs and node IDs
                                #-- supply capacity
                                supcap_with_hubID = pd.merge(sup_cap, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                                supcap_add_nodeID = pd.merge(supcap_with_hubID, db_def_Snode_df[['hub','node','Unique_SupplyNode_ID']], how='left', on=['hub','node'])
                                #-- supply cost
                                supcost_with_hubID = pd.merge(sup_cost, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                                supcost_add_nodeID = pd.merge(supcost_with_hubID, db_def_Snode_df[['hub','node','Unique_SupplyNode_ID']], how='left', on=['hub','node'])
                                #- insert supply capacity into database
                                supcap_add_nodeID.to_excel('supcap.xlsx')
                                supcap_add_nodeID = supcap_add_nodeID[['Unique_Hub_ID','Unique_SupplyNode_ID','node','hub','capacity','date','case_id','topology']]
                                supcap_add_nodeID['date'] = pd.to_datetime(supcap_add_nodeID['date']).dt.date
                                supcap_add_nodeID['date'] = supcap_add_nodeID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                                # insert dataframe into db
                                col_name = list(supcap_add_nodeID.columns)
                                tbl_name = 'tbl_NEMOI_Supply_Capacity'
                                insert_df_into_db(col_name, tbl_name, supcap_add_nodeID)                                

                                #-- sup cost
                                supcost_add_nodeID = supcost_add_nodeID[['Unique_Hub_ID','Unique_SupplyNode_ID','node','hub','cost','date','case_id','topology']]
                                supcost_add_nodeID['date'] = pd.to_datetime(supcost_add_nodeID['date']).dt.date
                                supcost_add_nodeID['date'] = supcost_add_nodeID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                                # insert dataframe into db
                                col_name = list(supcost_add_nodeID.columns)
                                tbl_name = 'tbl_NEMOI_Supply_Cost'
                                insert_df_into_db(col_name, tbl_name, supcost_add_nodeID)                               

                            else:
                                flash('The node and hub combs in supply capacity sheet do not match that in supply cost sheet','info')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','info')
                                if len(sup_cap_only)>0:
                                    sup_cap_only['topology'] = topology
                                    flash('In detail, new node and hub combs found from supply capacity sheet','info')
                                    sup_cap_only.to_excel(writer_upload,sheet_name='Supfile-NH_in_cap_not_cost')
                                if len(sup_cost_only)>0:
                                    sup_cost_only['topology'] = topology
                                    flash('In detail, new node and hub combs found in supply cost sheet','info')
                                    sup_cost_only.to_excel(writer_upload,sheet_name='Supfile-NH_in_cost_not_cap')
                        else:
                            flash('Please upload hub definition file for '+ topology,'warning')
                    else:
                        flash('No matched data for ' + topology + ' in ' + filename_sup)


#-------------------------------------------------------
                if file_sto:
                    #- check the existence of temporary file
                    #-- if there is, delete it
                    temp_sto = str(caseName) + '_' + str(topology) + '_sto_temp.xlsx'
                    if os.path.exists(file_path + temp_sto): 
                        os.remove(file_path + temp_sto)

                    #- save the file uploaded by user to remote drive
                    filename_sto = secure_filename(file_sto.filename)
                    file_sto.save(os.path.join(file_path, filename_sto))

                    #- read workbook, change the calculation status
                    #- and save and close
                    
                    pythoncom.CoInitialize()
                    app_sto = xw.App(visible=False)
                    wb_sto = xw.Book(file_path + filename_sto)
                    wb_sto.app.display_alerts = False
                    wb_sto.app.screen_updating = False
                    wb_sto.app.calculation = 'automatic'
                    wb_sto.save()
                    wb_sto.close()
                    app_sto.kill()
                    
                    #- rename the file in remote drive
                    os.rename(file_path + filename_sto, file_path + temp_sto)

                    #- read excel file into dataframe format
                    xlsx_sto = pd.ExcelFile(file_path + temp_sto) 
                    #xlsx_sto = pd.ExcelFile('d:/Users/fanxin/Desktop/NEMO_INPUT_and_storage/storage_input_data_20181127.xlsx')

                    #- storage definition
                    sto_def_raw = pd.read_excel(xlsx_sto,sheet_name = 'nemo_sto_defs')
                    sto_def_raw = sto_def_raw.replace("'","_", regex=True)

                    sto_def = sto_def_raw[sto_def_raw['topology']==topology]

                    if len(sto_def) > 0:

                        #-- storage sheets
                        storingCost_raw = pd.read_excel(xlsx_sto,sheet_name = 'storing_cost')
                        injectionCost_raw = pd.read_excel(xlsx_sto,sheet_name = 'injection_cost')
                        extractionCost_raw = pd.read_excel(xlsx_sto,sheet_name = 'extraction_cost')
                        maxInjection_raw = pd.read_excel(xlsx_sto,sheet_name = 'max_injection')
                        maxExtraction_raw = pd.read_excel(xlsx_sto,sheet_name = 'max_extraction')
                        maxStorageCap_raw = pd.read_excel(xlsx_sto,sheet_name = 'max_storage_cap')
                        minStorageCap_raw = pd.read_excel(xlsx_sto,sheet_name = 'min_storage_cap')
                        
                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dflist_raw = [storingCost_raw,injectionCost_raw,extractionCost_raw,maxInjection_raw,maxExtraction_raw,maxStorageCap_raw,minStorageCap_raw]

                        for df in dflist_raw:
                            df = df.replace("'","_", regex=True)
                            df.columns = [col.strip() for col in df.columns]
                        
                        dflist_raw_1_name = ['storing_cost_sheet','max_injection_sheet','max_extraction_sheet','max_storage_capacity_sheet','min_storage_capacity_sheet']
                        dflist_raw_1 = [storingCost_raw, maxInjection_raw, maxExtraction_raw, maxStorageCap_raw, minStorageCap_raw]
                        
                        for j in range(len(dflist_raw_1)):
                            merged_df_1 = pd.merge(dflist_raw_1[j][['sto_facility']],sto_def[['sto_facility']],on=['sto_facility'],how='outer',indicator=True)
                            excel_data_only_1 = []
                            excel_data_only_1 = merged_df_1[merged_df_1['_merge']=='left_only'][['sto_facility']]
                            excel_def_only_1 = merged_df_1[merged_df_1['_merge']=='right_only'][['sto_facility']]
                            if len(excel_data_only_1)>0 or len(excel_def_only_1)>0:
                                raise ValueError('misaligned data structure detected in {} vs {}'.format(dflist_raw_1_name[j],'sto_defs_sheet')
                                                       + '\n'+
                                                       '(they must be aligned)')
                            
                        
                        dflist_raw_2_name = ['injection_cost_sheet','extraction_cost_sheet']
                        
                        dflist_raw_2 = [injectionCost_raw, extractionCost_raw]
                        
                        for k in range(len(dflist_raw_2)):
                            merged_df_2 = pd.merge(dflist_raw_2[k][['hub','sto_facility']],sto_def[['hub','sto_facility']],on=['hub','sto_facility'],how='outer',indicator=True)
                            excel_data_only_2 = []
                            excel_data_only_2 = merged_df_2[merged_df_2['_merge']=='left_only'][['hub','sto_facility']]
                            excel_def_only_2 = merged_df_2[merged_df_2['_merge']=='right_only'][['hub','sto_facility']]
                            if len(excel_data_only_2)>0 or len(excel_def_only_2)>0:
                                raise ValueError('misaligned data structure detected in {} vs {}'.format(dflist_raw_2_name[k],'sto_defs_sheet')
                                                       + '\n'+
                                                       '(they must be aligned)')
                        
                        
                        #- first compare hubs in sto_defs sheet with hub def table in db then compare hub & storage facility combination with storage definition table in db   
                        #- then merge data and insert into their corresponding tables in db
                        
                        #- step 1 comparison
                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_hub_def_df = DB_table_data(conn, sql, topology) 
                        
                        hub_sto_def = pd.merge(sto_def[['hub','topology']],db_hub_def_df[['hub','topology']],on=['hub','topology'],how='outer',indicator=True)
                        
                        hub_sto_sheet_only = hub_sto_def[hub_sto_def['_merge']=='left_only'][['hub','topology']]
                        
                        if len(hub_sto_sheet_only)>0:
                            hub_sto_sheet_only.to_excel(writer_upload, sheet_name = 'Hub_from_sto_not_hubdefDB')
                            flash('New hubs found in storage defs sheet and added into hub definition table in database','warning')
                            for idx, row in hub_sto_sheet_only.iterrows():
                                insertList = row.tolist()
                                insertString = ""
                                for item in insertList:
                                    if insertString:
                                        insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                    else:
                                        insertString = "\'" + item + "\'"
                                    print(insertString)
                                cursor.execute("""insert tbl_NEMOI_Hub_Definition( 
                                                  hub,
                                                  topology
                                                  ) values (%s)""" % insertString)
                                conn.commit()
                            #conn = pyodbc.connect(driver='{SQL Server Native Client 10.0}',server='WMHNAPSTESLA\\NAPSSQL',database='NeMo',Trusted_Connection='Yes')
                            #cursor = conn.cursor()
                            sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                            new_db_def_hub_df = DB_table_data(conn, sql, topology)

                        else:
                            new_db_def_hub_df = db_hub_def_df.copy() 
                        
                        #-- compare sto defs sheet and table in db
                        
                        sql = """SELECT * FROM tbl_NEMOI_Storage_Definitions where topology=%s"""
                        db_sto_def = DB_table_data(conn, sql, topology)
                        
                        if len(db_sto_def) == 0:

                            sto_def_todb = sto_def[['sto_facility', 'hub', 'online_date', 'province', 'topology']]
                            sto_def_todb['online_date'] = pd.to_datetime(sto_def_todb['online_date']).dt.date
                            sto_def_todb['online_date'] = sto_def_todb['online_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            # insert dataframe into db
                            col_name = list(sto_def_todb.columns)
                            tbl_name = 'tbl_NEMOI_Storage_Definitions'
                            insert_df_into_db(col_name, tbl_name, sto_def_todb)                            

                            #db_Dnode_def = new_D_node_def.copy()
                        else:
                            two_defs_sheet_db = pd.merge(sto_def[['sto_facility', 'hub', 'online_date', 'province', 'topology']],db_sto_def[['sto_facility','hub']], on=['sto_facility','hub'],how='outer',indicator=True)
                            sto_def_sheet_only=[]
                            sto_def_sheet_only = two_defs_sheet_db[two_defs_sheet_db['_merge']=='left_only'][['sto_facility', 'hub', 'online_date', 'province', 'topology']]
                            
                            if len(sto_def_sheet_only)>0:
                                sto_def_sheet_only.to_excel(writer_upload, sheet_name = 'sto&h_inStoFile_notDB')
                                flash('New storage facility and hub combinations found in storage file and added into database','warning')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                                for idx, row in sto_def_sheet_only.iterrows():
                                    insertList = row.tolist()
                                    insertString = ""
                                    for item in insertList:
                                        if insertString:
                                            insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                        else:
                                            insertString = "\'" + item + "\'"
                                        #print(insertString)
                                    cursor.execute("""insert tbl_NEMOI_Storage_Definitions( 
                                                    sto_facility, 
                                                    hub, 
                                                    online_date,
                                                    province,
                                                    topology
                                                    ) values (%s)""" % insertString)
                                    conn.commit()
                                    
                        #- re-read storage definitions table in db
                                    
                        sql = """SELECT * FROM tbl_NEMOI_Storage_Definitions where topology=%s"""
                        db_sto_def = DB_table_data(conn, sql, topology)
                        
                        #- hub id is in new_db_def_hub_df
                        #- storage facility id is in db_sto_def
                        
                        #- step 2 merge and insert
                        storingCost = storingCost_raw.melt(id_vars=['sto_facility'],var_name='date', value_name='storing_cost')
                        maxInjection = maxInjection_raw.melt(id_vars=['sto_facility'],var_name='date', value_name='max_inj')
                        maxExtraction = maxExtraction_raw.melt(id_vars=['sto_facility'],var_name='date', value_name='max_ext')
                        maxStorageCap = maxStorageCap_raw.melt(id_vars=['sto_facility'],var_name='date', value_name='max_sto_cap')
                        minStorageCap = minStorageCap_raw.melt(id_vars=['sto_facility'],var_name='date', value_name='min_sto_cap')
                        #---
                        injectionCost = injectionCost_raw.melt(id_vars=['hub', 'sto_facility'],var_name='date', value_name='inj_cost')
                        extractionCost = extractionCost_raw.melt(id_vars=['sto_facility','hub'],var_name='date', value_name='ext_cost')
                        
                        par_name = ['max_injection','max_extraction','max_storage_capacity','min_storage_capacity','injection_cost','extraction_cost']

                        par_df = [maxInjection,maxExtraction,maxStorageCap,minStorageCap,injectionCost,extractionCost]
                    
                        for i in range(len(par_df)):
                            #print(i)
                            if par_df[i].shape[0] != storingCost.shape[0]:
                                raise ValueError('misaligned data structure detected in {} vs {}'.format(par_name[i],'storing_cost') 
                                                  + '\n'+ 
                                                 '(they must be aligned)')
                        #-- merge together
                        sto_par_ls = [storingCost,maxInjection,maxExtraction,maxStorageCap,minStorageCap] 
                    
                        sto_par_df = reduce(lambda left,right:pd.merge(left,right,'inner',
                                              on = ['sto_facility','date']),sto_par_ls)

                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dflist = [sto_par_df,injectionCost,extractionCost]

                        for df in dflist:
                            df['case_id'] = case_id
                            df['topology'] = topology

                        #-- add IDs to sto_par_df
                        sto_par_df_add_stoID = pd.merge(sto_par_df, db_sto_def[['sto_facility','Unique_Sto_ID']], how='left', on=['sto_facility'])
                        
                        #-- add IDs to costinjectionCost
                        injCost_with_hubID = pd.merge(injectionCost, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                        
                        injCost_add_stoID = pd.merge(injCost_with_hubID, db_sto_def[['hub','sto_facility','Unique_Sto_ID']], how='left', on=['hub','sto_facility'])
                        
                        #-- add IDs to extractionCost
                        extCost_with_hubID = pd.merge(extractionCost, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                        extCost_add_stoID = pd.merge(extCost_with_hubID, db_sto_def[['hub','sto_facility','Unique_Sto_ID']], how='left', on=['hub','sto_facility'])
                        
                       
                        #------
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM tbl_NEMOI_Storage_Constraints WHERE case_id=%s and topology=%s",(case_id,topology))
                        cursor.execute("DELETE FROM tbl_NEMOI_Storage_Injection WHERE case_id=%s and topology=%s",(case_id,topology))
                        cursor.execute("DELETE FROM tbl_NEMOI_Storage_Extraction WHERE case_id=%s and topology=%s",(case_id,topology))
                        conn.commit()
                        
                        # storage constraints
                        sto_par_df_add_stoID = sto_par_df_add_stoID[['Unique_Sto_ID','sto_facility','storing_cost','max_inj','max_ext','max_sto_cap','min_sto_cap','date','case_id','topology']]
                        sto_par_df_add_stoID.rename(columns = {'max_inj':'max_injection','max_ext':'max_extraction',
                                                                          'max_sto_cap':'max_sto_capacity','min_sto_cap':'min_sto_capacity'}, inplace = True) 
                        print(sto_par_df_add_stoID.head())
                        sto_par_df_add_stoID['date'] = pd.to_datetime(sto_par_df_add_stoID['date']).dt.date
                        sto_par_df_add_stoID['date'] = sto_par_df_add_stoID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # insert dataframe into db
                        for col in ['storing_cost','max_injection','max_extraction','max_sto_capacity','min_sto_capacity']:
                            sto_par_df_add_stoID[col] = sto_par_df_add_stoID[col].astype(float)
                        col_name = list(sto_par_df_add_stoID.columns)
                        tbl_name = 'tbl_NEMOI_Storage_Constraints'
                        insert_df_into_db(col_name, tbl_name, sto_par_df_add_stoID)

                        #-- injection cost
                        injCost_add_stoID = injCost_add_stoID[['Unique_Hub_ID','Unique_Sto_ID', 'hub','sto_facility','inj_cost','date','topology','case_id']]
                        injCost_add_stoID['date'] = pd.to_datetime(injCost_add_stoID['date']).dt.date
                        injCost_add_stoID['date'] = injCost_add_stoID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # insert dataframe into db
                        
                        injCost_add_stoID['inj_cost'] = injCost_add_stoID['inj_cost'].astype(float)
                        col_name = list(injCost_add_stoID.columns)
                        tbl_name = 'tbl_NEMOI_Storage_Injection'
                        insert_df_into_db(col_name, tbl_name, injCost_add_stoID)
                            
                        #-- extraction cost
                        extCost_add_stoID = extCost_add_stoID[['Unique_Sto_ID', 'Unique_Hub_ID','sto_facility','hub','ext_cost','date','topology','case_id']]
                        extCost_add_stoID['date'] = pd.to_datetime(extCost_add_stoID['date']).dt.date
                        extCost_add_stoID['date'] = extCost_add_stoID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # insert dataframe into db
                        extCost_add_stoID['ext_cost'] = extCost_add_stoID['ext_cost'].astype(float)
                        col_name = list(extCost_add_stoID.columns)
                        tbl_name = 'tbl_NEMOI_Storage_Extraction'
                        insert_df_into_db(col_name, tbl_name, extCost_add_stoID)                        

                    else:
                        flash('No matched data for ' + topology + ' in '+ filename_sto)

#-------------------------------------------------------
                if file_exp:
                    #- check the existence of temporary file
                    #-- if there is, delete it
                    temp_exp = str(caseName) + '_' +str(topology) + '_exp_temp.xlsx'
                    if os.path.exists(file_path + temp_exp): 
                        os.remove(file_path + temp_exp)

                    #- save the file uploaded by user to remote drive
                    filename_exp = secure_filename(file_exp.filename)
                    file_exp.save(os.path.join(file_path, filename_exp))

                    #- read workbook, change the calculation status
                    #- and save and close
                    
                    pythoncom.CoInitialize()
                    app_exp = xw.App(visible=False)
                    wb_exp = xw.Book(file_path + filename_exp)
                    wb_exp.app.display_alerts = False
                    wb_exp.app.screen_updating = False
                    wb_exp.app.calculation = 'automatic'
                    wb_exp.save()
                    wb_exp.close()
                    app_exp.kill()
                    
                    #- rename the file in remote drive
                    os.rename(file_path + filename_exp, file_path + temp_exp)

                    #- read excel file into dataframe format
                    xlsx_exp = pd.ExcelFile(file_path + temp_exp) 
                    #xlsx_exp = pd.ExcelFile('d:/Users/fanxin/Desktop/NEMO_INPUT_and_storage/inputs/export/NeMoID_Southern_Cone_Export_test.xlsx')

                    #- export definition
                    exp_def_raw = pd.read_excel(xlsx_exp,sheet_name = 'nemo_export_defs')
                    exp_def_raw = exp_def_raw.replace("'","_", regex=True)

                    exp_def = exp_def_raw[exp_def_raw['topology']==topology]

                    if len(exp_def) > 0:

                        #-- storage sheets
                        expPrice_raw = pd.read_excel(xlsx_exp,sheet_name = 'FOB_export_price')
                        expCap_raw = pd.read_excel(xlsx_exp,sheet_name = 'export_capacity')
                        
                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dflist_raw = [expPrice_raw,expCap_raw]

                        for df in dflist_raw:
                            df = df.replace("'","_", regex=True)
                            df.columns = [col.strip() for col in df.columns]
                        
                        dflist_raw_name = ['export_price_sheet','export_capacity_sheet']
                        
                        dflist_raw = [expPrice_raw, expCap_raw]
                        
                        for k in range(len(dflist_raw)):
                            merged_df = pd.merge(dflist_raw[k][['hub','node']],exp_def[['hub','node']],on=['hub','node'],how='outer',indicator=True)
                            excel_data_only = []
                            excel_data_only = merged_df[merged_df['_merge']=='left_only'][['hub','node']]
                            excel_def_only = merged_df[merged_df['_merge']=='right_only'][['hub','node']]
                            if len(excel_data_only)>0 or len(excel_def_only)>0:
                                raise ValueError('misaligned data structure detected in {} vs {}'.format(dflist_raw_name[k],'nemo_export_defs_sheet')
                                                       + '\n'+
                                                       '(they must be aligned)')
                        
                        
                        #- first compare hubs in exp_defs sheet with hub def table in db then compare hub & node combination with export definition table in db   
                        #- then merge data and insert into their corresponding tables in db
                        
                        #- step 1 comparison
                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_hub_def_df = DB_table_data(conn, sql, topology)
                        
                        hub_exp_def = pd.merge(exp_def[['hub','topology']],db_hub_def_df[['hub','topology']],on=['hub','topology'],how='outer',indicator=True)
                        
                        hub_exp_sheet_only = hub_exp_def[hub_exp_def['_merge']=='left_only'][['hub','topology']]
                        
                        if len(hub_exp_sheet_only)>0:
                            hub_exp_sheet_only.to_excel(writer_upload, sheet_name = 'Hub_from_exp_not_hubdefDB')
                            flash('New hubs found in export defs sheet and added into hub definition table in database','warning')
                            for idx, row in hub_exp_sheet_only.iterrows():
                                insertList = row.tolist()
                                insertString = ""
                                for item in insertList:
                                    if insertString:
                                        insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                    else:
                                        insertString = "\'" + item + "\'"
                                    print(insertString)
                                cursor.execute("""insert tbl_NEMOI_Hub_Definition( 
                                                  hub,
                                                  topology
                                                  ) values (%s)""" % insertString)
                                conn.commit()
                            #conn = pyodbc.connect(driver='{SQL Server Native Client 10.0}',server='WMHNAPSTESLA\\NAPSSQL',database='NeMo',Trusted_Connection='Yes')
                            #cursor = conn.cursor()
                            sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                            new_db_def_hub_df = DB_table_data(conn, sql, topology)

                        else:
                            new_db_def_hub_df = db_hub_def_df.copy() 
                        
                        #-- compare exp defs sheet and table in db
                        
                        sql = """SELECT * FROM tbl_NEMOI_Export_Node_Definitions where topology=%s"""
                        db_exp_def = DB_table_data(conn, sql, topology)
                        
                        if len(db_exp_def) == 0:

                            exp_def_todb = exp_def[['hub', 'node', 'topology']]
                            # insert dataframe into db
                            col_name = list(exp_def_todb.columns)
                            tbl_name = 'tbl_NEMOI_Export_Node_Definitions'
                            insert_df_into_db(col_name, tbl_name, exp_def_todb)                            
                            #db_Dnode_def = new_D_node_def.copy()
                        else:
                            two_defs_sheet_db = pd.merge(exp_def,db_exp_def[['hub','node']], on=['hub','node'],how='outer',indicator=True)
                            exp_def_sheet_only=[]
                            exp_def_sheet_only = two_defs_sheet_db[two_defs_sheet_db['_merge']=='left_only'][['hub', 'node', 'topology']]
                            
                            if len(exp_def_sheet_only)>0:
                                exp_def_sheet_only.to_excel(writer_upload, sheet_name = 'H&N_inExpFile_notDB')
                                flash('New hub and node combinations found in export file and added into database','warning')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','warning')
                                for idx, row in exp_def_sheet_only.iterrows():
                                    insertList = row.tolist()
                                    insertString = ""
                                    for item in insertList:
                                        if insertString:
                                            insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                        else:
                                            insertString = "\'" + item + "\'"
                                        #print(insertString)
                                    cursor.execute("""insert tbl_NEMOI_Export_Node_Definitions( 
                                                    hub, 
                                                    node,
                                                    topology
                                                    ) values (%s)""" % insertString)
                                    conn.commit()
                                    
                        #- re-read storage definitions table in db
                                    
                        sql = """SELECT * FROM tbl_NEMOI_Export_Node_Definitions where topology=%s"""
                        db_exp_def = DB_table_data(conn, sql, topology)
                        
                        #- step 2 merge and insert
                        exportPrice = expPrice_raw.melt(id_vars=['hub','node'],var_name='date', value_name='FOB_price')
                        exportCap = expCap_raw.melt(id_vars=['hub','node'],var_name='date', value_name='capacity')

                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dflist = [exportPrice,exportCap]

                        for df in dflist:
                            df['case_id'] = case_id
                            df['topology'] = topology

                        #-- add IDs to export price
                        expPrice_with_hubID = pd.merge(exportPrice, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                        
                        expPrice_add_expID = pd.merge(expPrice_with_hubID, db_exp_def[['hub','node','Unique_ExpNode_ID']], how='left', on=['hub','node'])
                        
                        #-- add IDs to export capacity
                        expCap_with_hubID = pd.merge(exportCap, new_db_def_hub_df[['hub','Unique_Hub_ID']], how='left', on=['hub']) 
                        expCap_add_expID = pd.merge(expCap_with_hubID, db_exp_def[['hub','node','Unique_ExpNode_ID']], how='left', on=['hub','node'])
                        
                        #------
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM tbl_NEMOI_Export_Price WHERE case_id=%s and topology=%s",(case_id,topology))
                        cursor.execute("DELETE FROM tbl_NEMOI_Export_Capacity WHERE case_id=%s and topology=%s",(case_id,topology))
                        conn.commit()
                        
                        # export FOB price
                        expPrice_add_expID = expPrice_add_expID[['Unique_Hub_ID', 'Unique_ExpNode_ID','hub', 'node','FOB_price','date','case_id','topology']]
                        expPrice_add_expID['date'] = pd.to_datetime(expPrice_add_expID['date']).dt.date
                        expPrice_add_expID['date'] = expPrice_add_expID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # insert dataframe into db
                        expPrice_add_expID['FOB_price'] = expPrice_add_expID['FOB_price'].astype(float)
                        col_name = list(expPrice_add_expID.columns)
                        tbl_name = 'tbl_NEMOI_Export_Price'
                        insert_df_into_db(col_name, tbl_name, expPrice_add_expID)

                        #-- export capacity
                        expCap_add_expID = expCap_add_expID[['Unique_Hub_ID', 'Unique_ExpNode_ID','hub', 'node','capacity','date','case_id','topology']]
                        expCap_add_expID['date'] = pd.to_datetime(expCap_add_expID['date']).dt.date
                        expCap_add_expID['date'] = expCap_add_expID['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # insert dataframe into db
                        expCap_add_expID['capacity'] = expCap_add_expID['capacity'].astype(float)
                        col_name = list(expCap_add_expID.columns)
                        tbl_name = 'tbl_NEMOI_Export_Capacity'
                        insert_df_into_db(col_name, tbl_name, expCap_add_expID)
                            
                    else:
                        flash('No matched data for ' + topology + ' in '+ filename_exp)

#----------------------------------------------------


                if file_arc:
                    #- check the existence of temporary file
                    #-- if there is, delete it
                    temp_arc = str(caseName) + '_' +str(topology) + '_arc_temp.xlsx'
                    if os.path.exists(file_path + temp_arc): 
                        os.remove(file_path + temp_arc)

                    #- save the file uploaded by user to remote drive
                    filename_arc = secure_filename(file_arc.filename)
                    file_arc.save(os.path.join(file_path, filename_arc))

                    #- read workbook, change the calculation status
                    #- and save and close
                    
                    pythoncom.CoInitialize()
                    app_arc = xw.App(visible=False)
                    wb_arc = xw.Book(file_path + filename_arc)
                    wb_arc.app.display_alerts = False
                    wb_arc.app.screen_updating = False
                    wb_arc.app.calculation = 'automatic'
                    wb_arc.save()
                    wb_arc.close()
                    app_arc.kill()
                    
                    #- rename the file in remote drive
                    os.rename(file_path + filename_arc, file_path + temp_arc)

                    #- read excel file into dataframe format
                    xlsx_arc = pd.ExcelFile(file_path + temp_arc)

                    #- arc definition
                    arc_def_raw = pd.read_excel(xlsx_arc,sheet_name = 'nemo_arc_definitions')
                    arc_def_raw = arc_def_raw.replace("'","_", regex=True)

                    arc_def = arc_def_raw[arc_def_raw['topology']==topology]

                    if len(arc_def) > 0:

                        #-- arc capacity sheet
                        arc_cap = pd.read_excel(xlsx_arc,sheet_name = 'nemo_infrastructure_capacity')
                        arc_cap = arc_cap.replace("'","_", regex=True)
                        # print(arc_cap.head())
                        #-- arc cost sheet
                        arc_cost_horizon = pd.read_excel(xlsx_arc,sheet_name = 'nemo_infrastructure_cost')
                        arc_cost_horizon = arc_cost_horizon.replace("'","_", regex=True)
                        arc_cost_horizon = arc_cost_horizon.rename(index=str, columns={"name": "arc_name"})
                        #- change arc horizon format to be vertical
                        arc_cost_ver = pd.melt(arc_cost_horizon,id_vars=['from_hub','to_hub','arc_name'],
                                               var_name = 'date',value_name = 'cost_pesoGJ')

                        #- arc flow constraint
                        arc_flow_raw_data = pd.read_excel(xlsx_arc,sheet_name = 'nemo_flow_constraints')
                        arc_flow_raw_data = arc_flow_raw_data.replace("'","_", regex=True)
                        # print(arc_flow_raw_data.head())
                        #--need to compare the hub for min flow (not done yet)---
                        #- remove space from node and hub columns, in case the data read from excel file have space in some cells  
                        dtlist=[arc_cap,arc_cost_horizon,arc_cost_ver,arc_def_raw,arc_def,arc_flow_raw_data]

                        for dt in dtlist:
                            dt.columns = [col.strip() for col in dt.columns]
                            dt['from_hub'] = dt['from_hub'].str.strip()
                            dt['to_hub'] = dt['to_hub'].str.strip()
                        #--get unique pipeline (ppn) from arc cost ver df
                        ppn_cost_unique = arc_cost_horizon.groupby(['from_hub','to_hub']).size().reset_index().rename(columns={0:'count'})
                        #- get unique pipeline (ppn) from arc cost ver df
                        ppn_cap_unique = arc_cap.groupby(['from_hub','to_hub']).size().reset_index().rename(columns={0:'count'})
                        
                        ppn_diff = pd.merge(ppn_cap_unique[['from_hub','to_hub']],
                                                ppn_cost_unique[['from_hub','to_hub']],
                                                on=['from_hub','to_hub'],how='outer',indicator=True)

                        arc_cap_only = ppn_diff[ppn_diff['_merge']=='left_only'] 
                        arc_cost_only = ppn_diff[ppn_diff['_merge']=='right_only']

                        #-- read hub definition table in database
                        sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                        db_def_hub_df = DB_table_data(conn, sql, topology)

                        if len(db_def_hub_df)>0:

                            if len(arc_cap_only)==0 and len(arc_cost_only)==0:
                                #- compare the list of pipeline(ppn) from arc definition and that from the arc cost (or arc capacity)
                                ppn_arcdef = arc_def.groupby(['from_hub','to_hub']).size().reset_index().rename(columns={0:'count'})
                                ppn_arc_defvscost = pd.merge(ppn_arcdef[['from_hub','to_hub']],
                                                                    ppn_cost_unique[['from_hub','to_hub']],
                                                                    on=['from_hub','to_hub'],how='outer',indicator=True)
                                arc_def_only = ppn_arc_defvscost[ppn_arc_defvscost['_merge']=='left_only'] 
                                arc_cost_only1 = ppn_arc_defvscost[ppn_arc_defvscost['_merge']=='right_only']
                                
                                if len(arc_cost_only1) >0 :
                                    arc_cost_only_withname = pd.merge(arc_cost_only1[['from_hub','to_hub']],
                                                                      arc_cost_horizon[['from_hub','to_hub','arc_name']],
                                                                      on=['from_hub','to_hub'],how='left')
                                    arc_cost_only_withname['topology'] = topology
                                    arc_cost_only_withname.to_excel(writer_upload,sheet_name='arcfile-Pipe_in_data_not_def')

                                    flash('Arc file: new pipelines found in input data not definition sheet','danger')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','danger')
                                    flash('Add information to arc definition sheet and re-upload if needed','danger')
                                    
                                    new_arc_def = arc_def.append(arc_cost_only_withname,ignore_index=True)
                                else:
                                    new_arc_def = arc_def.copy()


                                #- compare this 'new_arc_def' from arc file with arc_definition table in db
                                sql = """SELECT * FROM tbl_NEMOI_Arc_Definitions where topology=%s"""
                                db_arc_def = DB_table_data(conn, sql, topology) 

                                if len(db_arc_def)==0:

                                    db_arc_def = new_arc_def[['from_hub','to_hub','arc_name','arc_type','topology','corridor']]
                                    # insert dataframe into db
                                    col_name = list(db_arc_def.columns)
                                    tbl_name = 'tbl_NEMOI_Arc_Definitions'
                                    insert_df_into_db(col_name, tbl_name, db_arc_def)                                    

                                #- compare 
                                two_arc_defs_ppn = pd.merge(new_arc_def[['from_hub','to_hub','arc_name','arc_type','topology','corridor']],
                                                        db_arc_def[['from_hub','to_hub']], on=['from_hub','to_hub'],how='outer',indicator=True)

                                file_arc_def_only = []
                                file_arc_def_only = two_arc_defs_ppn[two_arc_defs_ppn['_merge']=='left_only'][['from_hub','to_hub','arc_name','arc_type','topology','corridor']]

                                if len(file_arc_def_only)>0:
                                    file_arc_def_only.to_excel(writer_upload, sheet_name='pipe_in_arcfile_notDB')
                                    flash('New pipeline found in arc file and added into database','danger')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','danger')
                                    for idx, row in file_arc_def_only.iterrows():
                                        insertList = row.tolist()
                                        insertString = ""
                                        for item in insertList:
                                            if insertString:
                                                insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                            else:
                                                insertString = "\'" + item + "\'"
                                            print(insertString)
                                        cursor.execute("""insert tbl_NEMOI_Arc_Definitions(
                                                        from_hub,
                                                        to_hub,
                                                        arc_name,
                                                        arc_type,
                                                        topology,
                                                        corridor 
                                                        ) values (%s)""" % insertString)
                                        conn.commit()

                                    nnew_arc_def = db_arc_def.append(file_arc_def_only,ignore_index=True)
                                else:
                                    nnew_arc_def = db_arc_def.copy()

                                #- Then compare the hub list of from_hub + to_hub from arc file with list of hub from database
                                #-- hub list from arc def data
                                hubs = np.array(sorted(set(list(nnew_arc_def['from_hub']) + list(nnew_arc_def['to_hub']))))

                                hubs_from_db_hub_def = np.array(db_def_hub_df['hub'].unique())

                                hub_dif_arc_db = np.setdiff1d(hubs,hubs_from_db_hub_def)

                                hub_arc_only = pd.DataFrame(hub_dif_arc_db,columns=['hub'])
                                hub_arc_only['topology'] = topology
                                
                                #- if there are new hubs from arc definition, 
                                #- the new hubs will be added into the hub df from hub definition table in db
                                if len(hub_arc_only)>0:
                                    flash('New hubs found in arc file and added into database','danger')
                                    flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','danger')
                                    flash('Add more information in hub definition sheet and re-upload, if needed','danger')
                                    hub_arc_only.to_excel(writer_upload,sheet_name='H_in_arc_def_not_db')
                                    
                                    for idx, row in hub_arc_only.iterrows():
                                        insertList = row.tolist()
                                        insertString = ""
                                        for item in insertList:
                                            if insertString:
                                                insertString = insertString + ', ' + "\'" + str(item)  + "\'"
                                            else:
                                                insertString = "\'" + item + "\'"
                                            #print(insertString)
                                        cursor.execute("""insert tbl_NEMOI_Hub_Definition(    
                                                        hub, 
                                                        topology
                                                        ) values (%s)""" % insertString)
                                        conn.commit()

                                    #---read updated hub definition table from database

                                    sql = """SELECT * FROM tbl_NEMOI_Hub_Definition WHERE topology=%s"""
                                    new_db_def_hub_df = DB_table_data(conn, sql, topology)
                                else:
                                    new_db_def_hub_df = db_def_hub_df.copy()

                                #------------  
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM tbl_NEMOI_Arc_Tariffs WHERE case_id = %s AND topology=%s",(case_id,topology))
                                cursor.execute("DELETE FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id = %s AND topology=%s",(case_id,topology))
                                cursor.execute("DELETE FROM tbl_NEMOI_Arc_Constraints WHERE case_id = %s AND topology=%s",(case_id,topology))
                                conn.commit()
                                #- attach hub IDs for arc cost, arc cap, and arc def

                                #- arc cost
                                #-- attach hub IDs for from_hub   
                                arccost = pd.merge(arc_cost_ver,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'from_hub', right_on = 'hub')
                                arccost = arccost.rename(index=str, columns={"Unique_Hub_ID": "Unique_From_Hub_ID", "hub": "Fromhub"})
                                # print(arccost.head())
                                #-- attach hub IDs for to_hub
                                arc_cost = pd.merge(arccost,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'to_hub', right_on = 'hub')
                                arc_cost = arc_cost.rename(index=str, columns={"Unique_Hub_ID": "Unique_To_Hub_ID", "hub": "Tohub"})
                                arc_cost['topology'] = topology
                                arc_cost['case_id'] = case_id
                                arc_cost = arc_cost[['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub','to_hub',
                                                     'cost_pesoGJ','date','topology','case_id','arc_name']]
                                arc_cost['date'] = pd.to_datetime(arc_cost['date']).dt.date
                                arc_cost['date'] = arc_cost['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

                                #- arc cap
                                #-- attach hub IDs for from_hub   
                                arccap = pd.merge(arc_cap,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'from_hub', right_on = 'hub')

                                arccap = arccap.rename(index=str, columns={"Unique_Hub_ID": "Unique_From_Hub_ID", "hub": "Fromhub"})
                                #-- attach hub IDs for to_hub
                                arccap1 = pd.merge(arccap,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'to_hub', right_on = 'hub')
                                arccap1 = arccap1.rename(index=str, columns={"Unique_Hub_ID": "Unique_To_Hub_ID", "hub": "Tohub"})
                                arccap1['case_id'] = case_id
                                arccap1 = arccap1[['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub',
                                                  'to_hub','arc_name','online_date','ramp_up_months','capacity',
                                                  'comments','topology','case_id']]
                                arccap1['online_date'] = pd.to_datetime(arccap1['online_date']).dt.date
                                arccap1['online_date'] = arccap1['online_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                                arccap1['ramp_up_months'] = arccap1['ramp_up_months'].astype('object')
                                arccap1['capacity'] = arccap1['capacity'].astype('float')
                                arccap1 = arccap1.where((pd.notnull(arccap1)), 'None')
                                # print(arccap1.head())
                                #- arc flow constraints
                                #-- attach hub IDs for from_hub   
                                arc_flow_raw = pd.merge(arc_flow_raw_data,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'from_hub', right_on = 'hub')
                                
                                
                                arc_flow_raw = arc_flow_raw.rename(index=str, columns={"Unique_Hub_ID": "Unique_From_Hub_ID", "hub": "Fromhub"})
                                #-- attach hub IDs for to_hub
                                
                                arc_flow_raw1 = pd.merge(arc_flow_raw,new_db_def_hub_df[['hub','Unique_Hub_ID']] , how = 'left', left_on = 'to_hub', right_on = 'hub')
                                arc_flow_raw1 = arc_flow_raw1.rename(index=str, columns={"Unique_Hub_ID": "Unique_To_Hub_ID", "hub": "Tohub"})
                                
                                    
                                arc_flow_raw1['case_id'] = case_id
                                
                                
                                arc_flow_raw1.rename(columns = {'1':'Jan', '2':'Feb', 
                                                                '3':'Mar','4':'Apr',
                                                                '5':'May','6':'Jun',
                                                                '7':'Jul','8':'Aug',
                                                                '9':'Sept','10':'Oct',
                                                                '11':'Nov','12':'Dec'
                                                                 }, inplace = True) 


                                arc_flow_raw1 = arc_flow_raw1[['Unique_From_Hub_ID', 'Unique_To_Hub_ID','from_hub', 
                                                               'to_hub','arc_name', 'data_type', 'comments','case_id','topology', 'year',
                                                                'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',  
                                                                'Jul','Aug', 'Sept','Oct', 'Nov', 'Dec'
                                                               ]]

                                #- upload arc cost input dataframe to database
                                # insert dataframe into db
                                col_name = list(arc_cost.columns)
                                tbl_name = 'tbl_NEMOI_Arc_Tariffs'
                                insert_df_into_db(col_name, tbl_name, arc_cost)                                
                                #-- arc cap
                                # insert dataframe into db
                                col_name = list(arccap1.columns)
                                tbl_name = 'tbl_NEMOI_Arc_Pipeline_Infrastructure'
                                insert_df_into_db(col_name, tbl_name, arccap1)                                

                                #-- arc flow constraints
                                # insert dataframe into db
                                for col in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul','Aug', 'Sept','Oct', 'Nov', 'Dec']:
                                    arc_flow_raw1[col] = arc_flow_raw1[col].astype('float')
                                arc_flow_raw1['year'] = arc_flow_raw1['year'].astype('object')
                                arc_flow_raw2 = arc_flow_raw1.where((pd.notnull(arc_flow_raw1)), 'None')

                                col_name = list(arc_flow_raw2.columns)
                                tbl_name = 'tbl_NEMOI_Arc_Constraints'
                                insert_df_into_db(col_name, tbl_name, arc_flow_raw2)

                            else:
                                flash('The pipelines in capacity sheet are not the same with that in cost sheet','danger')
                                flash('Check saved excel file in //hou-file1/woodmac$/Winnie/NeMo_v1/','danger')
                                #flash('Please add information to the sheet with less records and upload again','danger')
                                if len(arc_cap_only)>0:
                                    flash('New pipelines found in arc capacity sheet','danger')
                                    arc_cap_only[['from_hub','to_hub']].to_excel(writer_upload,sheet_name='arcfile-pipe_in_cap_not_cost')
                                if len(arc_cost_only)>0:
                                    flash('New pipelines found in arc cost sheet','danger')
                                    arc_cost_only[['from_hub','to_hub']].to_excel(writer_upload,sheet_name='arcfile-pipe_in_cost_not_cap')
                        else:
                            flash('Please upload hub definition file for '+ topology,'warning')
                    else:
                        flash('No matched data for ' + topology + ' in '+ filename_arc)



                writer_upload.save()
                timeElapsed = datetime.datetime.now()-startTime
                flash('Time elapsed for uploading files (hh:mm:ss.ms):'+ str(timeElapsed),'info')

                """
                else:
                    flash("No file uploaded for this case",'danger')
                """

        else:
            flash("Haven't seleted a case to update")

    return render_template('update_input.html',form=form)

"""
class runmodel(FlaskForm):

        # Form fields
        form_name = HiddenField('Form Name')

        #choices = [("DEV","DEV")]
        case = SelectField('Select Case:' ,  id='select_case')

        start_year = SelectField('Starting Year:', id='select_start')

        end_year = SelectField('Ending Year:', id='select_end')

        topology = SelectField('Select Topology:')

        run_model = SelectField('Select Model:') 
"""       


@app.route('/run_nemo/',methods=['GET', 'POST'])
def run_nemo():
    """
    #run a model
    """
    #form = runmodel(form_name='runmodel')

    # create cursor
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    cursor = conn.cursor()

    # get all data from a table
    results = cursor.execute("SELECT * FROM case_info ")

    alldata = cursor.fetchall()

    # create a list for case_id
    case_id_list=list()

    for i in range(len(alldata)):
        element = alldata[i][-3]
        if element is not None:
            case_id_list.append(alldata[i][-3])
            
    unique_case=sorted(list(set(case_id_list)))
    unique_case.remove('DEV')

    data=[]
    for i in range(len(unique_case)):
        data.append({'name':unique_case[i]})


    if request.method == 'GET':
        # get Form Fields
        case = request.args.get('case')

        starting = request.args.get('start')
        
        ending = request.args.get('end')

        region = request.args.get('topology')

        M_model = request.args.get('NeMo_M (no Max Flow)')
        M_model_max = request.args.get('NeMo_M (with Max Flow)')

        Y_model_sto = request.args.get('NeMo_Calendar_Y_Sto (no Max Flow)')
        Y_model_max_sto = request.args.get('NeMo_Calendar_Y_Sto (with Max Flow)')
        Y_forward_model_sto = request.args.get('NeMo_Forward_12m_Sto (no Max Flow)')
        Y_forward_model_max_sto = request.args.get('NeMo_Forward_12m_Sto (with Max Flow)')

        Y_model_exp = request.args.get('NeMo_Calendar_Y_Exp (no Max Flow)')
        Y_model_max_exp = request.args.get('NeMo_Calendar_Y_Exp (with Max Flow)')
        Y_forward_model_exp = request.args.get('NeMo_Forward_12m_Exp (no Max Flow)')
        Y_forward_model_max_exp = request.args.get('NeMo_Forward_12m_Exp (with Max Flow)')

        Y_model_StoExp = request.args.get('NeMo_Calendar_Y_StoExp (no Max Flow)')
        Y_model_max_StoExp = request.args.get('NeMo_Calendar_Y_StoExp (with Max Flow)')
        Y_forward_model_StoExp = request.args.get('NeMo_Forward_12m_StoExp (no Max Flow)')
        Y_forward_model_max_StoExp = request.args.get('NeMo_Forward_12m_StoExp (with Max Flow)')


        T1 = request.args.get("T1")
        T11 = request.args.get("T11")
        T12 = request.args.get("T12")

        T2 = request.args.get("T2")
        T21 = request.args.get("T21")        
        T22 = request.args.get("T22")

        T3 = request.args.get("T3")
        T31 = request.args.get("T31")
        T32 = request.args.get("T32")
        
        
        # get input data from a certain case
        
        if str(ending)<str(starting):
            flash('Please re-select the ending year to be greater than the starting year')
        else:
            if case is not None and T12 is not None:
                T1 = str(T1)
                T11 = str(T11)
                T12 = str(T12)
                T2 = str(T2)
                T21 = str(T21)
                T22 = str(T22)
                T3 = str(T3)
                T31 = str(T31)
                T32 = str(T32)
                sum_cap_portion = float(T12) + float(T22) + float(T32)
                if sum_cap_portion != 1:
                    flash('Last run, capacity portions in Infrastructure Tariff Utilisation table is:'+' '+T12+','+T22+','+T32+'. '+'Their total not = 1, please re-run.','danger')
                else:
                    startTime4 = datetime.datetime.now()

                    startTime1 = datetime.datetime.now()

                    case = str(case)
                    starting = str(starting)
                    ending = str(ending)
                    region = str(region)

                    #- read input data

                    ## supply capacity
                    sql = """SELECT * FROM tbl_NEMOI_Supply_Capacity WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    supplycap = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    # print(supplycap.head())

                    ## supply cost
                    sql = """SELECT * FROM tbl_NEMOI_Supply_Cost WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    supplycost = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    
                    ## arc capacity
                    sql = """SELECT * FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id=%s and topology=%s"""
                    arc_cap_raw = DB_table_data(conn, sql, (str(case),str(region)))
                    #-------------------------------------------------------------------------------------
                    #- get time-series pipeline capacity based on data in 'tbl_NEMOI_Arc_Pipeline_Infrastructure' table
                    #- only get several columns for pipeline capacity df
                    arc_cap = arc_cap_raw[['from_hub','to_hub',
                                          'online_date','ramp_up_months','capacity']]
                    #- dates from 01/01/2000 to 12/1/2040
                    dates = pd.DataFrame({'date': pd.date_range(start='1/1/2000', end='12/1/2040', freq='MS')})

                    dates['join_key'] = 1
                    #- get information for unique pipeline

                    pipeline = arc_cap_raw.groupby(['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub','arc_name',
                                                     'to_hub','topology','case_id']).size().reset_index().rename(columns={0:'count'})
                    pipeline.drop('count', axis=1, inplace=True)
                    pipeline['join_key'] = 1
                    #- merge pipeline and dates to expand full dates for every pipeline 
                    pipelines = pd.merge(pipeline, dates, on='join_key', how='inner')
                    pipelines.drop('join_key', axis=1, inplace=True)
                    #- merge full dates pipeline information df with capacity df
                    #- forward fill the capacity value of pipeline time series df using the nearest not null capacity value 
                    arccap_ts = pd.merge(arc_cap,pipelines,left_on=['from_hub','to_hub','online_date'],right_on=['from_hub','to_hub','date'],how='right')

                    # Update 2018/09/25 --WF

                    # 1) find unique tuples for from_hub & to_hub
                    arc_tuple = arc_cap[['from_hub','to_hub']].drop_duplicates()

                    df = pd.DataFrame()
                    # 2) loop through every element in tuples to find subset of df for every arc, sort by date, 
                         # let the first capacity value (if nan) to equal 0 and fill the NAs with its precedent value

                    for row in arc_tuple.itertuples():
                        fromhub = row[1]
                        tohub = row[2]
                        arccap_t = arccap_ts[(arccap_ts['from_hub'] ==fromhub) & (arccap_ts['to_hub'] ==tohub )]
                        arccap_t1 = arccap_t.set_index(['date']).sort_index()
                        
                        if pd.isnull(arccap_t1[['capacity']].iloc[0])[0] == True:
                            cap_col_index = arccap_t1.columns.get_loc('capacity')
                            arccap_t1.iloc[[0], [cap_col_index]] = 0
                        else:
                            pass

                        arccap_t2 = arccap_t1.fillna(method='ffill')
                        df = df.append(arccap_t2)

                    df = df.reset_index()
                    arccap_ts2 = df[['Unique_From_Hub_ID','Unique_To_Hub_ID', 'from_hub', 'to_hub', 'arc_name',
                                     'date','capacity','topology', 'case_id']]

                    arccap_ts2['date'] = pd.to_datetime(arccap_ts2['date'],errors='coerce').dt.date
                    first = datetime.date(year=int(starting),month=1,day=1)
                    last = datetime.date(year=int(ending),month=12,day=1)
                    

                    arccap = arccap_ts2[(arccap_ts2['date']>=first) & (arccap_ts2['date']<=last)]            
                    #if len(arccap_raw)>0:
                     #   arccap = arccap_raw.groupby(["Unique_From_Hub_ID","Unique_To_Hub_ID","from_hub", "to_hub",'arc_name',"date",'case_id','topology'],as_index=False)['capacity'].max()
                    ## arc cost
                    #tStart4 = datetime.datetime.now()
                    sql = """SELECT * FROM tbl_NEMOI_Arc_Tariffs WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    arccost = DB_table_data(conn, sql, (str(case),str(region),starting,ending))

                    ## arc min flow
                    sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s and topology=%s and year between %s and %s and data_type='Min Flow'"""
                    arcmin_raw = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    m = {
                        'Jan': 1,
                        'Feb': 2,
                        'Mar': 3,
                        'Apr':4,
                        'May':5,
                        'Jun':6,
                        'Jul':7,
                        'Aug':8,
                        'Sept':9,
                        'Oct':10,
                        'Nov':11,
                        'Dec':12
                        }
                    arcmin=[]
                    if len(arcmin_raw)>0:
                        arcmin = pd.melt(arcmin_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                                       'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                                        var_name = 'month',value_name = 'min_flow')

                        arcmin.month = arcmin.month.map(m)
                        arcmin['day'] = 1
                        arcmin['date'] = pd.to_datetime(arcmin[['year','month','day']]).dt.date
                        arcmin['date'] = pd.to_datetime(arcmin['date'],errors='coerce',format = '%Y-%m-%d').dt.date 

                    
                    ## arc max flow
                    sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s and topology=%s and year between %s and %s and data_type='Max Flow'"""
                    arcmax_raw = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    if len(arcmax_raw)>0:
                        arcmax = pd.melt(arcmax_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                                        'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                                         var_name = 'month',value_name = 'max_flow')
                        arcmax.month = arcmax.month.map(m)
                        arcmax['day'] = 1
                        arcmax['date'] = pd.to_datetime(arcmax[['year','month','day']])
                        arcmax['date']=pd.to_datetime(arcmax['date'],errors='coerce',format = '%Y-%m-%d').dt.date  
                    else:
                        arcmax = []

                    ## tariff surcharges
                    tranche = [T1,T2,T3]
                    multiplier = [float(T11),float(T21),float(T31)]
                    capacity_portion = [float(T12),float(T22),float(T32)]
                    tariff_surc = pd.DataFrame({'tranche':tranche,'multiplier':multiplier,'capacity_portion':capacity_portion})   
                    tariff_surc.to_excel('tariff_surcharge.xlsx')

                    ## demand
                    sql = """SELECT * FROM tbl_NEMOI_Demand WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    dmd = DB_table_data(conn, sql, (str(case),str(region),starting,ending))

                    ## sto_par_df
                    sql = """SELECT * FROM tbl_NEMOI_Storage_Constraints WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    sto_par_df_db = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    sto_par_df_db = sto_par_df_db.rename(columns = {'max_injection':'max_inj','max_extraction':'max_ext','max_sto_capacity':'max_sto_cap','min_sto_capacity':'min_sto_cap'})

                    ## inj_cost
                    sql = """SELECT * FROM tbl_NEMOI_Storage_Injection WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    inj_cost_db = DB_table_data(conn, sql, (str(case),str(region),starting,ending)) 

                    ## ext_cost
                    sql = """SELECT * FROM tbl_NEMOI_Storage_Extraction WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    ext_cost_db = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
#---------------------------------------------
                    ## export price
                    sql = """SELECT * FROM tbl_NEMOI_Export_Price WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    exp_price_db = DB_table_data(conn, sql, (str(case),str(region),starting,ending))
                    ## export capacity
                    sql = """SELECT * FROM tbl_NEMOI_Export_Capacity WHERE case_id=%s and topology=%s and YEAR(date) between %s and %s"""
                    exp_cap_db = DB_table_data(conn, sql, (str(case),str(region),starting,ending))

                    timeElapsed1 = datetime.datetime.now()-startTime1
                    flash('Time elapsed for reading data from database (hh:mm:ss.ms):'+' '+str(timeElapsed1),'info')

                    startTime2 = datetime.datetime.now()

                    #1 all input data are available 
                    if len(arcmax) != 0 and len(sto_par_df_db) != 0 and len(exp_price_db) != 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        elif M_model_max:
                            run = run_model_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc)

                        elif Y_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = False)
                        elif Y_model_max_sto:
                            run = run_model_sto_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    sto_par_df_db, inj_cost_db, ext_cost_db,Forward12m = False)

                        elif Y_forward_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)
                        elif Y_forward_model_max_sto:
                            run = run_model_sto_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)

                        elif Y_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = False)
                        elif Y_model_max_exp:
                            run = run_model_exp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = True)
                        elif Y_forward_model_max_exp:
                            run = run_model_exp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    exp_price_db, exp_cap_db,Forward12m = True)

                        elif Y_model_StoExp:
                            run = run_model_StoExp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                   sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                   exp_price_db, exp_cap_db,Forward12m = False)
                        elif Y_model_max_StoExp:
                            run = run_model_StoExp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                       sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                       exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_StoExp:
                            run = run_model_StoExp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                   sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                   exp_price_db, exp_cap_db,Forward12m = True)

                        elif Y_forward_model_max_StoExp:
                            run = run_model_StoExp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                       sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                       exp_price_db, exp_cap_db,Forward12m = True)
                        else:
                            run = ''

                    #2 export input data are not available
                    elif len(arcmax) != 0 and len(sto_par_df_db) != 0 and len(exp_price_db) == 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        elif M_model_max:
                            run = run_model_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc)

                        elif Y_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = False)
                        elif Y_model_max_sto:
                            run = run_model_sto_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    sto_par_df_db, inj_cost_db, ext_cost_db,Forward12m = False)

                        elif Y_forward_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)
                        elif Y_forward_model_max_sto:
                            run = run_model_sto_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no export input data available, please select other NeMo model','warning')

                    #3 storage and export input data are both not available
                    elif len(arcmax) != 0 and len(sto_par_df_db) == 0 and len(exp_price_db) == 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        elif M_model_max:
                            run = run_model_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no export or storage input data available, please select other NeMo model','warning')

                    #4 storage input data are not available
                    elif len(arcmax) != 0 and len(sto_par_df_db) == 0 and len(exp_price_db) != 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        elif M_model_max:
                            run = run_model_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc)

                        elif Y_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = False)
                        elif Y_model_max_exp:
                            run = run_model_exp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = True)
                        elif Y_forward_model_max_exp:
                            run = run_model_exp_max(supplycap,supplycost,arccap,arccost,arcmin,arcmax,dmd,tariff_surc,
                                                    exp_price_db, exp_cap_db,Forward12m = True)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no storage input data available, please select other NeMo model','warning')

                    #5 max flow input data are not available
                    elif len(arcmax) == 0 and len(sto_par_df_db) != 0 and len(exp_price_db) != 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)

                        elif Y_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = False)

                        elif Y_forward_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)

                        elif Y_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = True)

                        elif Y_model_StoExp:
                            run = run_model_StoExp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                   sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                   exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_StoExp:
                            run = run_model_StoExp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                   sto_par_df_db, inj_cost_db, ext_cost_db, 
                                                   exp_price_db, exp_cap_db,Forward12m = True)

                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no max flow input data available, please select other NeMo model','warning')

                    #6 max flow and storage data are both not available
                    elif len(arcmax) == 0 and len(sto_par_df_db) == 0 and len(exp_price_db) != 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)

                        elif Y_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = False)

                        elif Y_forward_model_exp:
                            run = run_model_exp(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                exp_price_db, exp_cap_db,Forward12m = True)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no max flow or storage input data available, please select other NeMo model','warning')
                    
                    #7 max flow and export data are not available 
                    elif len(arcmax) == 0 and len(sto_par_df_db) != 0 and len(exp_price_db) == 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        elif Y_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = False)
                        elif Y_forward_model_sto:
                            run = run_model_sto(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc,
                                                sto_par_df_db, inj_cost_db, ext_cost_db, Forward12m = True)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no max flow or export input data available, please select other NeMo model','warning')
                    
                    #8 max flow & storage & export data are all not available
                    elif len(arcmax) == 0 and len(sto_par_df_db) == 0 and len(exp_price_db) == 0:

                        if M_model:
                            run = run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc)
                        else:
                            run = ''
                            flash('For the case selected, ','warning')
                            flash('no max flow, storage or export input data available, please select other NeMo model','warning')
                            
                    timeElapsed2 = datetime.datetime.now()-startTime2
                    flash('Time elapsed for running model (hh:mm:ss.ms):'+' '+str(timeElapsed2),'info')

                    #---------------------------------------------------------------------------------------
                    if len(run) > 0:
                        startTime3 = datetime.datetime.now()
                        # Pull data for results from 'run'

                        ## results for supply
                        supply_solved = run[0].reset_index()
                        ## results for demand
                        demand_solved = run[1].reset_index()
                        ## results for arcs
                        arcs_solved = run[2].reset_index()
                        ## solution status
                        solver_status = run[3]
                        solver_status['topology'] = region

                        # only export
                        if len(run) == 5 :
                            solved_export = run[4]
                        
                        if len(run) > 5:
                            # only storage
                            solved_injection = run[4]
                            solved_extraction = run[5]
                            solved_inventory =  run[6]
                            # both storage and export
                            if len(run) == 8:
                                # results for export
                                solved_export = run[7]
                            
                        flash('Please check: //hou-file1/woodmac$/Winnie/NeMo_v1/ for solver information if needed')
                        
                        if case !='DEV' and '2019' in case:

                            case_sp = case.split('-')
                            year = case_sp[0]
                            month = case_sp[1]
                            day = case_sp[2]
                            caseName = case_sp[4] 
                            case1 = year + '-' + month + '-' + day + '_' + caseName

                        elif case != 'DEV' and '2019' not in case:

                            case_sp = case.split(' ')
                            case1 = case_sp[0]
                            case2 = case_sp[1]
                            caseName = case1

                        else:
                            case1 = case
                            caseName = case1
                        
                        writer_model = pd.ExcelWriter(path + case1 + '-ModelRun_Info.xlsx', engine='xlsxwriter')
                        #solver_status.to_excel(path +case+'-'+ starting+'-'+ending+'_'+'solver_info.xlsx')

                        solver_status.to_excel(writer_model,sheet_name= starting+'-'+ending+'-'+'solver_info')
                        writer_model.save()

                        #-- re-order the columns and make sure the data type is the one put into the database
                        #- supply output
                        
                        supply_solved.capacity = supply_solved.capacity.astype(float)
                        supply_solved.cost = supply_solved.cost.astype(float)
                        supply_solved.production = supply_solved.production.astype(float)
                        supply_solved['Date'] = pd.to_datetime(supply_solved['date'], errors='coerce').dt.date
                        supply_solved['date'] = supply_solved['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # supply_solved.to_excel('supply_solved.xlsx')
                        #- demand output
                        
                        demand_solved.demand = demand_solved.demand.astype(float)
                        demand_solved.price = demand_solved.price.astype(float)

                        demand_solved['Date'] = pd.to_datetime(demand_solved['date'], errors='coerce').dt.date
                        demand_solved['date'] = demand_solved['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        # demand_solved.to_excel('demand_solved.xlsx')
                        #- pipline output
                        
                        arcs_solved.capacity = arcs_solved.capacity.astype(float)
                        arcs_solved.flow = arcs_solved.flow.astype(float)
                        arcs_solved['Date'] = pd.to_datetime(arcs_solved['date'], errors='coerce').dt.date
                        arcs_solved['date'] = arcs_solved['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        arcs_solved['utilisation'].fillna(0, inplace=True)
                        arcs_solved.to_excel('arcs_solved.xlsx')
                        #-----------------------------
                        if len(run) == 5:
                            #- export
                            
                            solved_export.FOB_price = solved_export.FOB_price.astype(float)
                            solved_export.capacity = solved_export.capacity.astype(float)
                            solved_export.gas_export = solved_export.gas_export.astype(float)
                            solved_export['Date'] = pd.to_datetime(solved_export['date'], errors='coerce').dt.date
                            solved_export['date'] = solved_export['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        if len(run) > 5:
                            #- storage: injection
                            
                            solved_injection.rename(columns = {'inj_cost':'injection_cost'}, inplace = True)
                            solved_injection.to_excel('solved_injection.xlsx')
                            solved_injection.injection_cost = solved_injection.injection_cost.astype(float)
                            solved_injection.gas_injection = solved_injection.gas_injection.astype(float)
                            solved_injection['Date'] = pd.to_datetime(solved_injection['date'], errors='coerce').dt.date
                            solved_injection['date'] = solved_injection['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            #- storage: extraction
                            
                            solved_extraction.rename(columns = {'ext_cost':'extraction_cost'}, inplace = True) 
                            solved_extraction.extraction_cost = solved_extraction.extraction_cost.astype(float)
                            solved_extraction.gas_extraction = solved_extraction.gas_extraction.astype(float)
                            solved_extraction['Date'] = pd.to_datetime(solved_extraction['date'], errors='coerce').dt.date
                            solved_extraction['date'] = solved_extraction['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            
                            #- storage: inventory
                            
                            solved_inventory.rename(columns = {'max_inj':'max_injection','max_ext':'max_extraction',
                                                                                  'max_sto_cap':'max_sto_capacity','min_sto_cap':'min_sto_capacity'}, inplace = True)
                            solved_inventory.storing_cost = solved_inventory.storing_cost.astype(float)
                            solved_inventory.max_extraction = solved_inventory.max_extraction.astype(float)
                            solved_inventory.max_injection = solved_inventory.max_injection.astype(float)
                            solved_inventory.max_sto_capacity = solved_inventory.max_sto_capacity.astype(float)
                            solved_inventory.min_sto_capacity = solved_inventory.min_sto_capacity.astype(float)
                            solved_inventory.gas_inventory = solved_inventory.gas_inventory.astype(float)
                            solved_inventory['Date'] = pd.to_datetime(solved_inventory['date'], errors='coerce').dt.date
                            solved_inventory['date'] = solved_inventory['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            
                            if len(run) == 8:
                                #- export
                                solved_export = solved_export[['Unique_Hub_ID', 'Unique_ExpNode_ID', 'hub', 'node', 
                                                                'date', 'case_id', 'topology', 'FOB_price', 'capacity',
                                                                'gas_export']]
                                solved_export.FOB_price = solved_export.FOB_price.astype(float)
                                solved_export.capacity = solved_export.capacity.astype(float)
                                solved_export.gas_export = solved_export.gas_export.astype(float)
                                solved_export['Date'] = pd.to_datetime(solved_export['date'], errors='coerce').dt.date
                                solved_export['date'] = solved_export['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                        #-------------------------------------------
                        # Since all the outputs: supply, arcs, demand, plus storage and export all have the same dates, 
                        # here only need to check the list of year from one of the outputs
                        #-------------------------------------------------

                        ## the list of year from this run's results
                        year_solved = list(set( pd.to_datetime(arcs_solved['Date']).dt.year))

                        #---------------------------------------
                        # check whether there are existing outputs in db for the case just ran, 
                        # random pick up data from one of three output tables
                        # (other tables such as supply output table and demand output table of the current case also could be picked) 
                        ## arc flow output table
                        sql = """SELECT * FROM tbl_NEMOO_Supply WHERE case_id=%s and topology=%s"""
                        arc_out_existing_df = DB_table_data(conn, sql, (case,region))
                        if len(arc_out_existing_df):
                            year_existing = list(set(pd.to_datetime(arc_out_existing_df['date']).dt.year))
                        else:
                            year_existing=[]
                        print (year_existing)
                        print (year_solved)

                        # pull the demand and supply definition, and XRF_States data from database
                        ## tbl_NEMOI_Demand_Node_Definitions
                        sql_dmd_def = """SELECT * FROM tbl_NEMOI_Demand_Node_Definitions"""
                        df_dmd_def = DB_table_data(conn, sql_dmd_def)

                        sql_sup_def = """SELECT * FROM tbl_NEMOI_Supply_Node_Definition"""
                        df_sup_def = DB_table_data(conn, sql_sup_def)

                        sql_XRF = """SELECT * FROM tbl_XRF_States """
                        df_XRF = DB_table_data(conn, sql_XRF)

                        sql_hub_def = """SELECT * FROM tbl_NEMOI_Hub_Definition """
                        df_hub_def = DB_table_data(conn, sql_hub_def)

                        for year in year_solved:
                            # cut output data for that year and update data of that year in database
                            begin = datetime.date(year=int(year),month=1,day=1)
                            end = datetime.date(year=int(year),month=12,day=1)
                            str_begin = begin.strftime("%m-%Y")
                            str_end = end.strftime("%m-%Y") 

                            ## yearly output
                            supply_solved_yearly = supply_solved[(supply_solved['Date']>=begin) & (supply_solved['Date']<=end)]
                            supply_solved_yearly = supply_solved_yearly[['Unique_SupplyNode_ID','Unique_Hub_ID',
                                                                         'node','hub','date','capacity','cost',
                                                                         'production','case_id','topology']]
                            

                            demand_solved_yearly = demand_solved[(demand_solved['Date']>=begin) & (demand_solved['Date']<=end)]
                            demand_solved_yearly = demand_solved_yearly[['Unique_Node_ID','Unique_Hub_ID','node',
                                                                         'hub','date','demand','price','topology','case_id'
                                                                       ]]
                            

                            arcs_solved_yearly = arcs_solved[(arcs_solved['Date']>=begin) & (arcs_solved['Date']<=end)]
                            arcs_solved_yearly = arcs_solved_yearly[['from_hub','to_hub','arc_name','date',
                                                       'capacity','flow','utilisation','topology','case_id',
                                                       'str_date','Unique_From_Hub_ID','Unique_To_Hub_ID']]
                            
                            #- only export 
                            if len(run) == 5:
                                solved_export_yearly = solved_export[(solved_export['Date']>=begin) & (solved_export['Date']<=end)]
                                solved_export_yearly = solved_export_yearly[['Unique_Hub_ID', 'Unique_ExpNode_ID', 'hub',  
                                                                             'node','date', 'case_id', 'topology', 'FOB_price', 
                                                                             'capacity','gas_export']]
                                
                            #- storage
                            if len(run) > 5:
                                solved_inventory_yearly = solved_inventory[(solved_inventory['Date']>=begin) & (solved_inventory['Date']<=end)]
                                solved_inventory_yearly = solved_inventory_yearly[['Unique_Sto_ID', 'sto_facility', 'date', 'case_id', 'topology', 
                                                                                   'storing_cost', 'max_injection', 'max_extraction', 
                                                                                   'max_sto_capacity','min_sto_capacity', 'gas_inventory']]
                    

                                solved_extraction_yearly = solved_extraction[(solved_extraction['Date']>=begin) & (solved_extraction['Date']<=end)]
                                solved_extraction_yearly = solved_extraction_yearly[['Unique_Sto_ID', 'Unique_Hub_ID', 'sto_facility', 'hub',  
                                                                       'date', 'case_id', 'topology', 'extraction_cost', 'gas_extraction']]
                                

                                solved_injection_yearly = solved_injection[(solved_injection['Date']>=begin) & (solved_injection['Date']<=end)]
                                solved_injection_yearly = solved_injection_yearly[['Unique_Hub_ID', 'Unique_Sto_ID', 'hub', 'sto_facility', 
                                                                  'date', 'case_id', 'topology', 'injection_cost', 'gas_injection']]
                                if len(run) == 8:
                                    solved_export_yearly = solved_export[(solved_export['Date']>=begin) & (solved_export['Date']<=end)]
                                    solved_export_yearly = solved_export_yearly[['Unique_Hub_ID', 'Unique_ExpNode_ID', 'hub',  
                                                                             'node','date', 'case_id', 'topology', 'FOB_price', 
                                                                             'capacity','gas_export']]

                            if year in year_existing:
                                ### error need delete first
                                Y = str(year)
                                ## update data of output tables in database

                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM tbl_NEMOO_Supply WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                cursor.execute("DELETE FROM tbl_NEMOO_Arc_Flows WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                cursor.execute("DELETE FROM tbl_NEMOO_Demand WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))

                                cursor.execute("DELETE FROM tbl_NEMOO_Storage_Inventory WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                cursor.execute("DELETE FROM tbl_NEMOO_Storage_Injection WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                cursor.execute("DELETE FROM tbl_NEMOO_Storage_Extraction WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                cursor.execute("DELETE FROM tbl_NEMOO_Export WHERE YEAR(date) = %s and case_id = %s and topology = %s",(Y,case,region))
                                conn.commit()
                                

                            ## insert data of output tables in database

                            ### yearly supply output
                            #### insert yearly supply output into db
                            col_name = list(supply_solved_yearly.columns)
                            tbl_name = 'tbl_NEMOO_Supply'
                            insert_df_into_db(col_name, tbl_name, supply_solved_yearly)

                            ### yearly arc output
                            #### insert yearly arc output into db
                            col_name = list(arcs_solved_yearly.columns)
                            tbl_name = 'tbl_NEMOO_Arc_Flows'
                            insert_df_into_db(col_name, tbl_name, arcs_solved_yearly)
                            ### yearly demand output 
                            #### insert yearly demand output into db
                            col_name = list(demand_solved_yearly.columns)
                            tbl_name = 'tbl_NEMOO_Demand'
                            insert_df_into_db(col_name, tbl_name, demand_solved_yearly)

                            ## load data to tbl_DB_Demand
                            demand_solved_yearly['Date'] = pd.to_datetime(demand_solved_yearly['date'])
                            merge_dmd1 = pd.merge(demand_solved_yearly, df_dmd_def, on=['Unique_Node_ID','topology', 'node', 'hub'], how='inner')
                            merge_dmd2 = pd.merge(merge_dmd1, df_hub_def, on=['Unique_Hub_ID', 'hub','topology'], how='inner')
                            merge_dmd3 = pd.merge(merge_dmd2, df_XRF, left_on=['state','country'], right_on=['State Name','Country'], how='left')
                            merge_dmd3['Year'],merge_dmd3['Month'],merge_dmd3['future_date'] = [merge_dmd3['Date'].dt.year, merge_dmd3['Date'].dt.month, (merge_dmd3['Date'] + pd.DateOffset(months=1))]
                            merge_dmd3['demand_volume'] = merge_dmd3['demand']*(merge_dmd3['future_date'] - merge_dmd3['Date']).dt.days
                            merge_dmd3['demand_rate'] = merge_dmd3['demand'].copy()
                            merge_dmd4 = merge_dmd3[['hub_report_name','hub','node','super_hub','State Name','Region','gas_region',
                                             'country','sector','date','Year','Month','case_id','demand_volume','demand_rate']]
                    
                            merge_dmd4['Year'] = merge_dmd4['Year'].astype(float)
                            merge_dmd4['Month']= merge_dmd4['Month'].astype(float)

                            # insert dataframe into db
                            col_name = list(merge_dmd4.columns)
                            tbl_name = 'tbl_DB_Demand'
                            insert_df_into_db(col_name, tbl_name, merge_dmd4)

                            ## load data to tbl_SB_Supply
                            supply_solved_yearly['Date'] = pd.to_datetime(supply_solved_yearly['date'])

                            merge_sup1 = pd.merge(supply_solved_yearly, df_sup_def, on=['Unique_SupplyNode_ID','topology', 'node', 'hub'], how='inner')
                            merge_sup2 = pd.merge(merge_sup1, df_hub_def, on=['Unique_Hub_ID', 'hub','topology'], how='inner')
                            merge_sup3 = pd.merge(merge_sup2, df_XRF, left_on=['state','country'], right_on=['State Name','Country'], how='left')
                            merge_sup3['Year'],merge_sup3['Month'],merge_sup3['future_date'] = \
                               [merge_sup3['Date'].dt.year, merge_sup3['Date'].dt.month, (merge_sup3['Date'] + pd.DateOffset(months=1))]
                            merge_sup3['production_volume'] = merge_sup3['production']*(merge_sup3['future_date'] - merge_sup3['Date']).dt.days
                            merge_sup3['production_rate'] = merge_sup3['production'].copy()
                            merge_sup4 = merge_sup3[['hub_report_name','hub','node','super_hub','State Name','Region','gas_region',
                                                     'country','supply_source','supply_type','date','Year','Month','case_id',
                                                     'production_volume','production_rate']]
                            # merge_sup4['date'] = merge_sup4['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            merge_sup4['Year'] = merge_sup4['Year'].astype(float)
                            merge_sup4['Month']= merge_sup4['Month'].astype(float)
                            merge_sup4['Region'] = merge_sup4['Region'].astype(object).where(pd.notnull(merge_sup4['Region']), None)
                            merge_sup4['State Name'] = merge_sup4['State Name'].astype(object).where(pd.notnull(merge_sup4['State Name']), None)


                            ## insert dataframe into db
                            col_name = list(merge_sup4.columns)
                            tbl_name = 'tbl_SB_Supply'
                            insert_df_into_db(col_name, tbl_name, merge_sup4)

                            ## load data to tbl_PR_Hub_Prices
                            df_dmd1 = demand_solved_yearly[demand_solved_yearly['demand']>=0.001]
                            merge_p1 = pd.merge(df_dmd1, df_hub_def, on=['Unique_Hub_ID', 'hub','topology'], how='inner')
                            merge_p2 = pd.merge(merge_p1, df_XRF, left_on=['state','country'], right_on=['State Name','Country'], how='left')
                            merge_p2['Year'],merge_p2['Month'] = [merge_p2['Date'].dt.year, merge_p2['Date'].dt.month]
                            # df1
                            merge_p21 = merge_p2[['Country','date','Year','Month','case_id','price','demand']].copy()
                            merge_p21['region_type'],merge_p21['amount'] = ['country',merge_p21['demand']*merge_p21['price']]
                            merge_p21 = merge_p21.rename(columns={"Country": "region"})
                            # df2
                            merge_p22 = merge_p2[['gas_region','date','Year','Month','case_id','price','demand']].copy()
                            merge_p22['region_type'],merge_p22['amount'] = ['gas_region',merge_p22['demand']*merge_p22['price']]
                            merge_p22 = merge_p22.rename(columns={"gas_region": "region"})

                            # df3
                            merge_p23 = merge_p2[['Region','date','Year','Month','case_id','price','demand']].copy()
                            merge_p23['region_type'],merge_p23['amount'] = ['region',merge_p23['demand']*merge_p23['price']]
                            merge_p23 = merge_p23.rename(columns={"Region": "region"})

                            # df4
                            merge_p24 = merge_p2[['hub','date','Year','Month','case_id','price','demand']].copy()
                            merge_p24['region_type'],merge_p24['amount'] = ['hub',merge_p24['demand']*merge_p24['price']]
                            merge_p24 = merge_p24.rename(columns={"hub": "region"})
                            # df5
                            merge_p25 = merge_p2[['hub_report_name','date','Year','Month','case_id','price','demand']].copy()
                            merge_p25['region_type'],merge_p25['amount'] = ['hub_report_name',merge_p25['demand']*merge_p25['price']]
                            merge_p25 = merge_p25.rename(columns={"hub_report_name": "region"})
                            # df6
                            merge_p26 = merge_p2[['super_hub','date','Year','Month','case_id','price','demand']].copy()
                            merge_p26['region_type'],merge_p26['amount'] = ['super_hub',merge_p26['demand']*merge_p26['price']]
                            merge_p26 = merge_p26.rename(columns={"super_hub": "region"})

                            # df7
                            merge_p27 = merge_p2[['State Name','date','Year','Month','case_id','price','demand']].copy()
                            merge_p27['region_type'],merge_p27['amount'] = ['state',merge_p27['demand']*merge_p27['price']]
                            merge_p27 = merge_p27.rename(columns={"State Name": "region"})

                            df_final = merge_p21.append([merge_p22, merge_p23, merge_p24,
                                                         merge_p25,merge_p26,merge_p27])

                            gb_merge_p_final = df_final.groupby(['Year','Month','date','case_id','region','region_type'])
                            agg_merge_p_final = gb_merge_p_final.agg({'price':['mean','sum'],'demand':['sum'],'amount':['sum']})
                            agg_merge_p_final.columns = ["_".join(x) for x in agg_merge_p_final.columns.ravel()] 
                            agg_merge_p_final['wtd_avg_price'] = agg_merge_p_final['amount_sum']/agg_merge_p_final['demand_sum']
                            agg_merge_p_final = agg_merge_p_final.rename(columns={"price_mean": "price","demand_sum":"demand"})
                            agg_merge_p_final = agg_merge_p_final.reset_index()

                            hub_p_final\
                            = agg_merge_p_final[['region','region_type','date','Year', 'Month','case_id','price','demand','wtd_avg_price']].copy()
                            # hub_p_final['date'] = hub_p_final['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                            hub_p_final['Year'] = hub_p_final['Year'].astype(float)
                            hub_p_final['Month']= hub_p_final['Month'].astype(float)

                            ## insert dataframe into db
                            col_name = list(hub_p_final.columns)
                            tbl_name = 'tbl_PR_Hub_Prices'
                            insert_df_into_db(col_name, tbl_name, hub_p_final)

                            if len(run) == 5:
                                ## yearly export output   
                                #Unique_Hub_ID, Unique_ExpNode_ID, hub, node, date, case_id, topology, FOB_price, capacity, gas_export
                                # insert dataframe into db
                                col_name = list(solved_export_yearly.columns)
                                tbl_name = 'tbl_NEMOO_Export'
                                insert_df_into_db(col_name, tbl_name, solved_export_yearly)

                            if len(run) > 5:
                                ## yearly storage output -- inventory
                                # insert dataframe into db
                                col_name = list(solved_inventory_yearly.columns)
                                tbl_name = 'tbl_NEMOO_Storage_Inventory'
                                insert_df_into_db(col_name, tbl_name, solved_inventory_yearly)

                                ## yearly storage output -- injection
                                # insert dataframe into db
                                col_name = list(solved_injection_yearly.columns)
                                tbl_name = 'tbl_NEMOO_Storage_Injection'
                                insert_df_into_db(col_name, tbl_name, solved_injection_yearly)

                                ## yearly storage output -- extraction
                                # insert dataframe into db
                                col_name = list(solved_extraction_yearly.columns)
                                tbl_name = 'tbl_NEMOO_Storage_Extraction'
                                insert_df_into_db(col_name, tbl_name, solved_extraction_yearly)

                                if len(run) == 8:                              
                                    ## yearly export output   
                                    #Unique_Hub_ID, Unique_ExpNode_ID, hub, node, date, case_id, topology, FOB_price, capacity, gas_export
                                    # insert dataframe into db
                                    col_name = list(solved_export_yearly.columns)
                                    tbl_name = 'tbl_NEMOO_Export'
                                    insert_df_into_db(col_name, tbl_name, solved_export_yearly)

                        timeElapsed3 = datetime.datetime.now()-startTime3 
                        print('Time elapsed for uploading outputs (hh:mm:ss.ms) {}'.format(timeElapsed3))
                        flash('Time elapsed for uploading outputs to database (hh:mm:ss.ms):'+' '+str(timeElapsed3),'info')
                        timeElapsed4 = datetime.datetime.now()-startTime4
                        flash('Time elapsed in total (hh:mm:ss.ms):'+' '+str(timeElapsed4),'info')
                    #writer_model.save()

    return render_template('run_nemo.html',  data=data)

@app.route('/_get_start/')
def _get_start():
    case = request.args.get('case', '01', type=str)
    print(case)
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    cur = conn.cursor()
    result = cur.execute("SELECT * FROM case_info where case_id=%s",str(case))
    data = cur.fetchall()
    start_point = int(data[0][-2])
    end_point = int(data[0][-1])

    starts = [(i, i) for i in range(start_point,end_point+1)]
    return jsonify(starts)


@app.route('/_get_end/')
def _get_end():
    case = request.args.get('case', '01', type=str)
    print(case)
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    cur = conn.cursor()
    result = cur.execute("SELECT * FROM case_info where case_id=%s",str(case))
    data = cur.fetchall()
    start_point = int(data[0][-2])
    end_point = int(data[0][-1])

    ends_raw = [(i, i) for i in range(start_point,end_point+1)]
    ends = list(reversed(ends_raw))
    
    return jsonify(ends)
"""
class updateDEV(FlaskForm):
        # Form fields
        form_name = HiddenField('Form Name')
        case = SelectField('Case Used to Update DEV:', id='select_case')
        start_year = SelectField('The Update Starting from:', id='select_start')
        end_year = SelectField('The Update ending in:', id='select_end')
        topology = SelectField('Topology Updated:')
        #case = SelectField(':',choices=[(True, 'Yes'), (False, 'No')])
"""
class delcase(FlaskForm):
        # Form fields
        delcase = SelectField('Delete Case:')

@app.route('/delete',methods=['GET', 'POST'])
def delete_case():
    form = delcase()
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    cursor = conn.cursor()

    # get all data from a table
    results = cursor.execute("SELECT * FROM case_info ")

    alldata = cursor.fetchall()

    # create a list for case_id
    case_id_list=list()

    for i in range(len(alldata)):
        case_id_list.append(alldata[i][-3])
    unique_case=sorted(list(set(case_id_list)))
    unique_case.remove('DEV')

    choices=[(i, i) for i in unique_case] 
    
    form.delcase.choices = choices
    if request.method == 'POST' and form.validate():
        case = form.delcase.data
        case = str(case)
    #- Inputs
        #- supply capacity
        sql = """SELECT * FROM tbl_NEMOI_Supply_Capacity WHERE case_id=%s"""
        supplycap = DB_table_data(conn, sql, case)
        
        #- supply cost
        sql = """SELECT * FROM tbl_NEMOI_Supply_Cost WHERE case_id = %s"""
        supplycost = DB_table_data(conn, sql, case)

        #- arc capacity
        sql = """SELECT * FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id=%s"""
        arc_cap_raw = DB_table_data(conn, sql, case)
        
        if len(arc_cap_raw)>0:
            #-------------------------------------------------------------------------------------
            #- get time-series pipeline capacity based on data in 'tbl_NEMOI_Arc_Pipeline_Infrastructure' table
            #- only get several columns for pipeline capacity df
            arc_cap = arc_cap_raw[['from_hub','to_hub',
                                  'online_date','ramp_up_months','capacity']]
            #- dates from 01/01/2000 to 12/1/2040
            dates = pd.DataFrame({'date': pd.date_range(start='1/1/2000', end='12/1/2040', freq='MS')})

            dates['join_key'] = 1
            #- get information for unique pipeline

            pipeline = arc_cap_raw.groupby(['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub','arc_name',
                                             'to_hub','topology','case_id']).size().reset_index().rename(columns={0:'count'})
            pipeline.drop('count', axis=1, inplace=True)
            pipeline['join_key'] = 1
            #- merge pipeline and dates to expand full dates for every pipeline 
            pipelines = pd.merge(pipeline, dates, on='join_key', how='inner')
            pipelines.drop('join_key', axis=1, inplace=True)
            #- merge full dates pipeline information df with capacity df
            #- forward fill the capacity value of pipeline time series df using the nearest not null capacity value 
            arccap_ts = pd.merge(arc_cap,pipelines,left_on=['from_hub','to_hub','online_date'],right_on=['from_hub','to_hub','date'],how='right')


            # Update 2018/09/25 --WF
            # 1) find unique tuples for from_hub & to_hub
            arc_tuple = arc_cap[['from_hub','to_hub']].drop_duplicates()

            df = pd.DataFrame()
            # 2) loop through every element in tuples to find subset of df for every arc, sort by date, 
            #    let the first capacity value (if nan) to equal 0 and fill the NAs with its precedent value

            for row in arc_tuple.itertuples():
                fromhub = row[1]
                tohub = row[2]
                arccap_t = arccap_ts[(arccap_ts['from_hub'] ==fromhub) & (arccap_ts['to_hub'] ==tohub )]
                arccap_t1 = arccap_t.set_index(['date']).sort_index()
                if pd.isnull(arccap_t1[['capacity']].iloc[0])[0] == True:
                    cap_col_index = arccap_t1.columns.get_loc('capacity')
                    arccap_t1.iloc[[0], [cap_col_index]] = 0
                else:
                    pass

                arccap_t2 = arccap_t1.fillna(method='ffill')
                df = df.append(arccap_t2)

            df = df.reset_index()
            arccap_ts2 = df[['Unique_From_Hub_ID','Unique_To_Hub_ID', 'from_hub', 'to_hub', 'arc_name',
                       'date','capacity','topology', 'case_id']]   
            arccap_ts2['date'] = pd.to_datetime(arccap_ts2['date'],errors='coerce').dt.date 

        #- arc cost
        sql = """SELECT * FROM tbl_NEMOI_Arc_Tariffs WHERE case_id=%s"""
        arccost = DB_table_data(conn, sql, case)

        #- arc min flow
        sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s and data_type='Min Flow'"""
        arcmin_raw = DB_table_data(conn, sql, case)
        m = {
            'Jan': 1,
            'Feb': 2,
            'Mar': 3,
            'Apr':4,
            'May':5,
            'Jun':6,
            'Jul':7,
            'Aug':8,
            'Sept':9,
            'Oct':10,
            'Nov':11,
            'Dec':12
            }
        arcmin=[]
        if len(arcmin_raw)>0:
            arcmin = pd.melt(arcmin_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                           'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                            var_name = 'month',value_name = 'min_flow')

            arcmin.month = arcmin.month.map(m)
            arcmin['day'] = 1
            arcmin['date'] = pd.to_datetime(arcmin[['year','month','day']])
            arcmin['date'] = pd.to_datetime(arcmin['date'],errors='coerce',format = '%Y-%m-%d').dt.date 
        
        #- arc max flow
        sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s and data_type='Max Flow'"""
        arcmax_raw = DB_table_data(conn, sql, case)

        if len(arcmax_raw)>0:
            arcmax = pd.melt(arcmax_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                            'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                             var_name = 'month',value_name = 'max_flow')
            arcmax.month = arcmax.month.map(m)
            arcmax['day'] = 1
            arcmax['date'] = pd.to_datetime(arcmax[['year','month','day']])
            arcmax['date']=pd.to_datetime(arcmax['date'],errors='coerce',format = '%Y-%m-%d').dt.date  
        else:
            arcmax = []

        #- demand
        sql = """SELECT * FROM tbl_NEMOI_Demand WHERE case_id=%s"""
        dmd = DB_table_data(conn, sql, case)

        ## sto_par_df
        sql = """SELECT * FROM tbl_NEMOI_Storage_Constraints WHERE case_id=%s"""
        sto_par_df_db = DB_table_data(conn, sql, case)
        sto_par_df_db = sto_par_df_db.rename(columns = {'max_injection':'max_inj','max_extraction':'max_ext','max_sto_capacity':'max_sto_cap','min_sto_capacity':'min_sto_cap'})

        ## inj_cost
        sql = """SELECT * FROM tbl_NEMOI_Storage_Injection WHERE case_id=%s"""
        inj_cost_db = DB_table_data(conn, sql, case)

        ## ext_cost
        sql = """SELECT * FROM tbl_NEMOI_Storage_Extraction WHERE case_id=%s"""
        ext_cost_db = DB_table_data(conn, sql, case)

        ## export price
        sql = """SELECT * FROM tbl_NEMOI_Export_Price WHERE case_id=%s"""
        exp_price_db = DB_table_data(conn, sql, case)

        ## export capacity
        sql = """SELECT * FROM tbl_NEMOI_Export_Capacity WHERE case_id=%s"""
        exp_cap_db = DB_table_data(conn, sql, case)

    #- Outputs
        #- supply output
        sql = """SELECT * FROM tbl_NEMOO_Supply WHERE case_id=%s"""
        supout = DB_table_data(conn, sql, case)

        #- arc output
        sql = """SELECT * FROM tbl_NEMOO_Arc_Flows WHERE case_id=%s"""
        arcout = DB_table_data(conn, sql, case)

        #- demand output
        sql = """SELECT * FROM tbl_NEMOO_Demand WHERE case_id=%s"""
        dmdout = DB_table_data(conn, sql, case)

        #- storage inventory
        sql = """SELECT * FROM tbl_NEMOO_Storage_Inventory WHERE case_id=%s"""
        sto_invt_out = DB_table_data(conn, sql, case)

        #- storage Injection
        sql = """SELECT * FROM tbl_NEMOO_Storage_Injection WHERE case_id=%s"""
        sto_inj_out = DB_table_data(conn, sql, case)

        #- storage Extraction
        sql = """SELECT * FROM tbl_NEMOO_Storage_Extraction WHERE case_id=%s"""
        sto_ext_out = DB_table_data(conn, sql, case)

        #- export
        sql = """SELECT * FROM tbl_NEMOO_Export WHERE case_id=%s"""
        exp_out = DB_table_data(conn, sql, case)


    #- export all the inputs and outputs into excel file-----
    
        if case !='DEV' and '2019' in case:

            case_sp = case.split('-')
            year = case_sp[0]
            month = case_sp[1]
            day  = case_sp[2]
            caseName = case_sp[4]
            case1 = year + '-' + month + '-' + day + '_' + caseName

        elif case !='DEV' and '2019' not in case:
            
            case_sp = case.split(' ')
            case1 = case_sp[0]
            case2 = case_sp[1]
        else:
            case1 = case 

        writer_save_case = pd.ExcelWriter(path_save_delete + case1 + '-In&Output_Saved.xlsx', engine='xlsxwriter') 
        #- supply input
        if len(supplycap)>0:
            supplycap.to_excel(writer_save_case,sheet_name = 'Input_Supply_Capacity')
            supplycost.to_excel(writer_save_case,sheet_name = 'Input_Supply_Cost')
        #- arc input
        if len(arc_cap)>0:
            arc_cap.to_excel(writer_save_case,sheet_name = 'Input_Arc_Capacity')
            arccap_ts2.to_excel(writer_save_case,sheet_name = 'Input_Arc_Capacity_Vert')
            arccost.to_excel(writer_save_case,sheet_name = 'Input_Arc_Cost')

        if len(arcmin)>0:
            arcmin.to_excel(writer_save_case,sheet_name = 'Input_Arc_Minflow')
        if len(arcmax)>0:
            arcmax.to_excel(writer_save_case,sheet_name = 'Input_Arc_Maxflow')

        #- demand input
        if len(dmd)>0:
            dmd.to_excel(writer_save_case,sheet_name = 'Input_Demand')
        
        #- storage input
        if len(sto_par_df_db)>0:
            sto_par_df_db.to_excel(writer_save_case,sheet_name = 'Input_Sto_Constraints')
        if len(inj_cost_db)>0:
            inj_cost_db.to_excel(writer_save_case,sheet_name = 'Input_Sto_Inj_Cost')
        if len(ext_cost_db)>0:
            ext_cost_db.to_excel(writer_save_case,sheet_name = 'Input_Sto_Ext_Cost')

        #- export input
        if len(exp_price_db)>0:
            exp_price_db.to_excel(writer_save_case,sheet_name = 'Input_Export_Price')
            exp_cap_db.to_excel(writer_save_case,sheet_name = 'Input_Export_Capacity')

        #- supply and demand output
        if len(supout)>0:
            supout.to_excel(writer_save_case,sheet_name = 'Output_Supply')
            arcout.to_excel(writer_save_case,sheet_name = 'Output_Arc')
            dmdout.to_excel(writer_save_case,sheet_name = 'Output_Demand')

        #- storage output
        if len(sto_invt_out)>0:
            sto_invt_out.to_excel(writer_save_case,sheet_name = 'Output_Sto_Inventory')
            sto_inj_out.to_excel(writer_save_case,sheet_name = 'Output_Sto_Injection')
            sto_ext_out.to_excel(writer_save_case,sheet_name = 'Output_Sto_Extraction')

        #- export output
        if len(exp_out)>0:
            exp_out.to_excel(writer_save_case,sheet_name = 'Output_Export')
        writer_save_case.save()

        savedFileName = str(case2 + '-In&Output_Saved.xlsx')

        if os.path.exists(path_save_delete + savedFileName):
            #-- delete a case's data from database
            # INPUT 
            #- supply
            cursor.execute("DELETE FROM tbl_NEMOI_Supply_Cost WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Supply_Capacity WHERE case_id = %s ",case)

            #- demand
            cursor.execute("DELETE FROM tbl_NEMOI_Demand WHERE case_id = %s ",case)

            #- pipeline
            cursor.execute("DELETE FROM tbl_NEMOI_Arc_Tariffs WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Arc_Constraints WHERE case_id = %s ",case)

            #- storage
            cursor.execute("DELETE FROM tbl_NEMOI_Storage_Constraints WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Storage_Injection WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Storage_Extraction WHERE case_id = %s ",case)

            #- export
            cursor.execute("DELETE FROM tbl_NEMOI_Export_Capacity WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOI_Export_Price WHERE case_id = %s ",case)

            # OUTPUT
            cursor.execute("DELETE FROM tbl_NEMOO_Arc_Flows WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOO_Demand WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOO_Supply WHERE case_id = %s ",case)

            cursor.execute("DELETE FROM tbl_NEMOO_Storage_Inventory WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOO_Storage_Injection WHERE case_id = %s ",case)
            cursor.execute("DELETE FROM tbl_NEMOO_Storage_Extraction WHERE case_id = %s ",case)

            cursor.execute("DELETE FROM tbl_NEMOO_Export WHERE case_id = %s ",case)

            # case information table
            cursor.execute("DELETE FROM case_info WHERE case_id = %s ",case)

            conn.commit()


        flash("Delete "+ case+" from database and save into a excel file")
        flash('Please check://hou-file1/woodmac$/Latin America Markets/MeMo_Saved_Inputs_Outputs_for_Deleted_Cases/ for information')
        return redirect (url_for('run_nemo'))

        
    return render_template('del_case.html', form=form)

@app.route('/update_dev',methods=['GET', 'POST'])
def update_dev():
    #form = runmodel(form_name='updateDEV')

    # create cursor
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    cursor = conn.cursor()

    # get all data from a table
    results = cursor.execute("SELECT * FROM case_info ")

    alldata = cursor.fetchall()

    # create a list for case_id
    case_id_list=list()

    for i in range(len(alldata)):
        element = alldata[i][-3]
        if element is not None:
            case_id_list.append(alldata[i][-3])
    unique_case=sorted(list(set(case_id_list)))
    unique_case.remove('DEV')

    data=[]
    for i in range(len(unique_case)):
        data.append({'name':unique_case[i]})

    #print(data)

    if request.method == 'GET':
        # get Form Fields
        case = request.args.get('case')
        topology = request.args.get('topology')
        supply = request.args.get('supply')
        demand = request.args.get('demand')
        arc = request.args.get('arc')
        
        # update DEV with input from other case
        
        if case is not None:
            case = str(case)
            topology = str(topology)
            print(topology)
            

            if supply is not None:
                #- supply capacity
                sql = """SELECT * FROM tbl_NEMOI_Supply_Capacity WHERE case_id='DEV' and topology=%s"""
                supplycap = DB_table_data(conn, sql, topology)
                #- supply cost
                sql = """SELECT * FROM tbl_NEMOI_Supply_Cost WHERE case_id='DEV' and topology=%s"""
                supplycost = DB_table_data(conn, sql, topology)

                save_DEV_Sup_Inputs = pd.ExcelWriter(path_save_delete + 'NeMo_Last_DEV(BaseCase)_SupplyInputs.xlsx', engine='xlsxwriter') 
                supplycap.to_excel(save_DEV_Sup_Inputs,sheet_name = 'Input_Supply_Capacity')
                supplycost.to_excel(save_DEV_Sup_Inputs,sheet_name = 'Input_Supply_Cost')
                save_DEV_Sup_Inputs.save()

                if os.path.exists(path_save_delete + 'NeMo_Last_DEV(BaseCase)_SupplyInputs.xlsx'):

                    cursor.execute("DELETE FROM tbl_NEMOI_Supply_Cost WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()
                    cursor.execute("DELETE FROM tbl_NEMOI_Supply_Capacity WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()
                    cursor.execute("INSERT INTO tbl_NEMOI_Supply_Capacity (Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, capacity, date, case_id, topology) SELECT Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, capacity, date,'DEV', topology FROM tbl_NEMOI_Supply_Capacity WHERE case_id=%s  and topology=%s",(case,topology))
                    conn.commit()
                    cursor.execute("INSERT INTO tbl_NEMOI_Supply_Cost (Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, cost, date, case_id, topology) SELECT Unique_Hub_ID, Unique_SupplyNode_ID, node, hub, cost, date,'DEV', topology FROM tbl_NEMOI_Supply_Cost WHERE case_id=%s  and topology=%s",(case,topology))
                    conn.commit()
                    flash('DEV: Supply input data are updated and old data are saved','warning')



            if demand is not None:
                sql = """SELECT * FROM tbl_NEMOI_Demand WHERE case_id='DEV' and topology=%s"""
                dmd = DB_table_data(conn, sql, topology)

                save_DEV_Demand_Inputs = pd.ExcelWriter(path_save_delete + 'NeMo_Last_DEV(BaseCase)_DemandInputs.xlsx', engine='xlsxwriter')

                dmd.to_excel(save_DEV_Demand_Inputs,sheet_name = 'Input_Demand')
                save_DEV_Demand_Inputs.save()

                if os.path.exists(path_save_delete + 'NeMo_Last_DEV(BaseCase)_DemandInputs.xlsx'):
                    
                    cursor.execute("DELETE FROM tbl_NEMOI_Demand WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()
                    cursor.execute("INSERT INTO tbl_NEMOI_Demand (Unique_Node_ID,Unique_Hub_ID,node,hub,demand,date,case_id, topology,state) SELECT Unique_Node_ID,Unique_Hub_ID,node,hub,demand,date,'DEV', topology,state FROM tbl_NEMOI_Demand WHERE case_id=%s  and topology=%s",(case,topology))
                    conn.commit()
                    flash('DEV: Demand input data are updated and old data are saved','warning')



            if arc is not None:
                #- arc capacity
                sql = """SELECT * FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id='DEV' and topology=%s"""
                arc_cap_raw = DB_table_data(conn, sql, topology)
                #-------------------------------------------------------------------------------------
                #- get time-series pipeline capacity based on data in 'tbl_NEMOI_Arc_Pipeline_Infrastructure' table
                #- only get several columns for pipeline capacity df
                arc_cap = arc_cap_raw[['from_hub','to_hub',
                                      'online_date','ramp_up_months','capacity']]
                #- dates from 01/01/2000 to 12/1/2040
                dates = pd.DataFrame({'date': pd.date_range(start='1/1/2000', end='12/1/2040', freq='MS')})

                dates['join_key'] = 1
                #- get information for unique pipeline

                pipeline = arc_cap_raw.groupby(['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub','arc_name',
                                                 'to_hub','topology','case_id']).size().reset_index().rename(columns={0:'count'})
                pipeline.drop('count', axis=1, inplace=True)
                pipeline['join_key'] = 1
                #- merge pipeline and dates to expand full dates for every pipeline 
                pipelines = pd.merge(pipeline, dates, on='join_key', how='inner')
                pipelines.drop('join_key', axis=1, inplace=True)
                #- merge full dates pipeline information df with capacity df
                #- forward fill the capacity value of pipeline time series df using the nearest not null capacity value 
                arccap_ts = pd.merge(arc_cap,pipelines,left_on=['from_hub','to_hub','online_date'],right_on=['from_hub','to_hub','date'],how='right')


                # Update 2018/09/25 --WF
                # 1) find unique tuples for from_hub & to_hub
                arc_tuple = arc_cap[['from_hub','to_hub']].drop_duplicates()

                df = pd.DataFrame()
                # 2) loop through every element in tuples to find subset of df for every arc, sort by date, 
                #    let the first capacity value (if nan) to equal 0 and fill the NAs with its precedent value

                for row in arc_tuple.itertuples():
                    fromhub = row[1]
                    tohub = row[2]
                    arccap_t = arccap_ts[(arccap_ts['from_hub'] ==fromhub) & (arccap_ts['to_hub'] ==tohub )]
                    arccap_t1 = arccap_t.set_index(['date']).sort_index()
                    if pd.isnull(arccap_t1[['capacity']].iloc[0])[0] == True:
                        cap_col_index = arccap_t1.columns.get_loc('capacity')
                        arccap_t1.iloc[[0], [cap_col_index]] = 0
                    else:
                        pass

                    arccap_t2 = arccap_t1.fillna(method='ffill')
                    df = df.append(arccap_t2)

                df = df.reset_index()
                arccap_ts2 = df[['Unique_From_Hub_ID','Unique_To_Hub_ID', 'from_hub', 'to_hub', 'arc_name',
                           'date','capacity','topology', 'case_id']]   
                arccap_ts2['date'] = pd.to_datetime(arccap_ts2['date'],errors='coerce').dt.date 

                #- arc cost
                sql = """SELECT * FROM tbl_NEMOI_Arc_Tariffs WHERE case_id='DEV' and topology=%s"""
                arccost = DB_table_data(conn, sql, topology)

                #- arc min flow
                sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id='DEV' and data_type='Min Flow' and topology=%s"""
                arcmin_raw = DB_table_data(conn, sql, topology)

                m = {
                    'Jan': 1,
                    'Feb': 2,
                    'Mar': 3,
                    'Apr':4,
                    'May':5,
                    'Jun':6,
                    'Jul':7,
                    'Aug':8,
                    'Sept':9,
                    'Oct':10,
                    'Nov':11,
                    'Dec':12
                    }
                arcmin=[]
                if len(arcmin_raw)>0:
                    arcmin = pd.melt(arcmin_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                                   'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                                    var_name = 'month',value_name = 'min_flow')

                    arcmin.month = arcmin.month.map(m)
                    arcmin['day'] = 1
                    arcmin['date'] = pd.to_datetime(arcmin[['year','month','day']])
                    arcmin['date'] = pd.to_datetime(arcmin['date'],errors='coerce',format = '%Y-%m-%d').dt.date 
                
                #- arc max flow
                sql = """SELECT * FROM tbl_NEMOI_Arc_Constraints WHERE case_id='DEV' and data_type='Max Flow' and topology=%s"""
                arcmax_raw = DB_table_data(conn, sql, topology)
                if len(arcmax_raw)>0:
                    arcmax = pd.melt(arcmax_raw,id_vars=['Unique_From_Hub_ID','Unique_To_Hub_ID',
                                    'from_hub','to_hub','arc_name','data_type','comments','case_id','topology','year'],
                                     var_name = 'month',value_name = 'max_flow')
                    arcmax.month = arcmax.month.map(m)
                    arcmax['day'] = 1
                    arcmax['date'] = pd.to_datetime(arcmax[['year','month','day']])
                    arcmax['date']=pd.to_datetime(arcmax['date'],errors='coerce',format = '%Y-%m-%d').dt.date  
                else:
                    arcmax = []

                save_DEV_Arc_Inputs = pd.ExcelWriter(path_save_delete + 'NeMo_Last_DEV(BaseCase)_PipelineInputs.xlsx', engine='xlsxwriter')

                arc_cap.to_excel(save_DEV_Arc_Inputs,sheet_name = 'Input_Arc_Capacity')
                arccap_ts2.to_excel(save_DEV_Arc_Inputs,sheet_name = 'Input_Arc_Capacity_Vert')
                arccost.to_excel(save_DEV_Arc_Inputs,sheet_name = 'Input_Arc_Cost')
                if len(arcmin)>0:
                    arcmin.to_excel(save_DEV_Arc_Inputs,sheet_name = 'Input_Arc_Minflow')
                if len(arcmax)>0:
                    arcmax.to_excel(save_DEV_Arc_Inputs,sheet_name = 'Input_Arc_Maxflow')

                save_DEV_Arc_Inputs.save()

                if os.path.exists(path_save_delete + 'NeMo_Last_DEV(BaseCase)_PipelineInputs.xlsx'):
                    
                    cursor.execute("DELETE FROM tbl_NEMOI_Arc_Tariffs WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()
                    cursor.execute("DELETE FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()
                    cursor.execute("DELETE FROM tbl_NEMOI_Arc_Constraints WHERE case_id = 'DEV' and topology=%s ",topology)
                    conn.commit()

                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Pipeline_Infrastructure (Unique_From_Hub_ID,Unique_To_Hub_ID, from_hub, to_hub, arc_name, online_date, ramp_up_months, capacity, comments, topology,case_id ) SELECT Unique_From_Hub_ID,Unique_To_Hub_ID, from_hub, to_hub, arc_name, online_date, ramp_up_months, capacity, comments, topology,'DEV' FROM tbl_NEMOI_Arc_Pipeline_Infrastructure WHERE case_id=%s and topology=%s ",(case,topology))
                    conn.commit()
                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Tariffs (Unique_From_Hub_ID, Unique_To_Hub_ID, from_hub, to_hub, cost_pesoGJ, date, topology, case_id, arc_name) SELECT Unique_From_Hub_ID, Unique_To_Hub_ID, from_hub, to_hub, cost_pesoGJ, date, topology, 'DEV', arc_name FROM tbl_NEMOI_Arc_Tariffs WHERE case_id=%s  and topology=%s",(case,topology))
                    conn.commit()
                    cursor.execute("INSERT INTO tbl_NEMOI_Arc_Constraints (Unique_From_Hub_ID,Unique_To_Hub_ID,from_hub,to_hub,arc_name,data_type,comments,case_id,topology,year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,Dec ) SELECT Unique_From_Hub_ID,Unique_To_Hub_ID,from_hub,to_hub,arc_name,data_type,comments,'DEV',topology,year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,Dec FROM tbl_NEMOI_Arc_Constraints WHERE case_id=%s and topology=%s ",(case,topology))
                    conn.commit()

                flash('DEV: Pipeline input data are updated and old data are saved','warning')

            
    #return render_template('update_dev1.html',update_form=form, data=data)
    return render_template('update_dev.html',  data=data)


if __name__ == '__main__':
    app.secret_key='secret123'
    
    application.run(debug=True,port=8001, host = ip)
