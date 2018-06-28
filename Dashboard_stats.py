import re
import sys
import pyodbc
import datetime
import numpy as np
from datetime import datetime,timedelta,time


#<MISC FUNCTIONS>
def get_cursor(server,database):
    cnxn=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database,trusted_connection='yes')
    cursor=cnxn.cursor()
    return cursor

def get_RequestTypes(cursor):
    cursor.execute("select RequestType from RequestTypeMetaData")
    return [str(i[0]) for i in list(cursor)]

#</MISC FUNCTIONS>

#<DATETIME FUNCTIONS>
def get_max_time(cursor):
    cursor.execute("select max(CreationTime) from RequestStore")
    return cursor.fetchone()[0]

def get_last_sunday(today):
    curr=today
    while(curr.weekday()!=6):
            curr=curr-timedelta(days=1)
    curr=datetime.combine(curr,time())
    return curr

def get_monthly_range(cursor):
    max_time=get_max_time(cursor)
    last_sunday=get_last_sunday(max_time)
    monday_start=last_sunday-timedelta(weeks=3)-timedelta(days=6)
    monday_start=monday_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sunday_end=(last_sunday+timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return monday_start,sunday_end

def get_weekly_range(cursor,week):
    max_time=get_max_time(cursor)
    last_sunday=get_last_sunday(max_time)
    monday_start=last_sunday-timedelta(weeks=(week-1))-timedelta(days=6)
    sunday_end=monday_start+timedelta(days=7)
    monday_start=monday_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sunday_end=sunday_end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return monday_start,sunday_end

#</DATETIME FUNCTIONS>

#<VLAD ERROR FUNCTIONS>
def get_vlad_error_counts(lines):
    vlad_error_counts={}
    for line in lines:
        vlad_names=extract_vlad_names(line)
        for vlad_name in vlad_names:
            if(vlad_name in vlad_error_counts):
                vlad_error_counts[vlad_name]+=1
            else:
                vlad_error_counts[vlad_name]=1

    vlad_error_counts=[(v,k) for k,v in vlad_error_counts.items()]
    vlad_error_counts.sort()
    vlad_error_counts.reverse()
    vlad_error_counts=[(k,v) for v,k in vlad_error_counts]
    return vlad_error_counts
    
def extract_vlad_names(line):
    vlad_names=[]
    temp=re.split("Test name: ",line)
    for i in temp:
        temp=re.split(" ",i)[0]
        temp=re.split("_",temp)[0]
        vlad_names.append(temp)
    vlad_names=vlad_names[1:]
    return vlad_names

def get_total_vlad_failures(vlad_error_counts):
    sum=0
    for i in vlad_error_counts:
        sum+=i[1]
    return sum

def get_number_of_vlad_failures(cursor,days,RequestType):
    if(not days):
        days=60
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Failed' and StatusDescription like '%Test name: %'")
        return cursor.fetchone()[0]
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Failed' and StatusDescription like '%Test name: %'")
        return cursor.fetchone()[0]

def get_vlad_failure_percentage(cursor,days,RequestType):
    total_failures=get_number_of_failures(cursor,days,RequestType)
    vlad_failures=get_number_of_vlad_failures(cursor,days,RequestType)
    if(total_failures!=0):
        return vlad_failures*100/total_failures
    else:
        return 0.00

#</VLAD FUNCTIONS>

#<NON-VLAD FUNCTIONS>
def get_number_of_non_vlad_failures(cursor,days,RequestType):
    if(not days):
        days=60
    total_failures=get_number_of_failures(cursor,days,RequestType)
    vlad_failures=get_number_of_vlad_failures(cursor,days,RequestType)
    return (total_failures-vlad_failures)

def get_non_vlad_failures_monthly(cursor,RequestType):
    monday_start,sunday_end=get_monthly_range(cursor)
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestStatus='Failed' and RequestType="+"'"+RequestType+"' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' and StatusDescription not like '%Test name%'")
    else:
        cursor.execute("select count(*) from RequestStore where RequestStatus='Failed' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' and StatusDescription not like '%Test name%'")
    return cursor.fetchone()[0]

def get_non_vlad_error_descriptions_monthly(cursor,RequestType):
    monday_start,sunday_end=get_monthly_range(cursor)
    if(RequestType!='All'):
        cursor.execute("select StatusDescription from RequestStore where RequestType="+"'"+RequestType+"' and RequestStatus='Failed' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' and StatusDescription not like '%Test name%' and StatusDescription not like '%Failed all retries%' ")
    else:
        cursor.execute("select StatusDescription from RequestStore where CreationTime>'"+monday_start+"' and RequestStatus='Failed' and CreationTime<'"+sunday_end+"' and StatusDescription not like '%Test name%' and StatusDescription not like '%Failed all retries%'")
    return [str(i) for i in list(cursor)]

def get_non_vlad_failure_percentage(cursor,days,RequestType):
    vlad_failure_percentage=get_vlad_failure_percentage(cursor,days.RequestType)
    return 100.00-vlad_failure_percentage

def get_non_vlad_error_counts_monthly(cursor,RequestType):
    error_descriptions=get_non_vlad_error_descriptions_monthly(cursor,RequestType)
    non_vlad_error_counts={}
    descriptive_error={}
    for error in error_descriptions:
        if(len(re.split("path does not exist",error))>=2):
            if("Environment path does not exist" in non_vlad_error_counts):
                non_vlad_error_counts["Environment path does not exist"]+=1
            else:
                non_vlad_error_counts["Environment path does not exist"]=1
                descriptive_error["Environment path does not exist"]=error
        elif(len(re.split("already exists",error))>=2 and len(re.split("Add-Edge",error))<2):
            if("already exists" in non_vlad_error_counts):
                non_vlad_error_counts["already exists"]+=1
            else:
                non_vlad_error_counts["already exists"]=1
                descriptive_error["already exists"]=error
        elif(len(re.split("don't exist",error))>=2):
            if("don't exist" in non_vlad_error_counts):
                non_vlad_error_counts["don't exist"]+=1
            else:
                non_vlad_error_counts["don't exist"]=1
                descriptive_error["don't exist"]=error
        elif(len(re.split("required group membership",error))>=2):
            if("don't exist" in non_vlad_error_counts):
                non_vlad_error_counts["required group membership"]+=1
            else:
                non_vlad_error_counts["required group membership"]=1
                descriptive_error["required group membership"]=error
        elif(len(re.split("MachineSelectorTool failure",error))>=2):
            if("MachineSelectorTool failure" in non_vlad_error_counts):
                non_vlad_error_counts["MachineSelectorTool failure"]+=1
            else:
                non_vlad_error_counts["MachineSelectorTool failure"]=1
                descriptive_error["MachineSelectorTool failure"]=error
        elif(len(re.split("does not exist",error))>=2 and len(re.split("machine",error))>=2):
            if("Machine to be deleted does not exist" in non_vlad_error_counts):
                non_vlad_error_counts["Machine to be deleted does not exist"]+=1
            else:
                non_vlad_error_counts["Machine to be deleted does not exist"]=1
                descriptive_error["Machine to be deleted does not exist"]=error
        elif(len(re.split("Could not read CSV",error))>=2):
            if("Could not read CSV" in non_vlad_error_counts):
                non_vlad_error_counts["Could not read CSV"]+=1
            else:
                non_vlad_error_counts["Could not read CSV"]=1
                descriptive_error["Could not read CSV"]=error
        elif(len(re.split("Add-Edge failed",error))>=2):
            if("Add-Edge failed" in non_vlad_error_counts):
                non_vlad_error_counts["Add-Edge failed"]+=1
            else:
                non_vlad_error_counts["Add-Edge failed"]=1
                descriptive_error["Add-Edge failed"]=error
        elif(len(re.split("ClusterToolFailure",error))>=2):
            if("ClusterToolFailure" in non_vlad_error_counts):
                non_vlad_error_counts["ClusterToolFailure"]+=1
            else:
                non_vlad_error_counts["ClusterToolFailure"]=1
                descriptive_error["ClusterToolFailure"]=error
        elif(len(re.split("Requested move capacity",error))>=2):
            if("ClusterToolFailure" in non_vlad_error_counts):
                non_vlad_error_counts["Requested move capacity"]+=1
            else:
                non_vlad_error_counts["Requested move capacity"]=1
                descriptive_error["Requested move capacity"]=error
        else:
            if("Misc. Error" in non_vlad_error_counts):
                non_vlad_error_counts["Misc. error"]+=1
            else:
                non_vlad_error_counts["Misc. error"]=1
                descriptive_error["Misc. error"]=error
                
    non_vlad_error_counts=[(v,k) for k,v in non_vlad_error_counts.items()]
    non_vlad_error_counts.sort()
    non_vlad_error_counts.reverse()
    non_vlad_error_counts=[(k,v) for v,k in non_vlad_error_counts]
    descriptive_error=[(v,k) for k,v in descriptive_error.items()]
    descriptive_error.sort()
    descriptive_error.reverse()
    descriptive_error=[(k,v) for v,k in descriptive_error]
    return non_vlad_error_counts , descriptive_error
        

#</NON-VLAD FUNCTIONS>

#<ERROR DESCRIPTIONS FUNCTIONS>
def get_error_descriptions(cursor,days,RequestType):
    if(not days):
        days=60
    if(RequestType!='All'):
        cursor.execute("select StatusDescription from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>DATEADD(day,-"+str(days)+",GETDATE())")
    else:
        cursor.execute("select StatusDescription from RequestStore where CreationTime>DATEADD(day,-"+str(days)+",GETDATE())")
    return [str(i) for i in list(cursor)]
    
def get_error_descriptions_last_month(cursor,RequestType):
    monday_start,sunday_end=get_monthly_range(cursor)
    if(RequestType!='All'):
        cursor.execute("select StatusDescription from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"'")
    else:
        cursor.execute("select StatusDescription from RequestStore where CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"'")
    return [str(i) for i in list(cursor)]

#</ERROR DESCRIPTIONS FUNCTIONS>

#<TOTAL REQUESTS FUNCTIONS>
def get_number_of_failures(cursor,days,RequestType):
    if(not days):
        days=60
    failed_requests=int()
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Failed' ")
        failed_requests=cursor.fetchone()[0]
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Failed' ")
        failed_requests=cursor.fetchone()[0]
    return failed_requests
    
def get_failure_percentage(cursor,days,RequestType):
    if(not days):
        days=60
    total_requests=get_total_requests(cursor,days,RequestType)
    failed_requests=get_number_of_failures(cursor,days,RequestType)
    if(total_requests!=0):
        return failed_requests*100/total_requests
    else:
        return 0.00

def get_number_of_successes(cursor,days,RequestType):
    if(not days):
        days=60
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Succeeded' ")
        successful_requests=cursor.fetchone()[0]
        return successful_requests
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) and RequestStatus='Succeeded' ")
        successful_requests=cursor.fetchone()[0]
        return successful_requests
    
def get_number_of_successes_weekly(cursor,week,RequestType):
    monday_start,sunday_end=get_weekly_range(cursor,week)
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' and RequestStatus='Succeeded' ")
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' and RequestStatus='Succeeded' ")
    successful_requests=cursor.fetchone()[0]
    return successful_requests

def get_success_percentage_weekly(cursor,week,RequestType):
    total_requests=get_total_requests_weekly(cursor,week,RequestType)
    successful_requests=get_number_of_successes_weekly(cursor,week,RequestType)
    if(total_requests!=0):
        return successful_requests*100/total_requests
    else:
        return 0.00

def get_success_percentage(cursor,days,RequestType):
    if(not days):
        days=60
    total_requests=get_total_requests(cursor,days,RequestType)
    successful_requests=get_number_of_successes(cursor,days,RequestType)
    if(total_requests!=0):
        return successful_requests*100/total_requests
    else:
        return 0.00

def get_total_requests(cursor,days,RequestType):
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>DATEADD(day,-"+str(days)+",GETDATE())")
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>DATEADD(day,-"+str(days)+",GETDATE()")
    total_requests=cursor.fetchone()[0]
    return total_requests

def get_total_requests_weekly(cursor,week,RequestType):
    monday_start,sunday_end=get_weekly_range(cursor,week)
    if(RequestType!='All'):
        cursor.execute("select count(*) from RequestStore where RequestType="+"'"+RequestType+"' and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"'")
    else:
        cursor.execute("select count(*) from RequestStore where CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"'")
    total_requests=cursor.fetchone()[0]
    return total_requests

#</TOTAL REQUESTS FUNCTIONS>

#<AVERAGE LATENCY FUNCTIONS>
def get_avg_processing_time_cumulative(cursor,days,RequestType):
    successful_processing_times=np.array([])
    failed_processing_times=np.array([])
    if(not days):
        days=60
    if(RequestType!='All'):
        
        cursor.execute("select RequestStatusHistory.id,RequestStatusHistory.RequestStatus,RequestStatusHistory.Modtime,RequestType from RequestStatusHistory inner join RequestStore on RequestStatusHistory.id=RequestStore.id where RequestType="+"'"+RequestType+"' " + "and CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) order by RequestStatusHistory.id,RequestStatusHistory.ModTime")
    else:
        cursor.execute("select RequestStatusHistory.id,RequestStatusHistory.RequestStatus,RequestStatusHistory.Modtime,RequestType from RequestStatusHistory inner join RequestStore on RequestStatusHistory.id=RequestStore.id where CreationTime>DATEADD(day,-"+str(days)+",GETDATE()) order by RequestStatusHistory.id,RequestStatusHistory.ModTime")
    temp_diff=0
    row=cursor.fetchone()

    while row:
        if(row[1]=='Processing'):
            temp=cursor.fetchone()
            datetime_diff=temp[2]-row[2]
            if(temp[0]==row[0]):
                temp_diff+=datetime_diff/timedelta(minutes=1)
            else:
                temp_diff=0
            row=temp

        elif(row[1]=='Succeeded'):
            successful_processing_times=np.append(temp_diff,successful_processing_times)
            temp_diff=0
            row=cursor.fetchone()

        elif(row[1]=='Failed'):
            failed_processing_times=np.append(temp_diff,failed_processing_times)
            temp_diff=0
            row=cursor.fetchone()
            
        elif(row[1]=='Cancelled'):
            temp_diff=0;
            row=cursor.fetchone()

        else:
            row=cursor.fetchone()

    return_value={}
    if(successful_processing_times.size!=0):
        return_value['Success_empty']=0
        return_value['Success_median']=np.median(successful_processing_times)
        return_value['Success_average']=np.average(successful_processing_times)
        return_value['Success_90']=np.percentile(successful_processing_times,90)
    else:
        return_value['Success_empty']=1
    if(failed_processing_times.size!=0):
        return_value['Failed_empty']=0
        return_value['Failed_median']=np.median(failed_processing_times)
        return_value['Failed_average']=np.average(failed_processing_times)
        return_value['Failed_90']=np.percentile(failed_processing_times,90)
    else:
        return_value['Failed_empty']=1
    return return_value


def get_avg_processing_time_weekly(cursor,week,RequestType):
    monday_start,sunday_end=get_weekly_range(cursor,week)
    successful_processing_times=np.array([])
    failed_processing_times=np.array([])
    if(RequestType!='All'):
            cursor.execute("select RequestStatusHistory.id,RequestStatusHistory.RequestStatus,RequestStatusHistory.Modtime,RequestType from RequestStatusHistory inner join RequestStore on RequestStatusHistory.id=RequestStore.id where RequestType="+"'"+RequestType+"' " + "and CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' order by RequestStatusHistory.id,RequestStatusHistory.ModTime")
    else:
            cursor.execute("select RequestStatusHistory.id,RequestStatusHistory.RequestStatus,RequestStatusHistory.Modtime,RequestType from RequestStatusHistory inner join RequestStore on RequestStatusHistory.id=RequestStore.id where CreationTime>'"+monday_start+"' and CreationTime<'"+sunday_end+"' order by RequestStatusHistory.id,RequestStatusHistory.ModTime")
    temp_diff=0
    row=cursor.fetchone()
    while row:
        if(row[1]=='Processing'):
            temp=cursor.fetchone()
            datetime_diff=temp[2]-row[2]
            if(temp[0]==row[0]):
                temp_diff+=datetime_diff/timedelta(minutes=1)
            else:
                temp_diff=0
            row=temp

        elif(row[1]=='Succeeded'):
            successful_processing_times=np.append(temp_diff,successful_processing_times)
            temp_diff=0
            row=cursor.fetchone()

        elif(row[1]=='Failed'):
            failed_processing_times=np.append(temp_diff,failed_processing_times)
            temp_diff=0
            row=cursor.fetchone()
            
        elif(row[1]=='Cancelled'):
            temp_diff=0;
            row=cursor.fetchone()

        else:
            row=cursor.fetchone()
    return_value={}
    if(successful_processing_times.size!=0):
        return_value['Success_empty']=0
        return_value['Success_median']=np.median(successful_processing_times)
        return_value['Success_average']=np.average(successful_processing_times)
        return_value['Success_90']=np.percentile(successful_processing_times,90)
    else:
        return_value['Success_empty']=1
    if(failed_processing_times.size!=0):
        return_value['Failed_empty']=0
        return_value['Failed_median']=np.median(failed_processing_times)
        return_value['Failed_average']=np.average(failed_processing_times)
        return_value['Failed_90']=np.percentile(failed_processing_times,90)
    else:
        return_value['Failed_empty']=1
    return return_value

#</AVERAGE LATENCY FUNCTIONS>
