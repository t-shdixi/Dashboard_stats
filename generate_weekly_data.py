import re
import sys
import xlwt
import pyodbc
import datetime
import numpy as np
import Dashboard_stats as ds
import matplotlib.pyplot as plt
from datetime import datetime,timedelta,time

server='shivang-intern\sqlexpress'
database='Ironman'
cursor=ds.get_cursor(server,database)
RequestTypes=ds.get_RequestTypes(cursor)
RequestTypes=['All']+RequestTypes
book=xlwt.Workbook()
overview={}

def get_row_mappings_last_month():
    row_mappings={}
    row_mappings['Success rate last week']=1
    row_mappings['Success rate 2nd last week']=2
    row_mappings['Success rate 3rd last week']=3
    row_mappings['Success rate 4th last week']=4
    row_mappings['Avg. success latency last week']=5
    row_mappings['Avg. success latency 2nd last week']=6
    row_mappings['Avg. success latency 3rd last week']=7
    row_mappings['Avg. success latency 4th last week']=8
    row_mappings['Avg. failed latency last week']=9
    row_mappings['Avg. failed latency 2nd last week']=10
    row_mappings['Avg. failed latency 3rd last week']=11
    row_mappings['Avg. failed latency 4th last week']=12
    row_mappings['Median success last week']=13
    row_mappings['Median success 2nd last week']=14
    row_mappings['Median success 3rd last week']=15
    row_mappings['Median success 4th last week']=16
    row_mappings['Median failed last week']=17
    row_mappings['Median failed 2nd last week']=18
    row_mappings['Median failed 3rd last week']=19
    row_mappings['Median failed 4th last week']=20
    row_mappings['Total Requests last week']=21
    row_mappings['Total Requests 2nd last week']=22
    row_mappings['Total Requests 3rd last week']=23
    row_mappings['Total Requests 4th last week']=24
    return row_mappings

def drill_down_row_mappings():
    drill_down={}
    drill_down['VLAD failures last 4 weeks']=1
    drill_down['Top failing VLAD']=2
    drill_down['Top failing VLAD failures']=3
    drill_down['number of non-VLAD failures']=4
    drill_down['Top non-VLAD failure']=5
    return drill_down
    
def write_headers_drill_down(RequestType):
    global cursor
    global book
    sheet2=book.add_sheet(RequestType[:10] + " drill down")
    row_headers=list(drill_down_row_mappings().keys())
    for i in range(1,len(row_headers)+1):
        row=sheet2.row(i)
        row.write(0,row_headers[i-1])
    return sheet2

def write_headers_last_month():
    global cursor
    global book
    global RequestTypes
    sheet1=book.add_sheet("BY3P last month")
    row_headers=list(get_row_mappings_last_month().keys())
    for i in range(1,len(row_headers)+1):
        row=sheet1.row(i)
        row.write(0,row_headers[i-1])
    row=sheet1.row(0)
    for i in range(1,len(RequestTypes)+1):
        row.write(i,RequestTypes[i-1])
    return sheet1

def write_data_last_month():
    global cursor
    global RequestTypes
    global overview
    sheet1=write_headers_last_month()
    row_mappings=get_row_mappings_last_month()
    weeks=[1,2,3,4]
    for i in range(1,len(RequestTypes)+1):
        for week in weeks:
            write_value=ds.get_avg_processing_time_weekly(cursor,week,RequestTypes[i-1])
            success_rate=ds.get_success_percentage_weekly(cursor,week,RequestTypes[i-1])
            total_requests=ds.get_total_requests_weekly(cursor,week,RequestTypes[i-1])
            if(week==1):
                row=sheet1.row(row_mappings['Success rate last week'])
                row.write(i,success_rate)
                if(RequestTypes[i-1]=='All'):
                    overview['Success rate last week']=success_rate
                if(write_value['Success_empty']==0):
                    row=sheet1.row(row_mappings['Avg. success latency last week'])
                    row.write(i,write_value['Success_average'])
                    row=sheet1.row(row_mappings['Median success last week'])
                    row.write(i,write_value['Success_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. success latency last week']=write_value['Success_average']
                        overview['Median success last week']=write_value['Success_median']
                        overview['90th Success last week']=write_value['Success_90']
                if(write_value['Failed_empty']==0):
                    row=sheet1.row(row_mappings['Avg. failed latency last week'])
                    row.write(i,write_value['Failed_average'])
                    row=sheet1.row(row_mappings['Median failed last week'])
                    row.write(i,write_value['Failed_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. failed latency last week']=write_value['Failed_average']
                        overview['Median failed last week']=write_value['Failed_median']
                        overview['90th Failed last week']=write_value['Failed_90']
                row=sheet1.row(row_mappings['Total Requests last week'])
                row.write(i,total_requests)
            elif(week==2):
                row=sheet1.row(row_mappings['Success rate 2nd last week'])
                row.write(i,success_rate)
                if(RequestTypes[i-1]=='All'):
                    overview['Success rate 2nd last week']=success_rate
                if(write_value['Success_empty']==0):
                    row=sheet1.row(row_mappings['Avg. success latency 2nd last week'])
                    row.write(i,write_value['Success_average'])
                    row=sheet1.row(row_mappings['Median success 2nd last week'])
                    row.write(i,write_value['Success_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Median success 2nd last week']=write_value['Success_median']
                        overview['Avg. success latency 2nd last week']=write_value['Success_average']
                        overview['90th Success 2nd last week']=write_value['Success_90']
                if(write_value['Failed_empty']==0):
                    row=sheet1.row(row_mappings['Avg. failed latency 2nd last week'])
                    row.write(i,write_value['Failed_average'])
                    row=sheet1.row(row_mappings['Median failed 2nd last week'])
                    row.write(i,write_value['Failed_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. failed latency 2nd last week']=write_value['Failed_average']
                        overview['Median failed 2nd last week']=write_value['Failed_median']
                        overview['90th Failed 2nd last week']=write_value['Failed_90']
                row=sheet1.row(row_mappings['Total Requests 2nd last week'])
                row.write(i,total_requests)
            elif(week==3):
                row=sheet1.row(row_mappings['Success rate 3rd last week'])
                row.write(i,success_rate)
                if(RequestTypes[i-1]=='All'):
                    overview['Success rate 3rd last week']=success_rate
                if(write_value['Success_empty']==0):
                    row=sheet1.row(row_mappings['Avg. success latency 3rd last week'])
                    row.write(i,write_value['Success_average'])
                    row=sheet1.row(row_mappings['Median success 3rd last week'])
                    row.write(i,write_value['Success_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. success latency 3rd last week']=write_value['Success_average']
                        overview['Median success 3rd last week']=write_value['Success_median']
                        overview['90th Success 3rd last week']=write_value['Success_90']
                if(write_value['Failed_empty']==0):
                    row=sheet1.row(row_mappings['Avg. failed latency 3rd last week'])
                    row.write(i,write_value['Failed_average'])
                    row=sheet1.row(row_mappings['Median failed 3rd last week'])
                    row.write(i,write_value['Failed_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. failed latency 3rd last week']=write_value['Failed_average']
                        overview['Median failed 3rd last week']=write_value['Failed_median']
                        overview['90th Failed 3rd last week']=write_value['Failed_90']
                row=sheet1.row(row_mappings['Total Requests 3rd last week'])
                row.write(i,total_requests)
            else:
                row=sheet1.row(row_mappings['Success rate 4th last week'])
                row.write(i,success_rate)
                if(RequestTypes[i-1]=='All'):
                    overview['Success rate 4th last week']=success_rate
                if(write_value['Success_empty']==0):
                    row=sheet1.row(row_mappings['Avg. success latency 4th last week'])
                    row.write(i,write_value['Success_average'])
                    row=sheet1.row(row_mappings['Median success 4th last week'])
                    row.write(i,write_value['Success_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. success latency 4th last week']=write_value['Success_average']
                        overview['Median success 4th last week']=write_value['Success_median']
                        overview['90th Success 4th last week']=write_value['Success_90']
                if(write_value['Failed_empty']==0):
                    row=sheet1.row(row_mappings['Avg. failed latency 4th last week'])
                    row.write(i,write_value['Failed_average'])
                    row=sheet1.row(row_mappings['Median failed 4th last week'])
                    row.write(i,write_value['Failed_median'])
                    if(RequestTypes[i-1]=='All'):
                        overview['Avg. failed latency 4th last week']=write_value['Failed_average']
                        overview['Median failed 4th last week']=write_value['Failed_median']
                        overview['90th Failed 4th last week']=write_value['Failed_90']
                row=sheet1.row(row_mappings['Total Requests 4th last week'])
                row.write(i,total_requests)

def write_data_to_excel_drill_down(RequestType):
    global cursor
    global overview
    sheet2=write_headers_drill_down(RequestType)
    success_rate=[]
    success_median=[]
    success_90=[]
    fail_median=[]
    fail_90=[]
    weeks=[1,2,3,4]
    for week in weeks:
        info=ds.get_avg_processing_time_weekly(cursor,week,RequestType)
        success_rate.append(ds.get_success_percentage_weekly(cursor,week,RequestType))
        if(info['Success_empty']==0):
            success_median.append(info['Success_median'])
            success_90.append(info['Success_90'])
        else:
            success_median.append(0)
            success_90.append(0)
        if(info['Failed_empty']==0):
            fail_median.append(info['Failed_median'])
            fail_90.append(info['Failed_90'])
        else:
            fail_median.append(0)
            fail_90.append(0)
    plt.plot(success_rate)
    plt.plot([overview['Success rate last week'],overview['Success rate 2nd last week'],overview['Success rate 3rd last week'],overview['Success rate 4th last week']])
    plt.legend([RequestType,'Overall'],loc='upper left')
    plt.title("Success rates")
    plt.figure()
    plt.title("Median latency values")
    plt.subplot(121)
    plt.title('Median Failed latency')
    plt.plot(fail_median)
    plt.plot([overview['Median failed last week'],overview['Median failed 2nd last week'],overview['Median failed 3rd last week'],overview['Median failed 4th last week']])
    plt.legend([RequestType,'Overall'],loc='upper left')
    plt.subplot(122)
    plt.title("Median Success latency")
    plt.plot(success_median)
    plt.plot([overview['Median success last week'],overview['Median success 2nd last week'],overview['Median success 3rd last week'],overview['Median success 4th last week']])
    plt.legend([RequestType,'Overall'],loc='upper left')
    plt.figure()
    plt.title("90th percentiles latency values")
    plt.subplot(121)
    plt.title("90th percentile Failed latency")
    plt.plot(fail_90)
    plt.plot([overview['90th Failed last week'],overview['90th Failed 2nd last week'],overview['90th Failed 3rd last week'],overview['90th Failed 4th last week']])
    plt.legend([RequestType,'Overall'],loc='upper left')
    plt.subplot(122)
    plt.title("90th percentile Success latency")
    plt.plot(success_90)
    plt.plot([overview['90th Success last week'],overview['90th Success 2nd last week'],overview['90th Success 3rd last week'],overview['90th Success 4th last week']])
    plt.legend([RequestType,'Overall'],loc='upper left')
    plt.show()
    error_descriptions=ds.get_error_descriptions_last_month(cursor,RequestType)
    vlad_error_counts=ds.get_vlad_error_counts(error_descriptions)
    row_mappings=drill_down_row_mappings()
    total_vlad_failures=ds.get_total_vlad_failures(vlad_error_counts)
    non_vlad_failures=ds.get_non_vlad_failures_monthly(cursor,RequestType)
    row=sheet2.row(row_mappings['VLAD failures last 4 weeks'])
    row.write(1,total_vlad_failures)
    row=sheet2.row(row_mappings['Top failing VLAD'])
    if(total_vlad_failures!=0):
        row.write(1,vlad_error_counts[0][0])
        row=sheet2.row(row_mappings['Top failing VLAD failures'])
        row.write(1,vlad_error_counts[0][1])
    else:
        row.write(1,0);
        row=sheet2.row(row_mappings['Top failing VLAD failures'])
        row.write(1,0)
    row=sheet2.row(row_mappings['number of non-VLAD failures'])
    row.write(1,non_vlad_failures)

def write_VLAD_data_to_excel():
    global RequestTypes
    RTS=RequestTypes[1:]
    global book
    global cursor
    sheet=book.add_sheet("Vlad_data")
    i=0
    for RequestType in RTS:
        error_descriptions=ds.get_error_descriptions_last_month(cursor,RequestType)
        vlad_error_counts=ds.get_vlad_error_counts(error_descriptions)
        row=sheet.row(i)
        row.write(0,RequestType)
        for vlad in vlad_error_counts:
            i+=1
            row=sheet.row(i)
            row.write(0,vlad[0])
            row.write(1,vlad[1])
        i+=2

def write_non_VLAD_data_to_excel():
    global RequestTypes
    RTS=RequestTypes[1:]
    global book
    global cursor
    sheet=book.add_sheet("non_VLAD_data")
    for RequestType in RTS:
        error_description=ds.get_non_vlad_error_counts(cursor,RequestType)
        
        
        
        
write_data_last_month()
write_data_to_excel_drill_down('MoveMachine2')
#write_VLAD_data_to_excel()
book.save("Testing_drill_down.xls")

