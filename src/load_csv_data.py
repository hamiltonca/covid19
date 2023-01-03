#!/usr/bin/env python
import boto3
import logging
import sys
import re
import json
import csv
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from datetime import date
"""
A simple class to capture County covid-19 cases by county from
the Georgia Department of Healt's covid-19 status page.
Once parsed, the data is stored in the countyvaluemap for further use.
"""

def parsefile(datetime, fileName):
	session = boto3.Session()
	client = session.client('s3')
	dynamodb = session.client('dynamodb')
	s3_obj = client.get_object(Bucket='useast1-fetched-data', Key=fileName)
	filedata = s3_obj['Body'].read().decode()
	csvrdr = csv.reader(filedata.split('\n'),dialect='excel')
	dbData = {}
	for row in csvrdr:
		rowlength = len(row)
		p = re.compile("\\d+")
		if (len(row) == 15 and p.match(row[1])):
			(county, cases, county_id, StateFIPSCode, CountyFIPSCode, pop, hosp, deaths, caseRate, deathRate, caseRate14Day, cases14Day, antigenCases, probableDeaths, antigen_case_hospitalization) = row
			logger.info("County: {0}, cases: {1}, case_rate: {2}, deaths: {3}, death_rate: {4}".format(county,cases, caseRate, deaths, deathRate))
			dbData['county'] = {'S':county + ",GA" }
			dbData['reported-datetime'] = { 'S':datetime }
			dbData['numCasesReported'] = { 'N':cases }
			dbData['numDeathsReported'] = { 'N':deaths }
			dynamodb.put_item(TableName="covid-data-2", Item=dbData)

	s3_obj['Body'].close()

def getdateandfile():
	now = datetime.now(tz=timezone(timedelta(hours=-4)))
	year = now.year
	month = now.month
	day = now.day

	if (now.hour < 12):
		hour = 19
		day -= 1
	elif (now.hour >= 12 and now.hour < 19):
		hour= 12
	else:
		hour = 19

	reportdate = "{:04d}-{:02d}-{:02d}T{:02}:00:00".format(year,month,day,hour)
	file = "countycases-{:04d}{:02d}{:02d}-{:02d}00.csv".format(year,month,day,hour)

	return (reportdate,file)

def event_handler(event, context):
	(reportdate,filename) = getdateandfile()
	parsefile(reportdate,filename)
	return "success"
	
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
logger = logging.getLogger(name="__main__")

if __name__ == '__main__':
	session = boto3.Session()
	s3 = session.client('s3')
	resp = s3.list_objects(Bucket='useast1-fetched-data', Prefix='countycases-2022', MaxKeys=4096)
	csvfile = {}
	for csvfile in resp['Contents']:
		key = csvfile['Key']
		logger.info("key: {0}".format(key))
		# countycases-20210320-1900.cs
		year = key[12:16]
		month = key[16:18]
		day = key[18:20]
		hour = key[21:23]
		dayOfWeek = date(int(year),int(month),int(day)).weekday()
		logger.info("dayOfWeek: {0}, hour: {1}".format(dayOfWeek, hour))
		if ( dayOfWeek == 2 and hour == '19' and year == '2022' and month >= '10'):
			# (year,month,day,hour) = (m.group(1),m.group(2),m.group(3),m.group(4))
			reportdate = "{:s}-{:s}-{:s}T{:s}:00:00".format(year,month,day,hour)
			logger.info("file: {0}, LastModified: {1}, reportDate: {2}".format(csvfile['Key'], csvfile['LastModified'], reportdate))
			parsefile(reportdate, key)
		else:
			logger.info("skipping file: {0}".format(key))

