#!/usr/bin/env python
import boto3
import logging
import sys
import re
import json
from html.parser import HTMLParser
from datetime import datetime
from datetime import timezone
from datetime import timedelta
"""
A simple class to capture County covid-19 cases by county from
the Georgia Department of Healt's covid-19 status page.
Once parsed, the data is stored in the countyvaluemap for further use.
"""
class GaCovidPageParser(HTMLParser):
	state = str()
	county = str()
	countyvaluemap = {}
	def getcountyvaluemap(self):
		return self.countyvaluemap

class GaCovidV1PageParser(GaCovidPageParser):
	value = str()
	def handle_starttag(self, tag, attrs):
		if (tag == 'thead'):
			#print("start: {0}".format(tag))
			self.state = tag
		elif (tag == 'th' and self.state == 'thead'):
			self.state = 'capture th'
		elif (tag == 'td' and self.state == 'arm capture county name'):
			self.state = 'capture county name'
		elif (tag == 'td' and self.state == 'arm capture county value'):
			self.state = 'capture county value'

	def handle_data(self, data):
		if (self.state == 'capture th' and data == 'County'):
			#print("begin capture")
			self.state = "arm capture county name"
		elif (self.state == 'capture county name'):
			self.county = data
			self.state = 'arm capture county value'
		elif (self.state == 'capture county value'):
			self.value = data
			self.countyvaluemap[self.county] = self.value
			self.state = 'arm capture county name'


class GaCovidV2PageParser(GaCovidPageParser):
	value = {}
	def handle_starttag(self, tag, attrs):
		if (tag == 'table'):
			self.state = "table"
		elif (tag == 'td' and self.state == 'table'):
			self.state = 'check for county header'
		elif (tag == 'td' and self.state == 'arm capture county name'):
			self.state = 'capture county name'
		elif (tag == 'td' and self.state == 'arm capture county count'):
			self.state = 'capture county count'
		elif (tag == 'td' and self.state == 'arm capture county deaths'):
			self.state = 'capture county deaths'

	def handle_endtag(self, tag):
		if (tag == 'tr' and self.state == 'county header found'):
			self.state = 'arm capture county name'

	def handle_data(self, data):
		#logger.info("data: {0}".format(data))
		if (self.state == 'check for county header' and re.match("COVID-19 Confirmed Cases By County:",data)):
			self.state = "county header found"
		elif (self.state == 'capture county name'):
			self.county = data
			self.state = 'arm capture county count'
		elif (self.state == 'capture county count'):
			self.value['count'] = data
			self.state = 'arm capture county deaths'
		elif (self.state == 'capture county deaths'):
			self.value['deaths'] = data
			self.countyvaluemap[self.county] = self.value
			#logger.info("county: {0}, value: {1}".format(self.county,self.countyvaluemap[self.county]))
			self.state = 'arm capture county name'
			self.value = {}



def parsefile(reportdate, fileName):
	session = boto3.Session()
	client = session.client('s3')
	s3_obj = client.get_object(Bucket='useast1-fetched-data', Key=fileName)
	lines = s3_obj['Body'].read().decode('UTF-8')
	#logger.info(lines)
	s3_obj['Body'].close()
	parser = GaCovidV2PageParser()
	#file = open(fileName)
	#lines = file.read()
	parser.feed(lines)
	valuemap = parser.getcountyvaluemap()
	#logger.info("valuemap: {0}".format(valuemap))
	dynamodb = session.client('dynamodb')
	dbData = {}
	for key,value in valuemap.items():
		#logger.info ("key: {0}, count: {1}, deaths: {2}".format(key,valuemap[key]['count'],valuemap[key]['deaths']))
		dbData['county'] = {'S':key + ",GA" }
		dbData['reported-datetime'] = { 'S':reportdate }
		dbData['numCasesReported'] = { 'N' : value['count'].strip() }
		dbData['numDeathsReported'] = { 'N' : value['deaths'].strip() }
		dynamodb.put_item(TableName="covid-data", Item=dbData)
		logger.info(json.dumps(dbData))

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
	file = "covid19data-{:04d}{:02d}{:02d}-{:02d}00.html".format(year,month,day,hour)

	return (reportdate,file)

def event_handler(event, context):
	(reportdate,filename) = getdateandfile()
	logger.info("reportdate: {}, filename: {}".format(reportdate,filename))
	parsefile(reportdate,filename)
	return "success"
	
if __name__ == "__main__":
	# Use dummy event for testing lambda_function
	logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
	logger = logging.getLogger(name="__main__")
	logger.info("sys.argv: {0}".format(sys.argv))
	event_handler(None,None)
	#parsefile(sys.argv[1],sys.argv[2])
