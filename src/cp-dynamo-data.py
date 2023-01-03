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

def saveData(tableName, item, db):
	db.put_item(
		TableName=tableName,
		Item=item
	)

def cpTableData(tableName):
	db = boto3.Session().client('dynamodb')
	loading = True
	totalLoaded = 0
	requestArgs = { 
			'TableName':tableName,
			'Limit':1000
	}
	while(loading):
		loadedInPass = 0
		response = db.scan(**requestArgs)
		for item in response['Items']:
			reportedDateTime = datetime.fromisoformat(item['reported-datetime']['S'])
			weekday = reportedDateTime.weekday()
			if (weekday == 2 and reportedDateTime.hour == 19):
				saveData('covid-data-3', item, db)
				loadedInPass += 1

		if ('LastEvaluatedKey' not in response):
			loading = False;
		else:
			requestArgs['ExclusiveStartKey'] = response['LastEvaluatedKey']

		totalLoaded += loadedInPass
		logger.info("From table: {0}, Total loaded: {1}, Loaded in pass {2} records. loading == {3}".format(tableName,totalLoaded,loadedInPass,loading))
		# loading = False
	return


	
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
logger = logging.getLogger(name="__main__")

if __name__ == '__main__':
	logger.info("Starting")
	tableNames = ['covid-data-2']
	tableData = []
	for tableName in tableNames:
		logger.info("Loading table: {0}".format(tableName))
		tableData.extend(cpTableData(tableName))
	logger.info("Tables loaded. total record count: {0}".format(len(tableData)))
