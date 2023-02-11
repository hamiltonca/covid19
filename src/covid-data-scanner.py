#/usr/bin/env python
import boto3
import logging
import sys
import json
from datetime import datetime,timezone, timedelta, time
import dateutil

def getAllCountyData():
	logger = logging.getLogger('getAllCountyData')
	db = boto3.Session().client('dynamodb')
	requestArgs = {
		'TableName': 'covid-data-3'
	}
	stateInfections = {} 
	loading = True
	totalItems = 0
	while(loading):
		response = db.scan(**requestArgs)
		for data in response['Items']:
			county = data['county']['S']
			countyData = stateInfections.get(county,  {})
			
			countyDates = countyData.get('dates', [])
			countyDates.append(data['reported-datetime']['S'])
			countyData['dates'] = countyDates

			countyInfections = countyData.get('infections',[])
			countyInfections.append(data['numCasesReported']['N'])
			countyData['infections'] = countyInfections

			countyDeaths = countyData.get('deaths',[])
			countyDeaths.append(data['numDeathsReported']['N'])
			countyData['deaths'] = countyDeaths

			stateInfections[county] = countyData

		itemsInPass = len(response['Items'])
		totalItems += itemsInPass
		logger.info("Loaded: {0} items. Total: {1}".format(itemsInPass, totalItems))
		if ('LastEvaluatedKey' not in response):
			loading = False;
			logger.info("Done")
		else:
			requestArgs['ExclusiveStartKey'] = response['LastEvaluatedKey']

	return stateInfections

def validUntil():
	logger = logging.getLogger('validUntil')
	offsets = [ 5,6,0,1,2,3,4]
	mytz = dateutil.tz.gettz('EST5EDT')
	day = datetime.combine(datetime.now(),time(hour=19),tzinfo=mytz)
	vu = day - timedelta(days=offsets[day.weekday()]) + timedelta(days=7)
	logger.info("Day is {2}, {3} Wednesday is: {0}, {1}".format(vu, vu.weekday(), day, day.weekday()))
	logger.info("expires: {0}".format(vu.isoformat()))
	return vu

def lambda_handler(event, context):
	logging.getLogger().setLevel(logging.INFO)
	logger = logging.getLogger('lambda_handler')
	response = {
		"statusCode" : 200,
		"body" : getAllCountyData()
		# "body" : {}
	}
	s3 = boto3.session.Session().client('s3')
	validUntilVal = validUntil()
	logger.info("Valid-Until: {0}".format(validUntilVal.isoformat()))
	s3.put_object(
		ContentType='application/json',
		Expires=validUntilVal.isoformat(),
		Metadata = { 'Access-Control-Allow-Origin' : '*'},
		Bucket='useast1-covid-charts', Key='current-covid-data.json',
		Body=json.dumps(response))	
	return 'OK'

if __name__ == "__main__":
	logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
	logger = logging.getLogger(name="__main__")
	logger.info("sys.argv: {0}".format(sys.argv))
	# county = sys.argv[1]
	# countyInfections = getAllCountyData()
	# logger.info("countyInfections: {0}".format(json.dumps(countyInfections)))
	response = lambda_handler(event=None, context=None)
	#print (json.dumps(response))
