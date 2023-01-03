import boto3
import logging
import sys
import json

def getAllCountyData():
	db = boto3.Session().client('dynamodb')
	response = db.scan(
		TableName='covid-data'
	)
	stateInfections = {} 
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
	return stateInfections

def lambda_handler(event, context):
	response = {
		"statusCode" : 200,
		"body" : getAllCountyData()
	}
	s3 = boto3.session.Session().client('s3')
	s3.put_object(ACL='public_read',
		ContentType='application/json',
		Bucket='useast1-covid-charts', Key='current-covid-data.json',
		Metadata={"Access-Control-Allow-Origin" : "*"}
		Body=json.dumps(response))	

	return response
