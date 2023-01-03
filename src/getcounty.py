#/usr/bin/env python
import boto3
import logging
import sys
import json

def getCountyData(county):
	db = boto3.Session().client('dynamodb')
	response = db.query(
		TableName='covid-data', 
		KeyConditionExpression="#S = :county",
		ExpressionAttributeNames= {
			"#S" : "county"
		},
		ExpressionAttributeValues={
			":county": {"S":county}
		}
	)
	countyInfections = [] 
	for data in response['Items']:
		countyInfections.append(
			{
				'datetime' : data['reported-datetime']['S'],
				'infections' : data['numCasesReported']['N'],
				'deaths' : data['numDeathsReported']['N']
			})

	# logger.info("response: {0}".format(response['Items']))
	return countyInfections

def lambda_function(event, context):
	county = event['county']
	response = {
		"satusCode" : 200,
		"body" : getCounty(county)
	}
	return response

if __name__ == "__main__":
	logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
	logger = logging.getLogger(name="__main__")
	logger.info("sys.argv: {0}".format(sys.argv))
	county = sys.argv[1]
	countyInfections = getCountyData(county)
	logger.info("countyInfections: {0}".format(json.dumps(countyInfections)))
