#!/usr/bin/env python
import logging
import boto3
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import ssl
import urllib
import zipfile
import io

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
logger = logging.getLogger("fetchPage")

def putObject(key, content):
	s3 = boto3.session.Session().client("s3")
	response = s3.put_object(
			Bucket='useast1-fetched-data',
			ContentLength=len(content),
			ContentType='text/csv',
			Key=key,
			Body=content)
	#logger.debug("resonse: {0}".format(response))
	logger.info("Put response Code: {0}".format(response['ResponseMetadata']['HTTPStatusCode']))

def getcontent():
	"""
	Get the content from the host
	"""

	ctx = ssl.create_default_context()
	ctx.check_hostname = False
	ctx.verify_mode = ssl.CERT_NONE
	requestUrl = "https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip"
	#requestUrl = "https://d20s4vd27d0hk0.cloudfront.net"
	#requestUrl = "https://www.compucafe.com/"

	request = urllib.request.Request(url=requestUrl, method='GET')
	buff = bytearray()
	with urllib.request.urlopen(url=request,context=ctx) as response:
			logger.debug ("sent to: {0}".format(request.full_url))
			logger.debug("Response status: {0}".format(response.status))
			logger.debug("Headers: {0}".format(response.headers))
			for line in response:
				buff += line
	return buff

def getKey():
	now = datetime.now(tz=timezone(timedelta(hours=-4)))
	year = now.year
	month = now.month
	day = now.day

	if (now.hour < 12):
		hour = "1900"
		day -= 1
	elif (now.hour >= 12 and now.hour < 19):
		hour = "1200"
	else:
		hour = "1900"
	key = "countycases-{:04d}{:02d}{:02d}-{:s}.csv".format(year,month,day,hour)
	logger.info("key: {0}".format(key))
	return key

def lambda_handler(event,context):
	logger.info("Starting...")
	key = getKey()
	content = getcontent()
	z = zipfile.ZipFile(io.BytesIO(content))
	csvdata = ""
	with z.open('county_cases.csv') as csv:
		csvdata += csv.read().decode('UTF-8')

	#logger.info("csvdata: {0}".format(csvdata))
	# filenames = z.namelist()
	# for name in filenames:
	# 	logger.info("name: {}".format(name))
	# 	if (name == 'countycases.csv'):
	# 		logger.info("process the csv")

	#logger.info("content:{0}".format(content))
	#content = "<html><body>this is test content<br>Another content line.</body></html>"
	putObject(key,csvdata)	
	message = "fetched: {}".format(key)
	logger.info(message)
	return message
	
if __name__ == "__main__":
	lambda_handler(None,None)
