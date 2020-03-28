#!/usr/bin/env python
import logging
import sys
import re
from html.parser import HTMLParser
"""
A simple class to capture County covid-19 cases by county from
the Georgia Department of Healt's covid-19 status page.
Once parsed, the data is stored in the countyvaluemap for further use.
"""
class GaCovidPageParser(HTMLParser):
	state = str()
	county = str()
	countyvaluemap = {}
	def handle_starttag(self, tag, attrs):
		if (tag == 'thead'):
			#print("start: {0}".format(tag))
			self.state = tag
		if (tag == 'th' and self.state == 'thead'):
			self.state = 'capture th'
		if (tag == 'td' and self.state == 'arm capture county name'):
			self.state = 'capture county name'
		if (tag == 'td' and self.state == 'arm capture county value'):
			self.state = 'capture county value'

	def handle_data(self, data):
		if (self.state == 'capture th'):
			if (data == 'County'):
				#print("begin capture")
				self.state = "arm capture county name"
		elif (self.state == 'capture county name'):
			self.county = data
			self.state = 'arm capture county value'
		elif (self.state == 'capture county value'):
			self.value = data
			self.countyvaluemap[self.county] = self.value
			self.state = 'arm capture county name'

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
			self.state = 'arm capture county name'



def parsefile(fileName):
	parser = GaCovidV2PageParser()
	file = open(fileName)
	lines = file.read()
	parser.feed(lines)
	valuemap = parser.countyvaluemap
	for key, value in valuemap.items():
		logger.info ("key: {0}, count: {1}, deaths: {2}".format(key,value['count'],value['deaths']))



if __name__ == "__main__":
	# Use dummy event for testing lambda_function
	logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
	logger = logging.getLogger(name="__main__")
	logger.info("sys.argv: {0}".format(sys.argv))
	parsefile(sys.argv[1])
