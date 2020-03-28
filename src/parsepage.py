#!/usr/bin/env python
import logging
import sys
from html.parser import HTMLParser
"""
A simple class to capture County covid-19 cases by county from
the Georgia Department of Healt's covid-19 status page.
Once parsed, the data is stored in the countyvaluemap for further use.
"""
class GaCovidPageParser(HTMLParser):
	state = str()
	county = str()
	value = str()
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

#		print("data: {0}".format(data))

def parsefile(fileName):
	parser = GaCovidPageParser()
	file = open(fileName)
	lines = file.read()
	parser.feed(lines)
	valuemap = parser.countyvaluemap
	for key,value in valuemap.items():
		logger.info ("key: {0}, value: {1}".format(key,value))



if __name__ == "__main__":
	# Use dummy event for testing lambda_function
	logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",  level=logging.INFO)
	logger = logging.getLogger(name="__main__")
	logger.info("sys.argv: {0}".format(sys.argv))
	parsefile(sys.argv[1])
