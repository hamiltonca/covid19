# Covid19
This repo contains Python scripts that run as AWS Lambdas and perform the following functions:
* Fetch covid data from the Georgia Department of Health web site and storing it as a file in a private AWS S3 bucket. The Georgia Department of Health produces a data file that contains the number of infections and deaths for a point in time. This file only contains data for a single period and is updated weekly every Wednesday. The file does not contain any historical data and thus must be fetched, parsed and stored every week in order to create an historical record.
* Parse the point-in-time data file (CSV) and insert the data for the reporting period into a DynamoDB table. The key for the table is the Georgia county name and the data of the point-in-time data.
* Scan the DynamoDB table for all the historical data and produce a JSON formatted file containing the historical data on Covid 19 infections and deaths for consumption. Since the data is only produced once a week by the Georgia DoH, the JSON output is produced at same cadience. This data is input for the line graph app using React. That app is also here on github at https://github.com/hamiltonca/ga-covid-chart