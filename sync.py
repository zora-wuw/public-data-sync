import argparse
import logging
import os
import os.path
import subprocess
from multiprocessing import Process
from datetime import datetime
from datetime import timedelta
import CustomLogHandler
import shutil
import shlex
from multiprocessing import Pool
from pymongo import MongoClient
import pymongo

#---------------------------------------------------------
# Validates an integer is positive
#---------------------------------------------------------
def integer_param_validator(value):
	if int(value) <= 0:
		raise argparse.ArgumentTypeError("%s is an invalid, please specify a positive value greater than 0" % value)
	return int(value)

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', help='Path to place the public data files', default='./')
parser.add_argument('-s', '--summaries', help='Download summaries', action='store_true')
parser.add_argument('-a', '--activities', help='Download activities', action='store_true')
parser.add_argument('-t', '--tar', help='Compress the dump', action='store_true')
parser.add_argument('-max', '--max_cpus', default=30)
parser.add_argument('-d', '--days', help='Days to sync', type=integer_param_validator)
parser.add_argument('-host', '--host', help='MongoDB host', default='localhost')
parser.add_argument('-port', '--port', help='MongoDB port', default=27017)
parser.add_argument('-u', '--username', help='MongoDB username', default='')
parser.add_argument('-psword', '--password', help='MongoDB password', default='')
parser.add_argument('-db', '--database', help='MongoDB database name', default='')
parser.add_argument('-c', '--collection', help='MongoDB collection name', default='')
args = parser.parse_args()

path = args.path if args.path.endswith('/') else (args.path + '/')
path = path + 'ORCID_public_data_files/'
download_summaries = args.summaries
download_activities = args.activities
days_to_sync = args.days
tar_dump = args.tar
MAX_CPUS = int(args.max_cpus)
ip = args.host
port = int(args.port)
user_name = args.username
psword = args.password
db_name = args.database
collection_name = args.collection

logger = logging.getLogger('sync')
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
fileHandler = CustomLogHandler.CustomLogHandler('sync.log')
fileHandler.setFormatter(formatter)
logger.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

summaries_bucket = 's3://v2.0-summaries/'
activities_bucket = 's3://v2.0-activities/'

date_format = '%Y-%m-%d %H:%M:%S.%f'
date_format_no_millis = '%Y-%m-%d %H:%M:%S'

now = datetime.now()
month = str(now.month)
year = str(now.year)

#---------------------------------------------------------
# Download modified files in summaries
#---------------------------------------------------------
def download_summaries_file(orcid_to_sync):
	# create mongodb instance for each process
	client = MongoClient(ip, int(port), username=user_name, password=psword, maxPoolSize=10000)
	db = client[db_name]

	try:
		suffix = orcid_to_sync[-3:]
		prefix = suffix + '/' + orcid_to_sync + '.xml'
		cmd = 'aws s3 cp ' + summaries_bucket + prefix + ' ' + path + 'summaries/' + prefix + ' --only-show-errors'
		subprocess.call(shlex.split(cmd), shell=False)
		record_dict = {}
		record_dict["_id"] = orcid_to_sync
		with open(path + 'summaries/' + prefix,"r",encoding="UTF-8") as f:
			xml_content = f.read()
			record_dict["xml-content"] = xml_content
			record_dict["last-updated"] = datetime.now()

			try:
				db[collection_name].insert_one(record_dict)
			except pymongo.errors.DuplicateKeyError:
				db[collection_name].delete_one({"_id":record_dict["_id"]})
				db[collection_name].insert_one(record_dict)
			except Exception as e:
				logging.info("------------------------------------")
				logging.info(e)
				logging.info("Error object id(orcid/file name): {}".format(record_dict["_id"])) 
	except Exception as e:
		logging.info("------------------------------------")
		logging.info(e)

#---------------------------------------------------------
# Download modified files in activities
#---------------------------------------------------------
def download_activities_file(orcid_to_sync):
	client = MongoClient(ip, int(port), username=user_name, password=psword, maxPoolSize=10000)
	db = client[db_name]

	try:
		suffix = orcid_to_sync[-3:]
		prefix = suffix + '/' + orcid_to_sync + '/'
		local_directory = path + 'activities/' + prefix
		# fetch data from S3
		logger.info('aws s3 sync ' + activities_bucket + prefix + ' ' + local_directory + ' --delete')
		cmd = 'aws s3 sync ' + activities_bucket + prefix + ' ' + local_directory + ' --delete' + ' --only-show-errors'
		subprocess.call(shlex.split(cmd), shell=False)
		# aws cli will remove the files but not the folders so,
		# we need to check if the folders are empty and delete it
		if os.path.exists(local_directory) and os.path.isdir(local_directory):
			for root, dirs, files in os.walk(local_directory):
				for dir in dirs:
					if not os.listdir(local_directory + '/' + dir):
						logger.info('Deleting %s because it is empty', local_directory + '/' + dir)
						shutil.rmtree(local_directory + '/' + dir)
			if not os.listdir(local_directory):
				logger.info('Deleting %s because because it is empty', local_directory)
				shutil.rmtree(local_directory)
			# delete the suffix folder if needed
			if not os.listdir(path + 'activities/' + suffix):
				logger.info('Deleting %s because because it is empty', path + 'activities/' + suffix)
				shutil.rmtree(path + 'activities/' + suffix)
	except Exception as e:
		logging.info("------------------------------------")
		logging.info(e)

#---------------------------------------------------------
# Main process
#---------------------------------------------------------
if __name__ == "__main__":
	start_time = datetime.now()

	# Download the lambda file
	logger.info('Downloading the lambda file')
	cmd = 'aws s3 cp s3://orcid-lambda-file/last_modified.csv.tar last_modified.csv.tar'
	subprocess.call(shlex.split(cmd), shell=False)

	# Decompress the file
	logger.info('Decompressing the lambda file')
	cmd = 'tar -xzvf last_modified.csv.tar'
	subprocess.call(shlex.split(cmd), shell=False)

	# Look for the config file
	last_sync = None
	if days_to_sync is not None:
		last_sync = (datetime.now() - timedelta(days=days_to_sync))
	elif os.path.isfile('last_ran.config'):
		f = open('last_ran.config', 'r')
		date_string = f.readline()
		last_sync = datetime.strptime(date_string, date_format)
	else:
		last_sync = (datetime.now() - timedelta(days=30))

	logger.info('Sync records modified after %s', str(last_sync))

	records_to_sync = []

	is_first_line = True

	for line in open('last_modified.csv', 'r'):
		if is_first_line:
			is_first_line = False
			continue
		line = line.rstrip('\n')
		elements = line.split(',')
		orcid = elements[0]

		last_modified_str = elements[3]
		try:
			last_modified_date = datetime.strptime(last_modified_str, date_format)
		except ValueError:
			last_modified_date = datetime.strptime(last_modified_str, date_format_no_millis)

		if last_modified_date >= last_sync:
			records_to_sync.append(orcid)
			if len(records_to_sync) % 1000 == 0:
				logger.info('Records to sync so far: %s', len(records_to_sync))
		else:
			# Since the lambda file is ordered by last_modified date descendant,
			# when last_modified_date < last_sync we don't need to parse any more lines
			break

	logger.info('Records to sync: %s', len(records_to_sync))

	if download_summaries:
		pool = Pool(processes=MAX_CPUS)
		pool.map(download_summaries_file,records_to_sync)
		pool.close()
	
	if download_activities:
		pool = Pool(processes=MAX_CPUS)
		pool.map(download_activities_file,records_to_sync)
		pool.close()

	# keep track of the last time this process ran
	file = open('last_ran.config','w')
	file.write(str(start_time))
	file.close()
