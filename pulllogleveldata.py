#!/usr/bin/env python3

import json, requests, os, hashlib, sys, time, pickle, getopt, math, configparser, re, boto3, tempfile
from urllib.parse import urlparse


def checkAuth():
		r = requests.get('http://api.appnexus.com/user')
		resp = json.loads(r.text)
		if resp['response'].get('status', False) != "OK":
						#print "Auth is good"
						return False
		else:
						#print "No auth"
						return True

def saveCookies (cookieFile, cookieJar):
		if os.path.exists(cookieFile):
						os.remove(cookieFile)

		f = open(cookieFile, 'wb')
		pickle.dump(cookieJar, f)

def getSavedCookies (cookieFile):
		if os.path.exists(cookieFile):
						f = open(cookieFile, 'rb')
						cookieJar = pickle.load(f)
						#print "Cookies loaded"
						return cookieJar
		else:
						return False

def getAuth(username, password, cookieFile):
		cookieJar = getSavedCookies(cookieFile)
		authUrl = 'https://api.appnexus.com/auth'
		authPayload = {
						"auth":{
								"username":username,
								"password":password
						}
				}

		if not cookieJar or not checkAuth():
						r = requests.post(authUrl, data=json.dumps(authPayload))
						resp = json.loads(r.text)
						if resp['response'].get('status', False) != "OK":
								print('Auth failed: ' + str(resp['response']))
								return False
						else:
								#print "Successfully authenticated"
								cookieJar = r.cookies
								saveCookies(cookieFile, cookieJar)
		return cookieJar


def getAvailableLogs(cookieJar, updatedSince):
		logListUrl = 'http://api.appnexus.com/siphon'

		# set optional parameters
		params = {}
		if updatedSince:
			params["updated_since"]=updatedSince

		r = requests.get(logListUrl, cookies=cookieJar, params=params)
		resp = json.loads(r.text)["response"]

		if resp.get("status", False) != "OK":
				return False
		else:
				return resp["siphons"]

def ensureDirExists (path):
		# check if an s3 path (starts with s3://
		if path.startswith("s3://"):
				# handle as s3 path
				return ensureS3BucketExists(path)
		elif os.path.isdir(path):
				# local path and exists
				return True
		elif os.path.exists(path):
				print("Error: path ("+path+") exists but is not directory")
				return False
		else:
				# create local path for the first time
				os.makedirs(os.abspath(path))
				if os.path.isdir(path):
						return True
				else:
						print("Tried to create dir ("+path+") but didn't seem to work")
						return False

# read the checksum  of the S3 object.
# We store the checksum as a metadata named anchecksum.
# Note that if this doesn't exist, we will use the ETag as a fallback because in most cases
# ETag will hold the md5 checksum of the file. Worst case, our code should efault to upload a fresh copy of the file
# which will then set the proper anchecksum metadata value.
# @return the server's checksum value, or "" if the file or the checksum couldn't be found
def readS3Checksum(s3Path):
	s3 = s3Client()
	bucket, key = parseS3Path(s3Path)
	try:
		objHead = s3.head_object(Bucket = bucket, Key = key)
		serverChecksum = objHead['Metadata']['anchecksum']
		# Sometime the ETag value will be of the MD5 of the file, so if we got nothing from the previous call we can fallback
		# on the etag, incase it holds the information we need
		if (serverChecksum == None or serverChecksum == ""):
			# fallback on ETag. If not a match, we will "eat the cost" once and re-upload the file
			serverChecksum = objHead['ETag']
		return serverChecksum
	except:
		return ""


def isNewLogFile (localFileName, anServerFileMD5):
	if localFileName.startswith("s3://"):
		chksumS3 = readS3Checksum(localFileName)
		# return True if the checksumes do not match, i.e. server's file is different
		if chksumS3 == anServerFileMD5:
		    return False
		else:
			if chksumS3 != "": print(localFileName + " exists, but checksum wrong '" + chksumS3 + "' / '" + anServerFileMD5 + "'")
			return True
	elif os.path.exists(localFileName):
		chksumDisk = checksum(localFileName)
		if anServerFileMD5 == chksumDisk:
			return False
		else:
			print(localFileName + " exists, but checksum wrong '" + chksumDisk + "' / '" + anServerFileMD5 + "'")
			return True
	else:
			return True

# concat the full name of the file from the various parts
# dataDir the base path to the file, may be a local path or an s3://bucket/base-path
# mergeDailyFolder: if False, we will create a subfolder for each day as YYYY_MM_DD/ for all the daily files
# logType: the logType as provided from AppNexus (e.g. standard-feed, pixel-feed etc)
# logHour: the hour part of the log e.g. YYYY_MM_DD_HH
# timestamp: the actual timestamp in UTC that this data was last updated (per AN API, may not be correlated
#            with the logHour if the file was corrected later on)
# part: when the hourly file is split into multiple files, this is the part number
# dupe: if this is a duplicate (updated file) the timestamp will be added to the file as -dupe-{timestamp}
# extension: the file extension: e.g. gz
# returns: a complete path made of all the parts
def buildFileName (dataDir, mergeDailyFolder, logType, logHour, timestamp, part, dupe, extension):
	# if not merging daily folders, create a daily folder with the date part of the logHour (first
	# 10 character i.e. YYYY_MM_DD/
	dailyFolder = "" if mergeDailyFolder else (logHour[:10] + "/")

	name = dataDir + "/" + logType + "/" + dailyFolder + logHour
	if dupe:
			name += "-dupe-" + timestamp
	name += "_pt" + part + "." + extension
	return name

def downloadFile(url, params, localFile, cookieJar):
		#
		# Setup progress bar
		#
		maxProgress = 40
		sys.stdout.write("\t")
		sys.stdout.write("[%s]" % (" " * maxProgress))
		sys.stdout.flush()
		sys.stdout.write("\b" * (maxProgress+1)) # return to start of line, after '['
		currProgress = 0

		#
		# Do the download
		#
		r = requests.get(url, cookies=cookieJar, params=params, stream=True)
		dlData = {}
		dlData["size"] = int(r.headers['content-length'].strip())
		dlData["dlsize"] = 0
		with open(localFile, 'wb') as f:
						for chunk in r.iter_content(chunk_size=1024):
							if chunk: # filter out keep-alive new chunks
									dlData["dlsize"] += len(chunk)
									f.write(chunk)
									f.flush()
									# update progress bar
									if math.floor(float(dlData["dlsize"]) / dlData["size"] * maxProgress ) > currProgress:
										currProgress += 1
										sys.stdout.write("|")
										sys.stdout.flush()
		sys.stdout.write("\n")
		return dlData

def checksum(filepath):
		md5 = hashlib.md5()
		blocksize = 8192
		f = open(filepath, 'rb')
		while True:
						data = f.read(blocksize)
						if not data:
							break
						md5.update(data)
		return md5.hexdigest()


def checkDupes (logFiles):
		d = dict()
		for log in logFiles:
				k = log["name"] + "-" + log["hour"]
				v = log
				if k in d: # dupe!
						old = d[k]
						oldTimeStamp = old["timestamp"]
						logTimeStamp = log["timestamp"]
						print("Found duplicate for log: " + k)
						print("Will keep the one with the newest timestamp ("+oldTimeStamp+" vs "+logTimeStamp+").")
						if logTimeStamp < oldTimeStamp:
								log["dupe"] = True
								k = k + "-" + logTimeStamp
								d[k] = v
						else:
								d[k] = v
								old["dupe"] = True
								k = k + "-" + oldTimeStamp
								d[k] = old
				else:
						d[k] = v
		return list(d.values())

def downloadNewLogs (logFiles, dataDir, mergeDailyFolder, filter, url_logDownload, cookieJar, minTimePerRequestInSecs):
		maxRetries = 5
		numExisting = 0
		numDownloaded = 0
		numFailed = 0
		numFiltered = 0
		isS3 = dataDir.startswith("s3://")

		for log in logFiles:
						logType = log["name"]
						ensureDirExists(dataDir + "/" + logType)
						logHour = log["hour"]
						timestamp = log["timestamp"]

						dupe = False
						if "dupe" in log and log["dupe"]:
								dupe = True

						if dupe: # don't download dupes
								continue

						for logFile in log["splits"]:
							splitPart = logFile["part"]
							anChecksum = logFile["checksum"]
							status = logFile["status"] # e.g. new

							filename = buildFileName(dataDir, mergeDailyFolder, logType, logHour, timestamp, splitPart, dupe, "gz")
							downloadTo = filename

							if filter != '' and filename.find(filter) == -1:
								numFiltered += 1
								continue # skip downloading this one

							if isNewLogFile(filename, anChecksum):
									#download
									params_logDownload = dict(
											split_part=splitPart,
											hour=logHour,
											timestamp=timestamp,
											siphon_name=logType
									)
									trys = 0
									downloadCorrect = False
									if isS3:
										downloadTo = tempfile.NamedTemporaryFile(prefix="~dwnld-", suffix=".gz", delete=True).name
										
									while trys < maxRetries and not downloadCorrect:

											print("Getting: " + filename + " (try " + str(trys) + ")")
											timeStart = time.time()
											dlData = downloadFile(url_logDownload, params_logDownload, downloadTo, cookieJar)
											timeEnd = time.time()
											timeElapsed = timeEnd - timeStart
											dlSpeedk = round(float(dlData["dlsize"])/1024/timeElapsed, 2)
											dlActual = round(float(dlData["dlsize"])/1024/1024, 2)
											dlExpected = round(float(dlData["size"])/1024/1024, 2)
											print("\t" + str(dlActual) + " of " + str(dlExpected) + " MB in " + str(round(timeElapsed, 1)) + " seconds ("+str(dlSpeedk)+" kbps)")
											trys += 1

											downloadChecksum = checksum(downloadTo)

											if downloadChecksum == anChecksum:
												downloadCorrect = True
												if isS3:
													uploadToS3Path(downloadTo, filename, anChecksum)
													os.remove(downloadTo)
											else:
												print("\tAppNexus Checksum ("+anChecksum+") doesn't match downloaded file ("+downloadChecksum+").")

											sleepTime = minTimePerRequestInSecs - timeElapsed
											
											if sleepTime > 0:
												print("Sleeping for " + str(sleepTime) + " seconds")
												time.sleep(sleepTime)

									if downloadCorrect:
											numDownloaded += 1
									else:
											print("Failed to successfully download " + filename + ".  Removing.")
											numFailed += 1
											os.remove(downloadTo)

							else:
									#already have this one
									numExisting += 1

		print("Skipped " + str(numFiltered) + " (filtered) files")
		print("Skipped " + str(numExisting) + " (existing) files")
		print("Downloaded " + str(numDownloaded) + " (new/changed) files")
		print("Failed to download " + str(numFailed) + " files.")


# test that the given path is a valid s3 path that we have permissions to access
def ensureS3BucketExists(path):
		backet, key = parseS3Path(path)
		s3 = s3Client()
		try:
			bucketLocation = s3.get_bucket_location(Bucket = backet)
			# if we are here, bucket exists
			return (bucketLocation and bucketLocation['LocationConstraint'])
		except:
			print("Error: Bucket {0} not found".format(backet))
			return False


def uploadToS3Path(localFile, s3path, checksum):
	s3 = s3Client()
	bucket, key = parseS3Path(s3path)
	s3.upload_file(localFile, bucket, key, ExtraArgs={"Metadata": {"anchecksum": checksum}})
	return True

def s3Client():
	global awsAccessKeyId, awsSecret, awsRegion
	if awsAccessKeyId and awsSecret and awsRegion:
		return boto3.client("s3", aws_access_key_id = awsAccessKeyId, aws_secret_access_key = awsSecret, region_name = awsRegion)
	else:
		return boto3.client("s3")


def parseS3Path(s3Path):
	parsedPath = urlparse(s3Path)
	if parsedPath.scheme == "s3":
			bucket = parsedPath.netloc
			key = parsedPath.path
			if (key.startswith("/")):
				key = key[1:]
			return bucket, key
	else:
			raise ValueError ("Illegal s3 path: path must start with s3://bucket-name/", s3Path)




def main (argv):

		if sys.version_info < (3,0):
				raise "Must use Python 3.0 or newer"

		# TODO: Create a Configuration class to encapsulate all these variables and their parsing instead of using global variables
		# config vars
		configFile = "./pulllogleveldata-config" # name of default config file that contains all the following settings
		username = ""
		password = ""
		memberId = "" # appnexus "Seat" id
		dataDir = "" # where to save log files
		requestsPerMin = 25 # limit of how many request per minit to not hit the limit on AppNexus API calls
		updateSince = ""    # default updateSince value for AN API call (by default retrieve all, which is about 10 days worth of data)
		minTimePerRequestInSecs=60
		mergeDailyFolder = True

		global awsAccessKeyId, awsSecret, awsRegion # AWS credentials
		awsAccessKeyId = ""
		awsSecret = ""
		awsRegion = ""


		def read_config(configFileAbs):
			nonlocal username, password, memberId, dataDir, requestsPerMin, minTimePerRequestInSecs
			global awsAccessKeyId, awsSecret, awsRegion

			if os.path.isfile(configFileAbs) == False:
				print("Error: config file '" + configFileAbs + "' not found.")
				sys.exit(2)

			# load config
			try:
					Config = configparser.ConfigParser()
					Config.read(configFileAbs)
					LoginDataSection = Config["LoginData"]
					if LoginDataSection:
						username = LoginDataSection.get("username")
						password = LoginDataSection.get("password")
						memberId = LoginDataSection.get("memberId")
					else:
						print("Error: config file must have [LoginData] section defined.")
						sys.exit(2)

					PathsSection = Config["Paths"]
					if PathsSection:
						dataDir = PathsSection.get("dataDir", dataDir)

					RateLimitingSection = Config["RateLimiting"]
					if RateLimitingSection:
						requestsPerMin = RateLimitingSection.getint("requestsPerMin", requestsPerMin)

					if Config.has_section("aws"):
						AwsSection = Config["aws"]
						awsAccessKeyId = AwsSection.get("access_key_id", awsAccessKeyId)
						awsSecret =      AwsSection.get("secret_access_key", awsSecret)
						awsRegion =      AwsSection.get("region", awsRegion)

					# we do naive throttling (non-optimal) because this script isn't
					# aware of your other API usage that may be happening simultaneously.
					minTimePerRequestInSecs = 60/requestsPerMin

			except configparser.NoSectionError as sectionName:
					print("Error: reading config file '" + configFileAbs + " ': Section not found '" + sectionName + "'")
					sys.exit(2)

			# End of _read_config

		cookieFile = './authCookies'
		cookieJar = {}
		logFiles = {}

		def getUsage():
			nonlocal dataDir
			usage  = "USAGE:\n"
			usage += "pulllogleveldata.py       # download all files we don't currently have in '{0}'\n"
			usage +=     "\t-c <conffile>       # optional config file. Default is ./pulllogleveldata-config in the working dir\n"
			usage +=     "\t-f <filter>         # only download files matching filter\n"
			usage +=     "\t-d <datadir>        # change download location from default\n"
			usage +=     "\t-s                  # Split days: if set, feed files will be broken by day into sub directory\n"
			usage +=     "\t                    # e.g. standard-feed/2017_06_17/2017_06_17_*.gz\n"
			usage +=     "\t-u <YYYY_MM_DD_HH>  # last update time UTC (see AppNexus definition of siphon's updated_since)\n"
			usage +=     "\t-h                  # help: show this help\n\n"
			return usage.format(dataDir)

		# parse args
		try:
				opts, args = getopt.getopt(argv,"c:f:d:su:h")
		except getopt.GetoptError as error:
				print("Error parsing command line: " + error + "\n\n" + getUsage())
				sys.exit(2)

		filter = ''
		upadtedSince = ''


		# First check if there is a -c parameter and if so, reload the configuration from this file before processing the other parameters
		
		for opt, arg in opts:
				if opt in ("-c"):
					configFile = arg

		# convert the path into an absolute path
		configFileAbs = os.path.abspath(os.path.expanduser(configFile))

		# Read configuration
		read_config(configFileAbs)

		for opt, arg in opts:
				if opt in ("-h", "--help"):
						print(getUsage())
						sys.exit()
				elif opt in ("-f", "--filter"):
						filter = arg
				elif opt in ("-d", "--datadir"):
						dataDir = arg
				elif opt in ("-u", "--update-since"):
						updateSince = arg
						## validate format of update since
						if re.match("\d{4}\_\d{2}\_\d{2}|\_\d{2}", upadtedSince) == False:
							print("updatedSince value '" + updateSince + "' is not matching YYYY_MM_DD_HH")
				elif opt in ("-s", "--split"):
						## turn daily-merge off, files will go into a daily subfolder made of the first 10 chars
						## of the log's name i.e. YYYY_MM_DD
						mergeDailyFolder = False
				elif opt in ("-c"):
						## simply ignore, we already processed the -c option prior to this for loop, just make sure we don't fail
						## on unknown config option
						pass
				else:
						# Unknown config option, exit with error
						print(getUsage())
						sys.exit()

		#
		# Do the work
		#
		print("Running " + __file__ + " using this configuration:\n \
[LoginData]\n \
username={0}\n \
password=*******\n \
memberId={1}\n \
\n \
[Paths]\n \
dataDir={2}\n \
\n \
[aws]\n \
awsAccessKeyId={3}\n \
awsSecret=******\n \
awsRegion={4}\n \
\n \
Other Parameters:\n \
requestsPerMin={5}\n \
filter={6}\n \
updatesSince={7}\n \
Create daily sub folders={8}\n \
\n".format(username, memberId, dataDir,awsAccessKeyId,awsRegion, requestsPerMin, filter, updateSince, not(mergeDailyFolder)))

		try:
				print("Use CTRL-C to quit.\n")
				print("Authenticating...")
				cookieJar = getAuth(username, password, cookieFile)
				if cookieJar:
						print("Getting AppNexus log listing...")
						logFiles = getAvailableLogs(cookieJar, updateSince)
						if logFiles:
								print("Choosing new/updated log files to download...")
								logFiles = checkDupes(logFiles)
								if ensureDirExists(dataDir):
										print("Downloading new/updated log files...")
										url_logDownload = 'http://api.appnexus.com/siphon-download?member_id=' + memberId
										downloadNewLogs(logFiles, dataDir, mergeDailyFolder, filter, url_logDownload, cookieJar, minTimePerRequestInSecs)
								else:
										print("ERROR: Could not create data directory.")
						else:
								print("ERROR: Could not get log listing.")
				else:
						print("ERROR: AppNexus Authentication failed.")
		except KeyboardInterrupt:
				print("   ...Okay, quitting.")
				sys.exit(1)



if __name__ == "__main__":
		main(sys.argv[1:])