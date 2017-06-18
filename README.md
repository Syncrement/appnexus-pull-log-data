#### Syncrement Prefix:
This is based on a fork of the original script from https://github.com/mattpr/appnexus-pull-log-data

We have modified the original mostly in order for supporting AWS S3 uploading (and made some other improvements on the way).
Simply put, if you are using an `s3://bucket-name/path` as your dataDir value, and the shell has permissions (or you provided them in the config's `[aws]` section) we will do all the work against your S3 path directly instead of storing files locally.
We still maintain the checksum test to avoid re-downloading existing files.
Also note that we updated the script to use Python3 (because we are a new company and we like to start with the most up-to-date standards if possible)

___________________

# Read Me

This script enables you to pull "log level data" files from the AppNexus API.  

It is suitable for running on a server or on an individual workstation (e.g. an Ad-Ops person can pull a specific hour of data and grep for a specific event for troubleshooting purposes).

You must have the following in order to use this script:

- AppNexus account (aka "seat")
- Log Level Data enabled for your account
- A API enabled user/password.

See the AppNexus documentation for more details about their API.

# Usage

## Create config file

Create `pulllogleveldata-config` file and place it in the same directory as the script or provide the path to it using the -c flag (see usage below).

```
[LoginData]
username: apiuser
password: foobar
memberId: 911

[Paths]
## to store to s3 use the full path into your bucket's folder as s3://my-bucket-name/my/data/folder
dataDir: ./data

[RateLimiting]
requestsPerMin: 25

# if you use an s3 path, you muay provide the aws credentails to connect to your aws bucket
# If not provided, the code will use your default credentials as defined in ~/.aws/credentials

[aws]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
region=YOUR_AWS_REGION
```

## Before your run it
_Remember_: This script requires Python 3, and will not compile on earlier versions.
To ensure all dependencies are installed, run:

 `pip install -r requirements.txt`
 
This should ensure that all the needed dependencies are installed properly
 
## Run it

_Remember:_ This scripts requires Python 3.

```
python pulllogleveldata.py [-c configFilePath] [-d directoryForLogFiles] [-f filter] [-u updatedSince] [-s]
Where:
- configFilePath: option path to the config file. Default: pulllogleveldata-config in same folder as this script
- directoryForLogFiles: optional. Default taken from config file. Either a relative or AWS S3 path to where to save the files. if path starts with s3:// than this is the full path to a bucket in s3 to store in, such as s3://my-s3-bucket/my/data/
- filter: optional: used to filter out specific feed types. If the path name matches the filter, it will be included.
- updatedSince: optional YYYY_MM_DD_HH in UTC of the last feed downloaded to prevent going too far in history to match for files.
- -s optional: split into daily folders: if set, files will be groups by day into a YYYY_MM_DD sub folder for each feed type.
```

Running it with no parameters will perform a full update based on the information in the working dir's `./pulllogleveldata-config` file.

e.g.:  `python pulllogleveldata.py -d "./an-data/" -f "standard_feed/2017"`
will save files to an-data directory and only download files that have path/name matching standard_feed/2017.  So you can easily filter to a specific feed or specific date or specific hour.

e.g.:  `python pulllogleveldata.py -c "./config/pulllogleveldata-config" -d "s3://my-s3-bucket-name/an-data/" -u "2017_01_31_14"`
will save files to the bucket my-s3-bucket-name to a folder an-data/ and only download files that have updated since Jan 31st 2017 at 2 pm UTC at 2pm UTC.

## General Notes
1. This version of the script is adapted to Python 3! It will fail compiling on Python 2.
1. The script checks for the checksum of the local file against any existing files in the specified directory to avoid downloading the same file twice.  Only new/changed files are downloaded.
 * in case of s3 storage we keep track of the checksum value on S3 and will only download files if we can't verify the checksum on s3. (We store our own checksum in the metadata of the file, and fallback to the ETag value that in most cases will be of the file's MD5 checksum value, depending on the S3 upload method. Multi-part uploads might have a different checksum than singlepart uploads) 
1. You can run this with cron but no need to run more often than hourly, since the files are generated hourly on the AppNexus side. 
1. Per AppNexus logs are kept on the server for up to 10 days, so make sure to run this at least daily to give you enough time to handle issues.
1. AppNexus my correct files and resend them to you in a later time than the original. In that case, the timestamp will tell when it was updated, while the name will still be of the original time it was referring to.
