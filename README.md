NOTE:
This is based on a fork of the original script from https://github.com/mattpr/appnexus-pull-log-data

We are modifying it to support dowloading and syncing against a s3 bucket.

----------------


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

## Run it

```
python pulllogleveldata.py [-c configFilePath] [-d directoryForLogFiles] [-f filter] [-u updatedSince]
Where:
- configFilePath: option path to the config file. Default: pulllogleveldata-config in same folder as this script
- directoryForLogFiles: optional. Default taken from config file. Either a relative or AWS S3 path to where to save the files. if path starts with s3:// than this is the full path to a bucket in s3 to store in, such as s3://my-s3-bucket/my/data/
- filter: optional: used to filter out specific feed lick feed-standard etc.
- updatedSince: optional YYYY_MM_DD_HH in UTC of the last feed downloaded to prevent going too far in history to match for files.
```

Running it with no paramters will perform a full update based on the information in the local pulllogleveldata-config file

e.g.:  `python pulllogleveldata.py -d "~/an-data/" -f "standard_feed"`
will save files to an-data directory and only download files that have path/name matching standard_feed.  So you can easily filter to a specific feed or specific date or specific hour.

e.g.:  `python pulllogleveldata.py -c "~/pulllogleveldata-config" -d "s3://my-s3-bucket-name/an-data/" -u "2017_01-31-14"`
will save files to the bucket my-s3-bucket-name to a folder an-data/ and only download files that have updated since Jan 31st 2017 at 2pm UTC.

## Other notes

The script checks checksums against any existing files in the specified directory to avoid downloading the same file twice.  Only new/changed files are downloaded.
For efficient execution, the script will keep information about the last execution in a file named .pull-log-level-data-bookmark in the same folder as the config file to log the last update information, so it can pick from where it left off, in the next execution.

You can run this with cron but not more often than hourly, since the files are generated hourly.
Note: Per AppNexus logs are kept on the server for up to 10 days.
