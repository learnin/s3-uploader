# AWS S3 Uploader

Uploads the directories and files under the specified directory to AWS S3 with gzip compression for each file.
The uploaded directories and files will be deleted.

# Usage
```
s3-uploader
  -AwsAccessKeyId string
    	AWS access key id
  -AwsRegion string
    	AWS region
  -AwsSecretAccessKey string
    	AWS secret access key
  -ProxyUrl string
    	proxy url. eg. http://myproxy:myport/
  -S3Bucket string
    	S3 bucket name
  -S3DirPath string
    	S3 bucket directory path
  -S3PutTimeoutSeconds int
    	timeout seconds during uploading per file
  -Settings string
    	path to settings.json file. default: [this executable file dir]/settings.json
  -TargetDirNameRegExp string
    	regexp for target directory name
  -TargetFilesDirPath string
    	interface files directory path
  -Verbose
    	verbose mode. default: false
```

or

Create a settings.json file and place it in the same directory as the s3-uploader file, or in any path and specify the file path in the `-Settings` parameter.
The format of the settings.json file is as follows.

```
{
  "AwsAccessKeyId": "XXXXX",
  "AwsSecretAccessKey": "XXXXX",
  "AwsRegion": "ap-northeast-1",
  "S3Bucket": "example",
  "S3DirPath": "example/",
  "TargetFilesDirPath": "/tmp/example",
  "TargetDirNameRegExp": "^target_[0-9]$",
  "ProxyUrl": "http://localhost:8080/",
  "S3PutTimeoutSeconds": 300  
}
```

If a value is specified in both the parameter and the settings.json file, the parameter value will take precedence.

S3Bucket, S3DirPath and TargetFilesDirPath are required. They must be specified either as parameters or in the settings.json file.
