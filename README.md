# Amazon Rekognition S3 Batch Tool

Quickly and easily process your images that are in your S3 bucket through **[Amazon Rekognition]**.

This example provides two programs:
 - a scanner that finds all the images you'd like to process, and adds them to [SQS] (Simple Queue
   Service)
 - a processor to run those images through Rekognition for label analysis, and saves those results
   to [DynamoDB] .

You can run these commands locally and drive a high throughput of image analysis, as this program
is executing the processing and network transfers within AWS.

The examples can be run from the command line or by incorporating into your existing codebase. You
can modify the code to run different image analysis, or to add different downstream processing (like
writing to RDS, Elastic Search, or your on-premise DBs).

You should feel free to extend the code in these examples to meet your needs.

The scanner can be run in parallel to the processor; the processor monitors SQS and starts executing
as soon as it sees jobs hit the queue.

## Usage

### To scan:

```ignore
$ java -jar s3batch.jar -scan

usage: scanner -bucket <arg> [-filter <arg>] [-help] [-max <arg>] [-prefix <arg>] [-profile <arg>] -queue <arg>

 -bucket <arg>    S3 Bucket Name
 -filter <arg>    Key Filter Regex. Default '\.(jpg|jpeg|png)$'
 -help            Get this help.
 -max <arg>       Max number of images to add to queue.
 -prefix <arg>    S3 Bucket Prefix
 -profile <arg>   AWS Credential Profile Name (in ~/.aws/credentials).
                  Default 'default'
 -queue <arg>     SQS Queue to populate. Will create if it doesn't exit.
 ```

### To process:

```ignore
$ java -jar s3batch.jar -process

usage: scanner [-cloudsearch <arg>] [-concurrency <arg>]
 [-confidence <arg>] [-disablecerts] [-dynamo <arg>] [-endpoint <arg>] [-help]
 [-max <arg>] [-profile <arg>] -queue <arg>

 -cloudsearch <arg>   Cloud Search index to optionally insert into.
 -concurrency <arg>   Number of concurrent Rekognition jobs. Default 20
 -confidence <arg>    Minimum confidence in labels. Default 70.
 -disablecerts        Disable certificate checking.
 -dynamo <arg>        Dynamo DB table to optionally insert into.
 -endpoint <arg>      Override the Rekognition endpoint.
 -help                Get this help.
 -max <arg>           Max number of images to index.
 -profile <arg>       AWS Credential Profile Name (in ~/.aws/credentials).
                      Default 'default'
 -queue <arg>         SQS Queue to fetch tasks from.
 -tagprefix <arg>     S3 label tag prefix. Default 'rek.'
 -tagS3               Write detected labels back to S3 as Object Tags.
```

[Amazon Rekognition]: https://aws.amazon.com/rekognition/
[SQS]: https://aws.amazon.com/sqs/
[DynamoDB]: https://aws.amazon.com/dynamodb/
