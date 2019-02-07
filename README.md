
# Event sourcing example with Kinesis & Spring

A simple event sourcing example using Spring Boot and AWS Kinesis. A
Spring Boot starter abstracts consuming a Kinesis Stream and updating
a corresponding SQL data model using JDBC & Spring Data for each Kinesis
Record received. This starter could be shared between multiple micro
services in a larger architecture.

# Dependencies

  - Docker
  - MySQL H2 functions for upsert queries in tests, https://github.com/linux-china/h2-functions-4-mysql
     - Install this to local maven repository first
  - Java 8

### Run locally

The code uses Localstack for mocking Kinesis and Dynamo DB APIs. Start up
Localstack, https://github.com/localstack/localstack, in Docker using:
`docker-compose up`.

Add following environment variables when running the Spring app with
Localstack:
```
AWS_CBOR_DISABLE=1;
AWS_ACCESS_KEY=foobar;
AWS_SECRET_ACCESS_KEY=foobar
```

### Test

After starting up Localstack and the Spring Boot app you can setup a
Kinesis stream by calling `./stream-create.sh` and publish an event
 using `./stream-send-event.sh`.


### Notes

- The Kinesis consumer uses KCL v1 interfaces as Localstack does not
support v2, see: https://github.com/mhart/kinesalite/issues/75.
