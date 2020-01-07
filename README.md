# Dynein

Dynein is Airbnb's Open-source Distributed Delayed Job Queueing System.
Learn more about Dynein in this blog post: [Dynein: Building an Open-source Distributed Delayed Job Queueing System][dynein-blogpost].

## Example

Dynein's components are structured as [`Managed`][dw-managed] objects in Dropwizard.
We have prepared an example application to demonstrate how to integrate Dynein in your service in `./example`.

To run this example, follow the following steps:

1. Setup [`localstack`][localstack].
   The following steps will be using the default ports of `localstack`.
1. Run this command to create the necessary SQS queues:

   ```shell script
   aws sqs create-queue --endpoint-url http://localhost:4576 --queue-name example --region us-east-1
   aws sqs create-queue --endpoint-url http://localhost:4576 --queue-name inbound --region us-east-1
   ```

1. Run this command to create the DynamoDB table:

   ```shell script
   aws dynamodb create-table --table-name dynein_schedules \
     --attribute-definitions AttributeName=shard_id,AttributeType=S AttributeName=date_token,AttributeType=S \
     --key-schema AttributeName=shard_id,KeyType=HASH AttributeName=date_token,KeyType=RANGE \
     --billing-mode PAY_PER_REQUEST --endpoint-url http://localhost:4569 --region us-east-1
   ```

1. `cd example && gradle run --args='server example.yml'`

1. In another terminal, use `curl` to schedule jobs.
   The following endpoints are available, which maps to scheduled, immediate, and sqs-delayed jobs:

   1. `curl 'localhost:28080/schedule/scheduled'`
   2. `curl 'localhost:28080/schedule/immediate'`
   3. `curl 'localhost:28080/schedule/sqs-delayed'`

   Each endpoint supports optional parameters `delaySeconds` and `name`.
   For example: `curl 'localhost:28080/schedule/scheduled?name=testing-name&delaySeconds=15'`

1. In the original terminal where the example is started, at the scheduled time, the following log lines will be printed:

   ```text
   INFO  [2019-12-10 01:59:04,785] com.airbnb.dynein.example.ExampleJob: Running job with name: "testing-name"
   INFO  [2019-12-10 01:59:04,800] com.airbnb.dynein.example.worker.ManagedWorker: Successfully consumed job from queue
   ```

[dynein-blogpost]: https://medium.com/airbnb-engineering/dynein-building-a-distributed-delayed-job-queueing-system-93ab10f05f99
[dw-managed]: https://www.dropwizard.io/en/stable/manual/core.html#managed-objects
[localstack]: https://github.com/localstack/localstack

## Configuration

Here's an example from `./example/example.yml`.
We will use inline comments to explain what each option does.

```yaml
scheduler:
  sqs:
    inboundQueueName: inbound # inboud queue name
    endpoint: "http://localhost:4576" # SQS endpoint. normally this line can be omitted, but it's useful for using localstack
  dynamoDb:
    schedulesTableName: dynein_schedules # dynamodb table name
    queryLimit: 10 # max number of results per query
    endpoint: "http://localhost:4569" # DynamoDB endpoint. normally this line can be omitted, but it's useful for localstack
  workers:
    numberOfWorkers: 1 # number of workers to start in this scheduler
    recoverStuckJobsLookAheadMs: 60000 # look ahead time for recovering stuck jobs
    threadPoolSize: 2 # must be at least 1 more than the number of workers
    partitionPolicy: STATIC # can also be `K8S` to use dynamic partitioning
    staticPartitionList: [0] # only needed for static partition policy
  heartbeat:
    monitorInterval: 60000 # the interval for heartbeat monitoring
    stallTolerance: 300000 # the minimum time for a worker to be considered stopped
  maxPartitions: 1 # max number of partitions. can only be increased without reassigning partitions for jobs already scheduled
```
