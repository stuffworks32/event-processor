#!/bin/bash
set -euo pipefail

awslocal sqs create-queue --queue-name events-dlq --attributes MessageRetentionPeriod=1209600 #(14 days in seconds)

dlq_url=$(awslocal sqs get-queue-url --queue-name events-dlq --query QueueUrl --output text)
dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)

cat > /tmp/events-queue-attributes.json << JSON
{
  "VisibilityTimeout": "30",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${dlq_arn}\",\"maxReceiveCount\":\"5\"}"
}
JSON

awslocal sqs create-queue --queue-name events-queue --attributes file:///tmp/events-queue-attributes.json