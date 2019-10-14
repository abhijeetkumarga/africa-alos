#!/usr/bin/env python3

import os
import sys
import logging
import boto3

from alos_process import run_one

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


SQS_QUEUE = os.environ.get("SQS_QUEUE", 'not-implemented')
S3_DESTINATION = os.environ.get("S3_DESTINATION", 'not-implemented')

VISIBILITYTIMEOUT = os.environ.get('VISIBILITYTIMEOUT', 1000)


sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE)


def count_messages():
    logging.info("There are {} messages on the queue.".format(queue.attributes["ApproximateNumberOfMessages"]))
    return int(queue.attributes["ApproximateNumberOfMessages"])


if __name__ == "__main__":
    n_messages = count_messages()
    while n_messages > 0:
        messages = queue.receive_messages(
            VisibilityTimeout=VISIBILITYTIMEOUT,
            MaxNumberOfMessages=1
        )

        if len(messages) > 0:
            message = messages[0]
            TILE_STRING = message.body
        else:
            logging.info("No messages found...")
            # Bailing on an empty queue!
            sys.exit(0)
        logging.info("Found tile to process: {}".format(TILE_STRING))
        success = run_one(TILE_STRING, 'data/download', 'data/out', S3_DESTINATION)
        if success:
            logging.info("Job completed")
            message.delete()
        else:
            logging.warning("Job failed processing {}".format(TILE_STRING))

        n_messages = count_messages()
