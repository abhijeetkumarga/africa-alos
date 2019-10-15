#!/usr/bin/env python3

import boto3
import os
import logging

from filenames import get_mesh


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


LIMIT = int(os.environ.get('LIMIT', 5))
SQS_QUEUE = os.environ.get("SQS_QUEUE", 'alex-alive-queue')

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE)


def get_items(LIMIT=9999, START=0, YEARS=['2017']):
    count = 0
    logging.info("Adding {} items from to the queue: {}".format(LIMIT, SQS_QUEUE))

    for year in YEARS:
        logging.info("Working on year: {}".format(year))
        for item in get_mesh()[START:]:
            count += 1
            if count > LIMIT:
                break

            if count % 100 == 0:
                logging.info("Pushed {} items...".format(count))

            # Create a big list of items we're processing.
            one_tile_string = year + '/' + item
            queue.send_message(MessageBody=one_tile_string)

    logging.info("Added {} items".format(count - 1))


if __name__ == "__main__":
    logging.info("Starting to add to queue")
    get_items(LIMIT=LIMIT, START=100, YEARS=['2007'])
