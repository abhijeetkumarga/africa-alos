# africa-alos

# Docker process
To run the Docker process, you need Docker and Docker Compose installed.

When installed, you can build the Docker image with `docker-compose build` and then run a process 
with `docker-compose up`. This will use the SQS queue specified in the `docker-compose.yaml` file by default.

You can load jobs into the SQS queue with the script `add_to_queue.py`, and you can change some variables
in that script in order to pick a year and how many files to add. You can run the `add_to_queue.py` script 
using Docker with `docker-compose run /opt/alos/add_to_queue.py`.
