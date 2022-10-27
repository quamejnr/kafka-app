# Kafka App

## What is it?

This is a simple application depicting event collaboration with Kafka using the lifecycle of an order.

## Why this?

Built this application to gain better insight into the workings of Kafka

## How to use this?

1. Run `docker-compose up` from the root of the application to get application running.
2. Open another terminal and run `docker exec -it python-app /bin/bash` to enter into bash shell of the python-app container.
3. Run `python3 main.py`
4. A log file named `app.log` should be created. Check to see your logs.
