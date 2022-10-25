# kafka-app

## What is it?
This is a simple application depicting event collaboration with Kafka using the lifecycle of an order.

## Why this?
Built this application to gain better insight into the workings of Kafka

## How to use this?
1. Run `docker-compose up` from the root of the application to get Kafka running.
2. Open multiple terminals and run all but the `get_consumer()` functions in the handlers of the various services. 
(Yeah I know it sounds stressful, I'm working on implementing an easier to way to run it, let me know if you have any ideas though)
3. Run `python3 main.py` from the root folder