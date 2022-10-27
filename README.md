# kafka-app

## What is it?
This is a simple application depicting event collaboration with Kafka using the lifecycle of an order.

## Why this?
Built this application to gain better insight into the workings of Kafka

## How to use this?
1. Run `docker-compose up` from the root of the application to get application running.
2. Open another terminal and run `docker exec -it python-app /bin/bash` to enter into bash shell of the python-app container.
3. Run the command `python3 <service_name>/handlers.py` eg. To run the messaging service run `python3 messaging_service/handlers.py`
4. Repeat steps 2 and 3 for all services
5. Open another terminal and run `docker exec -it python-app /bin/bash` to enter into bash shell of the python-app container.
6. Run `python3 main.py`
7. A log file named `app.log` should be created. Check to see your logs.