# Truck Data Wrangler

This repository aims to resolve the code challenge proposed by the DTB Tech/Data Tech Hub.

## Summary

 - [Getting started](#getting-started)
 - [The technology stack](#the-technology-stack)
 - [The data classification](#the-data-classification)

## Getting started

First of all, you will need to create your configuration file by copying the `app.cfg.example` as `app.cfg` 
(a normal `cp app.cfg.example app.cfg` would do it)

Fill all the values with your preferred configurations. Special attention to the TimescaleDB section, if you are running
this project with Docker, then it will connect from the Spark container to the TimescaleDB container. 
In this case, the host part of it have to be configured accordingly with your Docker local IP (if running on Docker). 
To discover the Docker local ip of your TimescaleDB container you should use: 
`docker inspect --format '{{ .NetworkSettings.IPAddress }}' <container id or name>`

After properly configuration of the `app.cfg` file, you have to **create the table of the TimescaleDB by running**: `python recreate_tables.py`.
Again, if using Docker this command shall be runned on the Spark container.

Finally we're good to go. **We have two main entry points for this project:**

 - `python app_batch.py` will process all the _raw_data_ and give us:
    - The mean acceleration per event type and label
    - The maximum jerk timestamp on human readable date time stamp
 - `python app_stream.py` will watch the folder _raw_data_ or an S3 bucket folder or even an HDFS folder, every 10 seconds _(or less or more - configurable)_ will search for new files, stream it to Spark and do an upsert to our TimescaleDB:
    - More specifically on a table called `jerked_truck_events`. In that table we also have 4 new binary flags: `is_accelerating`, `is_breaking`, `is_turning_left`, `is_turning_right`

## The technology stack

We could do the same with other infinity of tools. So, below I will describe why we choosed these techs:

 - TimescaleDB
 - Apache Spark (with Structured Streaming)

Assuming we have several trucks to monitor on, it would be very practical, secure and easy to just upload a csv with the sensors values of each truck inside a folder (on S3 or HDFS), and automatically this file would be parsed by our Structured Streaming ETL and get inserted right into the TimescaleDB.
This way we have an scalable storage solution integrated into an scalable pipeline with Spark. Giving us the possibility to even run this on Amazon Web Service EMR.

## The data classification

The ideal data classification here, would be using a filter to wipe some noisy high frequencies that came with the sensors values. Altought I decided to focus more on the scalable part of this project than on the algorithmic part itself. The algorithm used to classify this data can be improved throughout the time.

As little information that was given about the sensors, we assumed several conditions that has to be illustrated here (_please, don't mind the airplane on the picture, its for illustration purposes_):

| Accelerometer sensor schema | Gyroscope sensor schema |
| --------------------------- | ----------------------- |
| ![Accelerometer sensor schema](https://raw.githubusercontent.com/gabfr/truck-data-wrangler/master/images/accelerometer.jpg) | ![Gyroscope sensor schema](https://raw.githubusercontent.com/gabfr/truck-data-wrangler/master/images/gyroscope.png) |

To classify if the truck was accelerating and breaking we simply check if the Jerk of the X value of the accelerometer is positive or negative. The Jerk is the variation rate of the accelerometer value. If it is positive, then the truck is accelerating. If it is negative, then the truck is breaking.

And lastly, but not leastly, to check if the truck was turning right or left we assumed the Yaw angle was accounted from left to right.
With that in mind, to classify a turn we check if the gyroscope yaw value was outside the standard deviation mod window. Example: if the std.d. was 0.2, then if the yaw value is between -0.2 to 0.2 we assume the truck is steady and straight forward. But if it was below -0.2, then we assume the truck was turning left. And finally if it was higher than 0.2, then we assume the truck was  turning right.

