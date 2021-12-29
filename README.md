# SIF-ingest-broker

### Purpose
* Receive raw messages from data sources (i.e. users, devices, etc)
* Authenticate these messages
* Map users and their app names to globally unique app ids
* Transform the incoming data into Intermediate Representation (IR), our standardized data transmission format that the [stream broker](https://github.com/tic/sif-stream-broker) understands
* Foward the app id and IR-formatted data to the stream broker

## How to run
In the SIF ecosystem, the main file in this repo -- `app.js` -- is running in an AWS EC2 instance. This instance hosts an MQTT broker that receives messages from programs using the SIF platform. There are several prerequisites in order to run this script *as-is*. For each requirement, listed below, the version used by our EC2 instance is included, if applicable:

1. NodeJS v14.18.2
2. npm v6.14.15
3. mosquitto v3.1.1
4. A proper `.env` file, details below
5. A CA certificate for your PostgreSQL database (i.e. TimescaleDB)

### Environment File
The `.env` file should be placed in the root directory of the project, i.e. at the same level as `app.js`. The file should define the following set of values:

Field | Value
----- | -----
ENV | enum ["DEVELOPMENT", "PRODUCTION"]
PUBLIC_BROKER | `mqtt://host` where `host` is the address where the script can access the MQTT broker that is receiving raw input data
INGEST_STREAM | `mqtt://host` where `host` is the address where the script can access the MQTT broker that is receiving pre-processed, IR formatted, data
INGEST_TOPIC | Name of the topic that pre-processed data should be published to on the stream broker
REGION | AWS region where the Cognito user pool is configured
USERPOOLID | Pool id of the Cognito user pool provided credentials belong to
TS_USER | PostgreSQL database username
TS_PASSWD | PostgreSQL database password
TS_HOST | PostgreSQL database hostname
TS_PORT | PostgreSQL database port
TS_DATABASE | PostgreSQL database name
PGSSLROOTCERT | `/path/to/ca.pem` (path to CA certificate, used for opening an SSL connection with the database)

## Sending data to the ingest-broker
Once the MQTT broker and the SIF-ingest-broker script are both up and running, they are ready to receive data from users. As a sanity check, this script should print the following on startup (the order of the two "Connected" messages is not important):

    Connected to the ingest stream
    Connected to the public broker
    Subscribed to all data routes on the public broker

Messages are expected to adhere to a particular format. Improperly formatted messages are ignored. All messages should be stringified JSONs containing three fields:
1. token -- a JSON web token (JWT), issued by AWS Cognito
2. app_name -- name of the application this data should be filed under
3. data -- a blob of data, formatted according to a schema based on the topic the message is being published to

An example of a message-sending script is provided in the form of a [virtual sensor](https://github.com/tic/smart-inf-virtual-sensor). Refer to this script for a fully functional demonstration of using AWS Cognito to get a JWT, connecting to the MQTT broker, generating properly formatted data, and publishing it to the broker.
