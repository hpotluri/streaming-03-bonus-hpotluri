"""
Harinya Potluri 05/20/2024

Message sender / emitter 

Description:
This script sends one message on a named queue.
It will execute and finish. 
You can change the message and run it again in the same terminal.

Remember:
- Use the up arrow to recall the last command executed in the terminal.
"""

import csv
import socket
import time
import logging


# Import from Standard Library
import sys

# Import External packages used
import pika

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)


HOST = "localhost"
PORT = 15672
ADDRESS_TUPLE = (HOST, PORT)
INPUT_FILE_NAME = "SAT__College_Board__2010_School_Level_Results_20240506.csv"


# ---------------------------------------------------------------------------
# Define program functions (bits of reusable code)
# ---------------------------------------------------------------------------


def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue

    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()

        # use the channel to declare a queue
        ch.queue_declare(queue=queue_name)

        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)

        # log a message for the user
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def prepare_message_from_row(row):
    """Prepare a binary message from a given row."""
    DBN,SchoolName,NumberofTestTakers,CriticalReadingMean,MathematicsMean,WritingMean = row

    # use an fstring to create a message from our data
    # notice the f before the opening quote for our string?
    fstring_message = f"[{DBN}, {SchoolName}, {NumberofTestTakers}, {CriticalReadingMean}, {MathematicsMean}, {WritingMean}]"

    # prepare a binary (1s and 0s) message to stream
    MESSAGE = fstring_message.encode()
    logging.debug(f"Prepared message: {fstring_message}")
    return MESSAGE


def stream_row(input_file_name, address_tuple):
    """Read from input file and stream data."""
    logging.info(f"Starting to stream data from {input_file_name} to {address_tuple}.")

    # Create a file object for input (r = read access)
    with open(input_file_name, "r") as input_file:
            logging.info(f"Opened for reading: {input_file_name}.")
            logging.info(f"Opened for writing: out9.txt.")
            # Create a CSV reader object
            reader = csv.reader(input_file, delimiter=",")
            
            header = next(reader)  # Skip header row
            logging.info(f"Skipped header row: {header}")

            # use socket enumerated types to configure our socket object
            # Set our address family to (IPV4) for 'internet'
            # Set our socket type to UDP (datagram)

            # Call the socket constructor, socket.socket()
            # A constructor is a special method with the same name as the class
            # Use the constructor to make a socket object
            # and assign it to a variable named `sock_object`            
            for row in reader:
                time.sleep(2)
                MESSAGE = prepare_message_from_row(row)
                send_message(HOST,"hello", MESSAGE)


# ---------------------------------------------------------------------------
# If this is the script we are running, then call some functions and execute code!
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        logging.info("===============================================")
        logging.info("Starting fake streaming process.")
        stream_row(INPUT_FILE_NAME, ADDRESS_TUPLE)
        logging.info("Streaming complete!")
        logging.info("===============================================")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
