#!/usr/bin/python
'''
converts text topic:value<CR> pairs into MQTT messages set to a MQTT broker
stops on <EOF>

does not have the conversion of human readable text to topics in this go around
'''

#
### IMPORTS
#
import paho.mqtt.client as mqtt
import socket
from parse import *
from string import split, strip, find
import sys
import getopt
import re
import time


#
### CONSTANTS
#


#
### GLOBALS
#
DEBUG = False


#
### FUNCTIONS
#
def printCommandSummary():
  print "usage: python text2mqtt.py [--broker=<url>] [-b <url] [--qos=<QOS>] [-q <QOS>] [-k] [--retain] [-h][--help] [--debug]"
  print "     [--topic=<topic>] -t <topic> [-h] [--help] [-i] [-I] [-a] [-v]"
  print "  <url> is url of MQTT broker, e.g., localhost, example.local, example.com:4343"
  print "    default is to print to standard output instead of sending MQTT messages"
  print "  <QOS> is one of 0, 1, 2; defaulting to 0"
  print "  <topic> is the super topic to use in MQTT messages, defaulting to none"
  print "  -k is to retain MQTT state, same as '--retain'; defaulting to no retention"
  print "  --debug is to print debug messages, defaulting to no printing"
  print "  -h or --help is to print this command summary"


# set up the mqtt call backs

def on_connect(mqttc, obj, flags, rc):
    pass

def on_message(mqttc, obj, msg):
    pass

def on_publish(mqttc, obj, mid):
    pass

def on_subscribe(mqttc, obj, mid, granted_qos):
    pass

def on_log(mqttc, obj, level, string):
    pass


#
### MAIN
#
def main(argv):
   global DEBUG

   # setup defaults
   mqttBrokerHost = ''
   mqttTopic = (socket.gethostname()) + "/"
   QOS = 0
   retention = False

   #process command line
   try:
      opts, args = getopt.getopt(argv,"b:q:kt:h",["broker=", "qos=", "retain", "debug", "help", "topic="])
   except getopt.GetoptError:
      printCommandSummary()
      sys.exit(2)
   for opt, arg in opts:
      if opt in ("--broker", "-b"):
        mqttBrokerHost = arg
      elif opt in ("-q", "--qos"):
        QOS = int(arg)
      elif opt in ("-k", "--retain"):
        retention = True
      elif opt in ("-t", "--topic"):
        mqttTopic = arg + "/"
      elif opt in ("--debug"):
        DEBUG = True
      elif opt in ("--help", "-h"):
        printCommandSummary()
        sys.exit(2)

   if DEBUG:
     print "Arguments:", argv
     print
     print 'MQTT broker URL is:', mqttBrokerHost
     print 'MQTT topic is', mqttTopic
     print 'MQTT QOS is', QOS
     print 'MQTT Retention is', retention

   # if requested, send a MQTT message with requested subtopics
   if mqttBrokerHost is not "":
     # If you want to use a specific client id, use
     # mqttc = mqtt.Client("client-id")
     # but note that the client id must be unique on the broker. Leaving the client
     # id parameter empty will generate a random id for you.
     mqttc = mqtt.Client()

     # set up call backs in the object
     mqttc.on_message = on_message
     mqttc.on_connect = on_connect
     mqttc.on_publish = on_publish
     mqttc.on_subscribe = on_subscribe
     # Uncomment to enable debug messages
     #mqttc.on_log = on_log

     #mqttc.connect_async( mqttBrokerHost, 1883, 60)
     mqttc.connect( mqttBrokerHost, 1883, 60)
     mqttc.loop_start()

     # set up parser
     # handle leading or trailing whitespace??
     # allow alternate separator?
     validSubtopic = re.compile ("^[a-zA-Z][a-zA-Z0-9\/._]*$")
     for line in sys.stdin:
       errors = True
       if DEBUG:
         print line
       # attempt to parse line into subtopic:value
       colonAt = find(line, ': ')
       if colonAt >= 0:
         subtopic = strip( line [:colonAt])
         payload = strip( line [colonAt+2:])
         if DEBUG:
           print subtopic + " ::", payload
         subtopic = validSubtopic.match( subtopic)
         if subtopic is not None and len( payload) > 0:
           subtopic = subtopic.group(0)
           errors = False
          
       if errors:
         if DEBUG:
           exit (1)
       else:
         if DEBUG:
           print "MQTT Message: " + mqttTopic + subtopic + ":" + str (payload) + " qos="+ str(QOS) + " retain=" + str(retention)
         mqttc.publish(mqttTopic + subtopic, payload=payload, qos=QOS, retain=retention)
         #time.sleep (.1)
     time.sleep (4)
     mqttc.disconnect( ) # assuming qos = 0... really should wait for any ack...
   exit (0)



if __name__ == "__main__":
  main(sys.argv[1:])
