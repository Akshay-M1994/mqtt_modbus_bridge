import serial.rs485
import paho.mqtt.client as mqtt
import minimalmodbus
import pyRTOS
import pyRTOS.message
import json
#import logging
from debug_nid import debug
import sys
import mqtt2modbus.mqtt2modbus
from   dotenv import load_dotenv
import os


#Serial port config details
RS485_PORT = "/dev/ttyTHS1"
RS485_BAUDRATE = 9600
RS485_SERIAL_TIMEOUT = 1

#MQTT broker details
MQTT_BROKER_IP_ADDRESS = 'localhost'
MQTT_BROKER_PORT = 1884

#Modbus Mqtt Topics
MODBUS_CMD_TOPIC =  "modbus_mqtt/cmd_request"
MODBUS_RESP_TOPIC = "modbus_mqtt/cmd_response"


#Message Queue Size
MQTT_MSG_QUEUE_SIZE = 256

def on_disconnect(self, client, userdata, reason_code, properties):
    #Log error code
    debug.logging.debug('Connection result code ' + str(reason_code))  
    debug.logging.debug('MQTT Client Disconnected.Attempting Reconnection...')

def on_publish(client, userdata, mid, reason_code, properties):
    # reason_code and properties will only be present in MQTTv5. It's always unset in MQTTv3
    debug.logging.debug(f"on_publish callback triggered with reason code :{reason_code}")

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    if reason_code_list[-1].is_failure:
        debug.logging.debug(f"Broker rejected you subscription: {reason_code_list[-1]}")
    else:
        debug.logging.debug(f"Broker granted the following QoS: {reason_code_list[-1].value}")

def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
    # Be careful, the reason_code_list is only present in MQTTv5.
    if len(reason_code_list) == 0 or not reason_code_list[-1].is_failure:
        debug.logging.debug("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
    else:
        debug.logging.debug(f"Broker replied with failure: {reason_code_list[-1]}")

    client.disconnect()    

def on_connect(client, userdata, flags, reason_code, properties):
    debug.logging.debug(f"Connected with result code : {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MODBUS_CMD_TOPIC)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    
    #Log receipt of msg
    debug.logging.debug(f"Msg received on topic:"+msg.topic+" Payload:" +str(msg.payload))
    
    try:
        #Convert to JSON object
        mqttJsonMsg = json.loads(msg.payload)

    except TypeError as typeErr:
        debug.logging.error(f"{typeErr}")

    except ValueError as valueErr:
        debug.logging.error(f"{valueErr}")

    #Add message to queue
    if mqttMsgQueue.nb_send(mqttJsonMsg) == True:
        debug.logging.debug(f"Msg added to queue!")
    else :
        debug.logging.debug(f"Failed to add msg to queue!")


#Task responsible for pulling modbus requests of the queue and placing them on the bus
def modbus_manager_task(self):
    while True:
        
        #Pull a request of the mqtt msg queue
        mqttMsg = mqttMsgQueue.nb_recv()

        #If we have a request then attempt to place it on the bus
        if mqttMsg != None:

            #Convert mqtt request to a modbus RTU message
            modbusMsgResult = mqtt2modbus.mqttMsg2ModbusMsg(mqttMsg)

            #Transmit modbus RTU message and wait for response
            mqttModbusResponse = mqtt2modbus.modbusMsgTx(modbus_port,mqttMsg)
            
            #Log response
            debug.logging.debug(f"Attempting to publish:"+json.dumps(mqttModbusResponse))
            
            #publish response
            mqttc.publish(MODBUS_RESP_TOPIC,json.dumps(mqttModbusResponse))
        
        #Yield for 1 secondMODBUS_RESP_TOPIC
        yield [pyRTOS.timeout(0.1)]

#Task responsible for managing mqtt connection
def mqtt_manager_task(self):
    while True:

        #Log every time task runs => Allows us to determine if task runs with sufficient frequency
        #logging.debug("Mqtt Manager Task Running\r\n")
        yield [pyRTOS.timeout(3)]

#Load environment variable file
load_dotenv()

#Log startup info
debug.logging.debug("[Starting Modbus Mqtt Bridge:%s]",os.getenv('MQTT_MODBUS_BRIDGE_VERSION'))

#Setting up MQTT comms
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.on_disconnect = on_disconnect
mqttc.on_subscribe = on_subscribe
mqttc.on_unsubscribe = on_unsubscribe
mqttc.on_publish = on_publish
mqttc.connect(MQTT_BROKER_IP_ADDRESS, MQTT_BROKER_PORT, 60)

#Setting up RS485 Port and Modbus comms
#Create Serial Object and configure serial port for RS485 comms
try:
    rs485_serial = serial.rs485.RS485(RS485_PORT,RS485_BAUDRATE)
    rs485_serial.rs485_mode = serial.rs485.RS485Settings(False,True)
except IOError as e:
    #This is a critical error -> Terminate script
    debug.logging.error(f"{e}")
    sys.exit()

#Create and configure a "modbus instrument"
modbus_port = minimalmodbus.Instrument(rs485_serial, 0, 'rtu', False, False)
modbus_port.serial.baudrate = RS485_BAUDRATE
modbus_port.serial.timeout = RS485_SERIAL_TIMEOUT

#Setting up queue to hold incoming mqtt messages
mqttMsgQueue = pyRTOS.message.MessageQueue(MQTT_MSG_QUEUE_SIZE)

#Setting up tasks
pyRTOS.add_task(pyRTOS.Task(modbus_manager_task, name="modbus_manager_task"))
pyRTOS.add_task(pyRTOS.Task(mqtt_manager_task, name="mqtt_manager_task"))

#Start paho mqtt background thread
mqttc.loop_start()

#Start RTOS
pyRTOS.start()

