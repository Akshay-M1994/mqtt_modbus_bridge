import serial.rs485
import minimalmodbus
import json
import sys
from os.path import exists
from enum import Enum
import logging


class mqtt2Modbus_ErrorStatus(Enum):
    INVALID_CMD           = 0
    INVALID_DEV_ADDRESS   = 1
    MISSING_PARAMETER     = 2
    INVALID_PARAMETER     = 3
    MODBUS_IO_FAILED      = 4
    DEVICE_PROFILE_ABSENT = 5
    GENERAL_ERROR         = 6
    OK                    = 7
    

class modbusMsgInfo:
    def __init__(self,regAdd: int,regCount: int, modFunc: int, devAdd: int,opType: int, regDataArr,valid : bool):
        self.devAdd = devAdd
        self.regAdd = regAdd
        self.regCount = regCount
        self.modfunc = modFunc
        self.regData = regDataArr 
        self.valid   = valid
        self.transactionType = opType   

mqttResponse = {"cmdName"  :"","uuid"     :"", "devAdd"   :0, "regData"  :[],"result"   :0} 
modbusMsgParams = modbusMsgInfo(0,0,0,0,0,0,False) 

def modbusMsgTx(modbusHandle : minimalmodbus.Instrument,mqttMsg : dict) -> dict:
     
     #declare mqtt response as global
     global mqttResponse

     #Determine if modbus message being passed as argument is valid
     if modbusMsgParams.valid != False:

        #Set device address as per current message parameters
        modbusHandle.address = modbusMsgParams.devAdd
        #Attempt to read/write to modbus depending on command
        try:
            if modbusMsgParams.transactionType == 0:
                mqttResponse["regData"]  = modbusHandle.read_registers(modbusMsgParams.regAdd, modbusMsgParams.regCount)
            else:
                mqttResponse["regData"]  = modbusHandle.write_registers(modbusMsgParams.regAdd, modbusMsgParams.regData)
        except:
            mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MODBUS_IO_FAILED.value
 
     return mqttResponse


def mqttMsg2ModbusMsg(mqttMsg: dict) -> mqtt2Modbus_ErrorStatus | modbusMsgInfo:

    #Declare mqtt response as global
    global mqttResponse
    mqttResponse = {"cmdName":"","uuid":"", "devAdd":0,"regData":[],"result":0} 

    global modbusMsgParams
    modbusMsgParams = modbusMsgInfo(0,0,0,0,0,0,False) 

    #Ensure that message has all the required keys
    if not "cmdName" in mqttMsg:
        print("\"cmdName\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER
    
    if not "devAdd" in mqttMsg:
        print("\"devAdd\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER
        

    if not "devProfile" in mqttMsg:
        print("\"devProfile\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER
    
    if not "regData" in mqttMsg:
        print("\"regData\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER
    if not "uuid" in mqttMsg:
        print("\"uuid\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER

    mqttResponse["devAdd"] = mqttMsg["devAdd"]
    mqttResponse["cmdName"] = mqttMsg["cmdName"]
    mqttResponse["uuid"] = mqttMsg["uuid"]
    
    deviceProfileJsonPath  = 'modbus_instrument_profiles/'+mqttMsg["devProfile"]+'_InstrumentProfile.json'

    #Now we determine if such a profile exists on the database
    if not exists(deviceProfileJsonPath):
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.DEVICE_PROFILE_ABSENT.value
        return mqtt2Modbus_ErrorStatus.DEVICE_PROFILE_ABSENT
    
    try:
        #Open device profile json object
        deviceProfileJsonFile = open(deviceProfileJsonPath)

        #Create device profile dictionary
        deviceProfileDictionary = json.load(deviceProfileJsonFile)

        #Filter through command list in device profile to determine if command in mqtt msg exists
        for cmd in deviceProfileDictionary["cmdList"]:
            
            if(cmd["cmdName"] == mqttMsg["cmdName"]):
                
                #If command can be found in devices profile then extract the function and register address parameters
                modbusMsgParams.modfunc = cmd["modfunc"]
                modbusMsgParams.regAdd  = cmd["regAdd"]
                modbusMsgParams.regCount = cmd["regCount"]
                modbusMsgParams.devAdd = mqttMsg["devAdd"]
                modbusMsgParams.valid = True

                if cmd["cmdType"] == "R":
                    modbusMsgParams.transactionType = 0
                else:
                    modbusMsgParams.transactionType = 1

                #Determine if this command requires additional data/parameters
                if cmd["regData"] != "N/A":
                    modbusMsgParams.regData = mqttMsg["regData"] 
                break

        #Close the file
        deviceProfileJsonFile.close()

        if modbusMsgParams.valid == True:
            mqttResponse["result"] = mqtt2Modbus_ErrorStatus.OK.value
            return modbusMsgParams
        else: 
            mqttResponse["result"] = mqtt2Modbus_ErrorStatus.INVALID_CMD.value
            return mqtt2Modbus_ErrorStatus.INVALID_CMD

    except:
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.GENERAL_ERROR.value
        return mqtt2Modbus_ErrorStatus.GENERAL_ERROR
    



