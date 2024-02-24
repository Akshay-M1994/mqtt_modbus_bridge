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
    RESULT_UNKNOWN        = 7
    OK                    = 8

class modbus_function_codes(Enum):
    READ_COILS                  = 1
    READ_CONTACTS               = 2
    READ_HOLDING_REGISTERS      = 3
    READ_INPUT_REGISTERS        = 4
    WRITE_SINGLE_COIL           = 5
    WRITE_SINGLE_REGISTER       = 6
    WRITE_MULTIPLE_COILS        = 15
    WRITE_MULTIPLE_REGISTERS    = 16

class modbusMsg:
    def __init__(self,regAdd: int,regCount: int, modFunc: int, devAdd: int,opType: int, regDataArr,valid : bool):
        self.devAdd = devAdd
        self.regAdd = regAdd
        self.regCount = regCount
        self.modfunc = modFunc
        self.regData = regDataArr 
        self.valid   = valid
        self.transactionType = opType   
    
    @classmethod
    def default(cls) :
        return cls(0,0,0,0,0,0,False)

class modbusMqttMsg:
    def blankMsg():
        blank = {
                    "cmdName"   :"",
                    "uuid"      :"",
                    "devId"     :"", 
                    "devProfile":"",
                    "modfunc"   :"",
                    "devAdd"    :0, 
                    "regAdd"    :0,
                    "regCount"  :0,
                    "regData"   :[],
                    "result"    :mqtt2Modbus_ErrorStatus.RESULT_UNKNOWN.value
                }
        
        return blank

    def CreateMsg(cmdName:str,uuid:str,devId:str,devProfile:str,modfunc:int,devAdd:int,regAdd:int,regCount:int,regData:int,result:int):
        Msg = {
                    "cmdName"   :cmdName,
                    "uuid"      :uuid,
                    "devId"     :devId, 
                    "devProfile":devProfile,
                    "modfunc"   :modfunc,
                    "devAdd"    :devAdd,
                    "regAdd"    :regAdd,
                    "regCount"  :regCount,
                    "regData"   :regData,
                    "result"    :result
              }
        
        return Msg


mqttResponse = modbusMqttMsg.blankMsg()
modbusMsgParams = modbusMsg.default()

def modbusMsgTx(modbusHandle : minimalmodbus.Instrument,mqttMsg : dict) -> dict:
     
     #declare mqtt response as global
     global mqttResponse

     #Determine if modbus message being passed as argument is valid
     if modbusMsgParams.valid != False:

        #Set device address as per current message parameters
        modbusHandle.address = modbusMsgParams.devAdd
        #Attempt to read/write to modbus depending on command
        try:
            match modbusMsgParams.modfunc:
                case modbus_function_codes.READ_COILS.value:
                    mqttResponse["regData"] = modbusHandle.read_bit(modbusMsgParams.regAdd,modbusMsgParams.modfunc)
                
                case modbus_function_codes.READ_CONTACTS.value:
                    mqttResponse["regData"] = modbusHandle.read_bits(modbusMsgParams.regAdd,modbusMsgParams.regCount,modbusMsgParams.modfunc)

                case modbus_function_codes.READ_HOLDING_REGISTERS.value:
                    mqttResponse["regData"] = modbusHandle.read_registers(modbusMsgParams.regAdd,modbusMsgParams.regCount,modbusMsgParams.modfunc)

                case modbus_function_codes.READ_INPUT_REGISTERS.value:
                    mqttResponse["regData"] = modbusHandle.read_registers(modbusMsgParams.regAdd,modbusMsgParams.regCount,modbusMsgParams.modfunc)

                case modbus_function_codes.WRITE_SINGLE_COIL.value:
                    mqttResponse["regData"] = modbusHandle.write_bit(modbusMsgParams.regAdd,modbusMsgParams.regData,modbusMsgParams.modfunc)
                
                case modbus_function_codes.WRITE_SINGLE_REGISTER.value:
                    mqttResponse["regData"] = modbusHandle.write_registers(modbusMsgParams.regAdd,modbusMsgParams.regData,modbusMsgParams.modfunc)

                case modbus_function_codes.WRITE_MULTIPLE_COILS.value:
                    mqttResponse["regData"] = modbusHandle.write_registers(modbusMsgParams.regAdd,modbusMsgParams.regData,modbusMsgParams.modfunc)

                case modbus_function_codes.WRITE_MULTIPLE_REGISTERS.value:
                    mqttResponse["regData"] = modbusHandle.write_registers(modbusMsgParams.regAdd,modbusMsgParams.regData,modbusMsgParams.modfunc)

        except:
                    mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MODBUS_IO_FAILED.value
 
     return mqttResponse


def mqttMsg2ModbusMsg(mqttMsg: dict) -> mqtt2Modbus_ErrorStatus | modbusMsg:

    global mqttResponse
    mqttResponse = modbusMqttMsg.blankMsg()

    global modbusMsgParams
    modbusMsgParams = modbusMsg.default() 

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
    
    if not "devId" in mqttMsg:
        print("\"devId\" key was not found")
        mqttResponse["result"] = mqtt2Modbus_ErrorStatus.MISSING_PARAMETER.value
        return mqtt2Modbus_ErrorStatus.MISSING_PARAMETER
    

    mqttResponse = modbusMqttMsg.CreateMsg(
                                            mqttMsg["cmdName"],
                                            mqttMsg["uuid"],
                                            mqttMsg["devId"],
                                            mqttMsg["devProfile"],
                                            mqttMsg["modfunc"],
                                            mqttMsg["devAdd"],
                                            mqttMsg["regAdd"],
                                            mqttMsg["regCount"],
                                            mqttMsg["regData"],
                                            mqtt2Modbus_ErrorStatus.RESULT_UNKNOWN
                                          )
    
   
                
    #If command can be found in devices profile then extract the function and register address parameters
    modbusMsgParams =  modbusMsg(
                                    mqttMsg["regAdd"],
                                    mqttMsg["regCount"],
                                    mqttMsg["modfunc"],
                                    mqttMsg["devAdd"],
                                    0 if mqttMsg["modfunc"] <= 4 else 1,
                                    mqttMsg["regData"],
                                    True
                                )

    mqttResponse["result"] = mqtt2Modbus_ErrorStatus.OK.value
    return modbusMsgParams

    




    





    




    




    




    



