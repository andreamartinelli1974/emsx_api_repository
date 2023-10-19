"""
Name: EMSX_wrapper.py
Class: EMSXconnect
Author: Andrea Martinelli
Description: simple wrapper to establish a connection with EMSX
Last update: 10/17/2023
"""

import copy
import time
import blpapi
import sys


class EMSXconnect():

    def __init__(self, server, host='localhost', port=8194, open=True):
        self.active = False
        self.host = host
        self.port = port
        self.server = server

        if open:
            self.open()

    def open(self):
        if not self.active:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(self.host)
            sessionOptions.setServerPort(self.port)
            self.session = blpapi.Session(sessionOptions)
            self.session.start()
            self.session.openService(self.server)
            self.service = self.session.getService(self.server)
            event = self.session.tryNextEvent()
            for msg in event:
                if msg.messageType() == blpapi.Name("SessionConnectionUp"):
                    self.active = True
                    print("Connected to EMSX")
                else:
                    print("fatal error: connection to EMSX failed", file=sys.stderr)
                    sys.exit(1)

    def close(self):
        self.session.stop()
        self.active = False
        print("EMSX disconnected")


class EMSX():
    def __init__(self, server):
        self.server = server
        self._connection = EMSXconnect(self.server)

        self.EMSX_TIF = "DAY"
        self.EMSX_HAND_INSTRUCTION = "MAN"
        self.EMSX_ACCOUNT = "EQUITYCM"
        self.pause = 0.01
        self.eventTimeout = 6000

        self.orderDict = dict()
        self.requestID_list = list()
        self.requestID = None
        self.emsx_route_id_list = list()
        self.emsx_seq_tuple = tuple()
        self.emsx_sequence = None
        self._inputParams_list = list()
        self._inputParams = None
        self.orderInfo = None
        self.routedOrderInfo = None
        self.tradeResponse = None

    def createOrder(self, params):
        # object filled with user info
        self._inputParams = copy.deepcopy(params)  # (dictionary)
        self.orderDict = {}  # clear the orderDict from previous info

        self.emsx_sequence = 0
        self.orderDict["EMSX_AMOUNT"] = self._inputParams["qty"]
        self._inputParams.pop("qty")
        self.orderDict["EMSX_ORDER_TYPE"] = self._inputParams["ordType"]
        self._inputParams.pop("ordType")
        self.orderDict["EMSX_SIDE"] = self._inputParams["side"]
        self._inputParams.pop("side")
        self.orderDict["EMSX_TICKER"] = self._inputParams["ticker"]
        self._inputParams.pop("ticker")
        self.orderDict["EMSX_BROKER"] = self._inputParams["broker"]
        self._inputParams.pop("broker")
        if "notes" in self._inputParams:
            self.orderDict["EMSX_NOTES"] = self._inputParams["notes"]
            self._inputParams.pop("notes")
        else:
            self.orderDict["EMSX_NOTES"] = ""

        self.orderDict["EMSX_TIF"] = self.EMSX_TIF
        self.orderDict["EMSX_HAND_INSTRUCTION"] = self.EMSX_HAND_INSTRUCTION
        self.orderDict["EMSX_ACCOUNT"] = self.EMSX_ACCOUNT

        if self._inputParams:
            for key in self._inputParams:
                self.orderDict[key] = self._inputParams[key]

        # create the request and send it
        request = self._connection.service.createRequest("CreateOrder")

        for key in self.orderDict:
            value = self.orderDict[key]
            request.set(key, value)

        self.requestID = blpapi.CorrelationId()

        self._connection.session.sendRequest(request, correlationId=self.requestID)

        # print("CreateOrder request sent for " + self.orderDict["EMSX_TICKER"])

        # get the response
        flag = True
        counter = 0
        while flag:
            counter += 1
            # print(str(counter))
            event = self._connection.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() == self.requestID.value():

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            self.emsx_sequence = errorMessage
                            flag = False
                        elif msg.messageType() == blpapi.Name("CreateOrder"):
                            self.emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE")
                            message = msg.getElementAsString("MESSAGE")
                            # print("EMSX_SEQUENCE: %d\tMESSAGE: %s" % (self.emsx_sequence, message))
                            flag = False

            time.sleep(self.pause)
            if counter > self.eventTimeout:
                print("Order not Confirmed")
                flag = False

        return self.emsx_sequence

    def createGroupOrder(self,param_list):

        self._inputParams_list = copy.deepcopy(param_list)
        for param in self._inputParams_list:
            self.orderDict = {}  # clear the orderDict from previous info

            self.emsx_sequence = 0
            self.orderDict["EMSX_AMOUNT"] = param["qty"]
            param.pop("qty")
            self.orderDict["EMSX_ORDER_TYPE"] = param["ordType"]
            param.pop("ordType")
            self.orderDict["EMSX_SIDE"] = param["side"]
            param.pop("side")
            self.orderDict["EMSX_TICKER"] = param["ticker"]
            param.pop("ticker")
            self.orderDict["EMSX_BROKER"] = param["broker"]
            param.pop("broker")
            if "notes" in param:
                self.orderDict["EMSX_NOTES"] = param["notes"]
                param.pop("notes")
            else:
                self.orderDict["EMSX_NOTES"] = ""

            self.orderDict["EMSX_TIF"] = self.EMSX_TIF
            self.orderDict["EMSX_HAND_INSTRUCTION"] = self.EMSX_HAND_INSTRUCTION
            self.orderDict["EMSX_ACCOUNT"] = self.EMSX_ACCOUNT

            if param:
                for key in param:
                    self.orderDict[key] = param[key]

            # create the request and send it
            request = self._connection.service.createRequest("CreateOrder")

            for key in self.orderDict:
                value = self.orderDict[key]
                request.set(key, value)

            self.requestID = blpapi.CorrelationId()

            self._connection.session.sendRequest(request, correlationId=self.requestID)

            self.requestID_list.append(self.requestID.value())

        # get the response
        flag = True
        counter = 0
        while flag:
            counter += 1
            # print(str(counter))
            event = self._connection.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() in self.requestID_list:

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            self.emsx_seq_tuple = self.emsx_seq_tuple + (errorMessage,)

                        elif msg.messageType() == blpapi.Name("CreateOrder"):
                            self.emsx_seq_tuple = self.emsx_seq_tuple + (msg.getElementAsInteger("EMSX_SEQUENCE"),)
                            message = msg.getElementAsString("MESSAGE")
                            # print("EMSX_SEQUENCE: %d\tMESSAGE: %s" % (self.emsx_sequence, message))

                        if len(self.emsx_seq_tuple) == len(self.requestID_list):
                            flag = False

            time.sleep(self.pause)
            if counter > self.eventTimeout:
                print("Some orders not confirmed before eventTimeout")
                flag = False

        return self.emsx_seq_tuple
    def routeOrder(self):
        # route the order stored in the object
        # fill the orderDict
        self.orderDict["EMSX_SEQUENCE"] = self.emsx_sequence
        self.orderDict.pop("EMSX_SIDE")

        if self._inputParams:
            for key in self._inputParams:
                self.orderDict[key] = self._inputParams[key]

        # create the request and send it
        request = self._connection.service.createRequest("RouteEx")

        for key in self.orderDict:
            value = self.orderDict[key]
            request.set(key, value)

        self.requestID = blpapi.CorrelationId()

        self._connection.session.sendRequest(request, correlationId=self.requestID)

        print("RouteOrder request sent for " + self.orderDict["EMSX_TICKER"])

        # get the response
        flag = True
        counter = 0
        while flag:
            counter += 1
            # print(str(counter))
            event = self._connection.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() == self.requestID.value():

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            flag = False
                        elif msg.messageType() == blpapi.Name("Route"):
                            self.emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE")
                            self.emsx_route_id = msg.getElementAsInteger("EMSX_ROUTE_ID")
                            message = msg.getElementAsString("MESSAGE")
                            print("EMSX_SEQUENCE: %d\tMESSAGE: %s" % (self.emsx_sequence, message))
                            flag = False

            time.sleep(self.pause)
            if counter > self.eventTimeout:
                print("Order not Confirmed")
                flag = False

        return self.emsx_route_id

    def routeGroupOrder(self, amountPerc = 100):
        # create the request
        request = self._connection.service.createRequest("GroupRouteEx")

        for orderID in self.emsx_seq_tuple:
            if type(orderID) == int:
                request.append("EMSX_SEQUENCE", orderID)

        request.set("EMSX_AMOUNT_PERCENT", amountPerc)
        request.set("EMSX_BROKER",self.orderDict["EMSX_BROKER"])

        # must be included but are meaningless, because the real info are automately retrived from the orders
        request.set("EMSX_HAND_INSTRUCTION", self.orderDict["EMSX_HAND_INSTRUCTION"])
        request.set("EMSX_ORDER_TYPE", self.orderDict["EMSX_ORDER_TYPE"])
        request.set("EMSX_TICKER", "XOM US Equity")
        request.set("EMSX_TIF", "DAY")

        self.requestID = blpapi.CorrelationId()

        self._connection.session.sendRequest(request, correlationId=self.requestID)

        print("RouteGroupOrder request sent")

        # get the response
        flag = True
        counter = 0
        while flag:
            counter += 1
            # print(str(counter))
            event = self._connection.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() == self.requestID.value():

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            flag = False
                        elif msg.messageType() == blpapi.Name("GroupRouteEx"):
                            if (msg.hasElement("EMSX_SUCCESS_ROUTES")):
                                success = msg.getElement("EMSX_SUCCESS_ROUTES")

                                nV = success.numValues()

                                for i in range(0, nV):
                                    e = success.getValueAsElement(i)
                                    sq = e.getElementAsInteger("EMSX_SEQUENCE")
                                    rid = e.getElementAsInteger("EMSX_ROUTE_ID")

                                    print("SUCCESS: %d,%d" % (sq, rid))
                                    self.emsx_route_id_list.append(rid)
                                    flag = False

                            if (msg.hasElement("EMSX_FAILED_ROUTES")):
                                failed = msg.getElement("EMSX_FAILED_ROUTES")

                                nV = failed.numValues()

                                for i in range(0, nV):
                                    e = failed.getValueAsElement(i)
                                    sq = e.getElementAsInteger("EMSX_SEQUENCE")

                                    print("FAILED: %d" % (sq))
                                    self.emsx_route_id_list.append("not routed")
                                    flag = False

            time.sleep(self.pause)
            if counter > self.eventTimeout:
                print("Order not Confirmed")
                flag = False

        return self.emsx_route_id_list

    def getInfo(self, orderID=None):
        if orderID is not None:
            self.emsx_sequence = orderID

        # create the request and send it
        request = self._connection.service.createRequest("OrderInfo")
        request.set("EMSX_SEQUENCE",self.emsx_sequence)

        self.requestID = blpapi.CorrelationId()

        self._connection.session.sendRequest(request, correlationId=self.requestID)

        print("OrderInfo request sent")

        # get the response
        flag = True
        counter = 0
        message = dict()
        while flag:
            counter += 1
            # print(str(counter))
            event = self._connection.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() == self.requestID.value():

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            flag = False
                        elif msg.messageType() == blpapi.Name("OrderInfo"):
                            message["EMSX_TICKER"] = msg.getElementAsString("EMSX_TICKER")
                            message["EMSX_EXCHANGE"] = msg.getElementAsString("EMSX_EXCHANGE")
                            message["EMSX_SIDE"] = msg.getElementAsString("EMSX_SIDE")
                            message["EMSX_POSITION"] = msg.getElementAsString("EMSX_POSITION")
                            message["EMSX_PORT_MGR"] = msg.getElementAsString("EMSX_PORT_MGR")
                            message["EMSX_NOTES"] = msg.getElementAsString("EMSX_NOTES")
                            message["EMSX_TRADER"] = msg.getElementAsString("EMSX_TRADER")
                            message["EMSX_AMOUNT"] = msg.getElementAsInteger("EMSX_AMOUNT")
                            message["EMSX_IDLE_AMOUNT"] = msg.getElementAsInteger("EMSX_IDLE_AMOUNT")
                            message["EMSX_WORKING"] = msg.getElementAsInteger("EMSX_WORKING")
                            message["EMSX_FILLED"] = msg.getElementAsInteger("EMSX_FILLED")
                            message["EMSX_TS_ORDNUM"] = msg.getElementAsInteger("EMSX_TS_ORDNUM")
                            message["EMSX_LIMIT_PRICE"] = msg.getElementAsFloat("EMSX_LIMIT_PRICE")
                            message["EMSX_AVG_PRICE"] = msg.getElementAsFloat("EMSX_AVG_PRICE")
                            message["EMSX_FLAG"] = msg.getElementAsInteger("EMSX_FLAG")
                            message["EMSX_SUB_FLAG"] = msg.getElementAsInteger("EMSX_SUB_FLAG")
                            message["EMSX_YELLOW_KEY"] = msg.getElementAsString("EMSX_YELLOW_KEY")
                            message["EMSX_BASKET_NAME"] = msg.getElementAsString("EMSX_BASKET_NAME")
                            message["EMSX_ORDER_CREATE_DATE"] = msg.getElementAsString("EMSX_ORDER_CREATE_DATE")
                            message["EMSX_ORDER_CREATE_TIME"] = msg.getElementAsString("EMSX_ORDER_CREATE_TIME")
                            message["EMSX_ORDER_TYPE"] = msg.getElementAsString("EMSX_ORDER_TYPE")
                            message["EMSX_TIF"] = msg.getElementAsString("EMSX_TIF")
                            message["EMSX_BROKER"] = msg.getElementAsString("EMSX_BROKER")
                            message["EMSX_TRADER_UUID"] = msg.getElementAsString("EMSX_TRADER_UUID")
                            message["EMSX_STEP_OUT_BROKER"] = msg.getElementAsString("EMSX_STEP_OUT_BROKER")
                            flag = False

            time.sleep(self.pause)
            if counter > self.eventTimeout:
                print("Unable to retrieve order info")
                flag = False

        return message

    def verifyExecution(self, orderID=None):
        # verify the execution of an order (True: filled, False: otherwise) and return also a
        # dictionary with the main order info

        if orderID is not None:
            self.emsx_sequence = orderID

        orderInfo = self.getInfo()

        # todo  order filled in number of shares and in % of the requested amount
        if "EMSX_FILLED" in orderInfo:
            filled_qty = int(orderInfo["EMSX_FILLED"])
            filled_perc = filled_qty/orderInfo["EMSX_AMOUNT"]

        return filled_qty, filled_perc, orderInfo
    def closeConnection(self):
        self._connection.close()
