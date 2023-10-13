"""
Name: EMSX_connection.py
Class: EMSXconnect
Author: Andrea Martinelli
Description: simple wrapper to establish a connection with EMSX
Last update: 10/13/2023

Class: TradeEMSX
Author: Andrea Martinelli
Description: script to create and route orders using EMSX.
These are the main functions:
1) createOrder (to create an order and get the EMSX_SEQUENCE)
2) routeOrder (to send the order to the broker using the EMSX_SEQUENCE as reference)
*****3) RouteGroupOrder (to send a group of order to the broker)*****

There are also other minor functions to check the order status:
1) getOrderStatusTable (returns a table with the status of the order)
2) verifyExecution (returns 1 if order is filled, 0 otherwise; also returns a table
                    with the status of the order)

The inputs are the EMSX connection and a dictionary build with mandatory and optional
 fields like that:
{
"ticker": "BBG_Ticker;
"side" = "BUY/SELL";
"qty" = integer > 0;
"ordType" = "MKT" (or other type like limit etc
"broker" = "broker name"
"notes" = "custom text to be added to the order (optional)"
"orderID" = EMSX_SEQUENCE of an order (optional). to be used to get the order info or
            to route an order created in a different instance of the class
}

any optional filed must be added at the end of the dictionary using the EMSX field name
as key and the appropriate value/string as value.
Last update: 10/13/2023
"""

import copy
import time
import blpapi
import sys

class RequestError(Exception):
    """
    A RequestError is raised when there is a problem with a Bloomberg API response.
    """

    def __init__(self, value, description):
        self.value = value
        self.description = description

    def __str__(self):
        return self.description + '\n\n' + str(self.value)

class EMSXconnect():

    def __init__(self,server, host='localhost', port=8194, open=True):
        self.active = False
        self.host = host
        self.port = port
        self.server = server

        self.EMSX_TIF = "DAY"
        self.EMSX_HAND_INSTRUCTION = "MAN"
        self.EMSX_ACCOUNT = "EQUITYCM"

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

class TradeEMSX:
    def __init__(self, params, connection):

        self.requestID = None
        self.EMSX_TIF = "DAY"
        self.EMSX_HAND_INSTRUCTION = "MAN"
        self.EMSX_ACCOUNT = "EQUITYCM"
        self.timeout = 10  # timeout(s)for order setup and routing
        self.fillTimeout = 60  # timeout(s) to wait for filling

        self._inputParams = copy.deepcopy(params)  # (dictionary)

        if "orderID" in params:
            self.emsx_sequence = self._inputParams["orderID"]
            self._inputParams.pop("orderID")
            # todo: use a function to get the other order info to fill the fields

        else:
            self.emsx_sequence = 0
            self.qty = self._inputParams["qty"]
            self._inputParams.pop("qty")
            self.ordType = self._inputParams["ordType"]
            self._inputParams.pop("ordType")
            self.side = self._inputParams["side"]
            self._inputParams.pop("side")
            self.ticker = self._inputParams["ticker"]
            self._inputParams.pop("ticker")
            self.broker = self._inputParams["broker"]
            self._inputParams.pop("broker")
            if "notes" in self._inputParams:
                self.notes = self._inputParams["notes"]
                self._inputParams.pop("notes")
            else:
                self.notes = ""

        self._emsx_conn = connection

        self.orderDict = dict()
        self.orderInfo = None
        self.routedOrderInfo = None
        self.tradeResponse = None
        self.emsx_sequence = None

    def createOrder(self):
        # send the order request and returns the orderID (EMSX_SEQUENCE)

        # fill the orderDict
        self.orderDict["EMSX_TICKER"] = self.ticker
        self.orderDict["EMSX_AMOUNT"] = self.qty
        self.orderDict["EMSX_ORDER_TYPE"] = self.ordType
        self.orderDict["EMSX_SIDE"] = self.side
        self.orderDict["EMSX_BROKER"] = self.broker
        self.orderDict["EMSX_NOTES"] = self.notes
        self.orderDict["EMSX_TIF"] = self.EMSX_TIF
        self.orderDict["EMSX_HAND_INSTRUCTION"] = self.EMSX_HAND_INSTRUCTION
        self.orderDict["EMSX_ACCOUNT"] = self.EMSX_ACCOUNT

        if self._inputParams:
            for key in self._inputParams:
                self.orderDict[key] = self._inputParams[key]

        # create the request and send it
        request = self._emsx_conn.service.createRequest("CreateOrder")

        for key in self.orderDict:
            value = self.orderDict[key]
            request.set(key, value)

        self.requestID = blpapi.CorrelationId()

        self._emsx_conn.session.sendRequest(request, correlationId=self.requestID)

        print("CreateOrder request sent")

        # get the response
        flag = True
        counter = 0
        while flag:
            counter += 1
            event = self._emsx_conn.session.tryNextEvent()
            if event is not None:
                for msg in event:
                    if msg.correlationIds()[0].value() == self.requestID.value():

                        if msg.messageType() == blpapi.Name("ErrorInfo"):
                            errorCode = msg.getElementAsInteger("ERROR_CODE")
                            errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                            print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                            flag = False
                        elif msg.messageType() == blpapi.Name("CreateOrder"):
                            self.emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE")
                            message = msg.getElementAsString("MESSAGE")
                            print("EMSX_SEQUENCE: %d\tMESSAGE: %s" % (self.emsx_sequence, message))
                            flag = False

                time.sleep(1)
                if counter > 50:
                    print("Order not Confirmed")
                    flag = False

        return self.emsx_sequence
