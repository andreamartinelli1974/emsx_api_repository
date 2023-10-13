# TradeEMSX.py

"""
This class must be used to create and route orders via bbg ESMX.
Once instantiated with the info needed, the usable functions are:

CreateOrder: to create an order for a single name
CreateBasket: to collect different orders into a basket
RouteOrder: to route a single or a basket order
RouteGroup: to route a group of existing order (TODO: difference with CreateBasket+RouteOrder)
GetAllFieldMetaData: to get Order infos
"""

import blpapi
import sys
import traceback

SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
ERROR_INFO              = blpapi.Name("ErrorInfo")
CREATE_ORDER            = blpapi.Name("CreateOrder")

bEnd = False

"""
d_service="//blp/emapisvc_beta"
d_host="localhost"
d_port=8194
bEnd=False
"""


class TradeEMSX:
    def __init__(self, params, request):

        self._host = "localhost"
        self._port = 8194

        self.sessionOptions = blpapi.SessionOptions()
        self._server = params.server
        self._inputParams = params
        self._myRequest = request

        self.EMSX_TIF = "DAY"
        self.EMSX_HAND_INSTRUCTION = "MAN"
        self.EMSX_ACCOUNT = "EQUITYCM"

        self.requestID = None

        self.tradeResponse = None
        self.emsx_sequence = None

        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(self._host)
        sessionOptions.setServerPort(self._port)

        print("Connecting to %s:%d" % (self._host, self._port))

        session = blpapi.Session(sessionOptions, self.processEvent)

        if not session.startAsync():
            print("Failed to start session.")
            return

        global bEnd
        while not bEnd:
            pass

        session.stop()

    def processEvent(self, event, session):
        try:
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event, session)

            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event, session)

            elif event.eventType() == blpapi.Event.RESPONSE:
                self.processResponseEvent(event)

            else:
                self.processMiscEvents(event)

        except:
            typeError, value = sys.exc_info()
            print("Exception:  %s" % typeError)
            print(value)
            session.stop()

            global bEnd
            bEnd = True

        #return False

    def processSessionStatusEvent(self, event, session):
        print("Processing SESSION_STATUS event")

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print("Session started...")
                session.openServiceAsync(self._server)

            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print(sys.stderr, "Error: Session startup failed")

            else:
                print(msg)

    def processServiceStatusEvent(self, event, session):
        print("Processing SERVICE_STATUS event")

        for msg in event:

            if msg.messageType() == SERVICE_OPENED:
                print("Service opened...")

                service = session.getService(self._server)

                if self._myRequest == "CreateOrder":
                    print("Creating the Order...")

                    request = service.createRequest("CreateOrder")

                    # params from inputParams
                    request.set("EMSX_TICKER", self._inputParams.EMSX_TICKER)
                    request.set("EMSX_AMOUNT", self._inputParams.EMSX_AMOUNT)
                    request.set("EMSX_ORDER_TYPE", self._inputParams.EMSX_ORDER_TYPE)
                    request.set("EMSX_SIDE", self._inputParams.EMSX_SIDE)
                    request.set("EMSX_BROKER", self._inputParams.EMSX_BROKER)
                    request.set("EMSX_NOTES", self._inputParams.EMSX_NOTES)

                    # constant params hard coded in TradeEMSX
                    request.set("EMSX_TIF", self.EMSX_TIF)
                    request.set("EMSX_HAND_INSTRUCTION", self.EMSX_HAND_INSTRUCTION)
                    request.set("EMSX_ACCOUNT", self.EMSX_ACCOUNT)

                    print("Request: %s" % request.toString())

                    self.requestID = blpapi.CorrelationId()

                    session.sendRequest(request, correlationId=self.requestID)

                elif self._myRequest == "RouteOrder":
                    a = 2

                elif self._myRequest == "CreateBasket":
                    a = 3

                else:
                    print(sys.stderr, "Error: you made an invalid request")

            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                print(sys.stderr, "Error: Service failed to open")

    def processResponseEvent(self, event):
        print("Processing RESPONSE event")

        for msg in event:

            print("MESSAGE: %s" % msg.toString())
            print("CORRELATION ID: %d" % msg.correlationIds()[0].value())

            if msg.correlationIds()[0].value() == self.requestID.value():
                print("MESSAGE TYPE: %s" % msg.messageType())

                if msg.messageType() == ERROR_INFO:
                    errorCode = msg.getElementAsInteger("ERROR_CODE")
                    errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                    print("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode, errorMessage))
                elif msg.messageType() == CREATE_ORDER:
                    self.emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE")
                    message = msg.getElementAsString("MESSAGE")
                    print("EMSX_SEQUENCE: %d\tMESSAGE: %s" % (self.emsx_sequence, message))

                global bEnd
                bEnd = True

    def processMiscEvents(self, event):

        print("Processing " + event.eventType() + " event")

        for msg in event:
            print("MESSAGE: %s" % (msg.tostring()))

