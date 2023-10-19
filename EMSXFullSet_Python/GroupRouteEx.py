# GroupRouteEx.py

import blpapi
import sys


SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
ERROR_INFO              = blpapi.Name("ErrorInfo")
GROUP_ROUTE_EX          = blpapi.Name("GroupRouteEx")

d_service="//blp/emapisvc_beta"
d_host="localhost"
d_port=8194
bEnd=False


class SessionEventHandler():

    def processEvent(self, event, session):
        try:
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event,session)
            
            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.RESPONSE:
                self.processResponseEvent(event)
            
            else:
                self.processMiscEvents(event)
                
        except:
            print ("Exception:  %s" % sys.exc_info()[0])
            
        return False


    def processSessionStatusEvent(self,event,session):
        print ("Processing SESSION_STATUS event")

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print ("Session started...")
                session.openServiceAsync(d_service)
                
            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print >> sys.stderr, ("Error: Session startup failed")
                
            else:
                print (msg)
                

    def processServiceStatusEvent(self,event,session):
        print ("Processing SERVICE_STATUS event")
        
        for msg in event:
            
            if msg.messageType() == SERVICE_OPENED:
                print ("Service opened...")

                service = session.getService(d_service)
    
                request = service.createRequest("GroupRouteEx")

                # Multiple order numbers can be added
                request.append("EMSX_SEQUENCE", 5156814)
                request.append("EMSX_SEQUENCE", 5156815)
                request.append("EMSX_SEQUENCE", 5156816)

                # The fields below are mandatory
                request.set("EMSX_AMOUNT_PERCENT", 50)  # Note the amount here is %age of order amount
                request.set("EMSX_BROKER", "BMTB")
                
                # For GroupRoute, the below values need to be added, but are taken 
                # from the original order when the route is created.
                request.set("EMSX_HAND_INSTRUCTION", "ANY")
                request.set("EMSX_ORDER_TYPE", "MKT")
                request.set("EMSX_TICKER", "XOM US Equity")
                request.set("EMSX_TIF", "DAY")
            
                # The fields below are optional
                #request.set("EMSX_ACCOUNT","TestAccount")
                #request.set("EMSX_BOOKNAME","BookName")
                #request.set("EMSX_CFD_FLAG", "1")
                #request.set("EMSX_CLEARING_ACCOUNT", "ClrAccName")
                #request.set("EMSX_CLEARING_FIRM", "FirmName")
                #request.set("EMSX_EXEC_INSTRUCTION", "Drop down values from EMSX Ticket")
                #request.set("EMSX_GET_WARNINGS", "0")
                #request.set("EMSX_GTD_DATE", "20170105")
                #request.set("EMSX_LIMIT_PRICE", 123.45)
                #request.set("EMSX_LOCATE_BROKER", "BMTB")
                #request.set("EMSX_LOCATE_ID", "SomeID")
                #request.set("EMSX_LOCATE_REQ", "Y")
                #request.set("EMSX_NOTES", "Some notes")
                #request.set("EMSX_ODD_LOT", "0")
                #request.set("EMSX_P_A", "P")
                #request.set("EMSX_RELEASE_TIME", 34341)
                #request.set("EMSX_REQUEST_SEQ", 1001)
                #request.set("EMSX_STOP_PRICE", 123.5)
                #request.set("EMSX_TRADER_UUID", 1234567)
                
                # Set the Request Type if this is for multi-leg orders
                # only valid for options
                '''
                requestType = request.getElement("EMSX_REQUEST_TYPE") 
                requestType.setChoice("Multileg")
                multileg = requestType.getElement("Multileg")
                multileg.setElement("EMSX_AMOUNT",10)
                multileg.getElement("EMSX_ML_RATIO").appendValue(2)
                multileg.getElement("EMSX_ML_RATIO").appendValue(3)
                '''
                
                # Add the Route Ref ID values
                """
                routeRefIDPairs = request.getElement("EMSX_ROUTE_REF_ID_PAIRS")
                route1 = routeRefIDPairs.appendElement()
                route1.setElement("EMSX_ROUTE_REF_ID","MyRouteRef1")
                route1.setElement("EMSX_SEQUENCE",5156814)
                
                route2 = routeRefIDPairs.appendElement();
                route2.setElement("EMSX_ROUTE_REF_ID","MyRouteRef2")
                route2.setElement("EMSX_SEQUENCE",5156815)
                
                route3 = routeRefIDPairs.appendElement()
                route3.setElement("EMSX_ROUTE_REF_ID","MyRouteRef3")
                route3.setElement("EMSX_SEQUENCE",5156816)
                """
                
                # Below we establish the strategy details. Strategy details
                # are common across all orders in a GroupRoute operation.
                '''
                strategy = request.getElement("EMSX_STRATEGY_PARAMS")
                strategy.setElement("EMSX_STRATEGY_NAME", "VWAP")
                
                indicator = strategy.getElement("EMSX_STRATEGY_FIELD_INDICATORS")
                data = strategy.getElement("EMSX_STRATEGY_FIELDS")
                
                # Strategy parameters must be appended in the correct order. See the output 
                # of GetBrokerStrategyInfo request for the order. The indicator value is 0 for 
                # a field that carries a value, and 1 where the field should be ignored
                
                data.appendElement().setElement("EMSX_FIELD_DATA", "09:30:00")  # StartTime
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)

                data.appendElement().setElement("EMSX_FIELD_DATA", "10:30:00")  # EndTime
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # Max%Volume
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # %AMSession
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # OPG
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # MOC
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # CompletePX
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                   
                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # TriggerPX
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # DarkComplete
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # DarkCompPX
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # RefIndex
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)

                data.appendElement().setElement("EMSX_FIELD_DATA", "")          # Discretion
                indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                '''
                print ("Request: %s" % request.toString())
                    
                self.requestID = blpapi.CorrelationId()
                
                session.sendRequest(request, correlationId=self.requestID )
                            
            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                print >> sys.stderr, ("Error: Service failed to open")        
                
    def processResponseEvent(self, event):
        print ("Processing RESPONSE event")
        
        for msg in event:
            
            print ("MESSAGE: %s" % msg.toString())
            print ("CORRELATION ID: %d" % msg.correlationIds()[0].value())


            if msg.correlationIds()[0].value() == self.requestID.value():
                print ("MESSAGE TYPE: %s" % msg.messageType())
                
                if msg.messageType() == ERROR_INFO:
                    errorCode = msg.getElementAsInteger("ERROR_CODE")
                    errorMessage = msg.getElementAsString("ERROR_MESSAGE")
                    print ("ERROR CODE: %d\tERROR MESSAGE: %s" % (errorCode,errorMessage))
                elif msg.messageType() == GROUP_ROUTE_EX:

                    if(msg.hasElement("EMSX_SUCCESS_ROUTES")):
                        success = msg.getElement("EMSX_SUCCESS_ROUTES")

                        nV = success.numValues()
                        
                        for i in range(0,nV):
                            e = success.getValueAsElement(i)
                            sq = e.getElementAsInteger("EMSX_SEQUENCE")
                            rid = e.getElementAsInteger("EMSX_ROUTE_ID")

                            print ("SUCCESS: %d,%d" % (sq,rid))
                    
                    if(msg.hasElement("EMSX_FAILED_ROUTES")):
                        failed = msg.getElement("EMSX_FAILED_ROUTES")

                        nV = failed.numValues()
                        
                        for i in range(0,nV):
                            e = failed.getValueAsElement(i)
                            sq = e.getElementAsInteger("EMSX_SEQUENCE")

                            print ("FAILED: %d" % (sq))                                                            

                global bEnd
                bEnd = True
                
    def processMiscEvents(self, event):
        
        print ("Processing " + event.eventType() + " event")
        
        for msg in event:

            print ("MESSAGE: %s" % (msg.tostring()))


def main():
    
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host)
    sessionOptions.setServerPort(d_port)

    print ("Connecting to %s:%d" % (d_host,d_port))

    eventHandler = SessionEventHandler()

    session = blpapi.Session(sessionOptions, eventHandler.processEvent)

    if not session.startAsync():
        print ("Failed to start session.")
        return
    
    global bEnd
    while bEnd==False:
        pass
    
    session.stop()
    
if __name__ == "__main__":
    print ("Bloomberg - EMSX API Example - GroupRouteEx")
    try:
        main()
    except KeyboardInterrupt:
        print ("Ctrl+C pressed. Stopping...")


__copyright__ = """
Copyright 2017. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""
