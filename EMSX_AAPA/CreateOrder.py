"""
AAPA version of the BBG EMSX CreateOrder.py
"""

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
