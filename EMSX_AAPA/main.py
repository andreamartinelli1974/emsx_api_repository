# This is a sample Python script.
import time
import sys, os

os.environ['PYTHONASYNCIODEBUG'] = '1'


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.



from TredeEMSX import TradeEMSX

d_service = "//blp/emapisvc_beta"
broker = "IMIB"


class inputParams():
    def __init__(self):
        self.server = None
        self.EMSX_TICKER = None
        self.EMSX_AMOUNT = 0
        self.EMSX_ORDER_TYPE = None
        self.EMSX_BROKER = None
        self.EMSX_SIDE = None
        self.EMSX_NOTES = "none"


def main():
    """
    session = blpapi.Session(sessionOptions)

    if not session.startAsync():
        print("Failed to start session.")
        return
    """
    tickers = [
        "AAPL US Equity",
        "LYB US Equity",
        "AXP US Equity",
        "VZ US Equity",
        "AVGO US Equity",
        "BA US Equity",
        "CAT US Equity",
        "JPM US Equity",
        "CVX US Equity",
        "KO US Equity"
    ]

    esmx_sequences = []

    for tkr in tickers:
        params = inputParams()
        params.server = d_service
        params.EMSX_TICKER = tkr
        params.EMSX_AMOUNT = 10
        params.EMSX_SIDE = "BUY"
        params.EMSX_ORDER_TYPE = "MKT"
        params.EMSX_BROKER = broker
        params.EMSX_NOTES = "this is my test order"

        trade = TradeEMSX(params, "CreateOrder")
        esmx_sequences.append(trade.emsx_sequence)
        time.sleep(5)

    a = 1


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
