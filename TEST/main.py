# test to start a EMSX Session and service

import EMSX_connection as emsx

d_service="//blp/emapisvc_beta"


def main():

    # testConnection = ESMX.ESMX(d_service)
    # request = testConnection.service.createRequest("CreateOrder")

    emsx_conn = emsx.EMSXconnect(d_service)

    params = {
        "ticker": "ISP IM Equity",
        "side": "BUY",
        "qty": 1,
        "ordType": "MKT",
        "broker": "BMTB", # IMIB, BB, BMTB, EFIX, IMIB, API
        "notes": "testing order"
    }

    trade_emsx = emsx.TradeEMSX(params,emsx_conn)

    check_order = trade_emsx.createOrder()
    message_order = trade_emsx.getInfo()
    check_route = trade_emsx.routeOrder()
    message_route = trade_emsx.getInfo()

    params = {
        "ticker": "UCG IM Equity",
        "side": "BUY",
        "qty": 2,
        "ordType": "MKT",
        "broker": "BMTB",  # IMIB, BB, BMTB, EFIX, IMIB, API
        "notes": "testing order",
        "EMSX_SETTLE_CURRENCY": "USD"
    }

    trade_emsx2 = emsx.TradeEMSX(params, emsx_conn)

    check_order2 = trade_emsx2.createOrder()
    message_order2 = trade_emsx2.getInfo()

    params = {"orderID": check_order2}

    trade_emsx3 = emsx.TradeEMSX(params, emsx_conn)

    check_route = trade_emsx3.routeOrder()
    message_route = trade_emsx3.getInfo()

    trade_emsx4 = emsx.TradeEMSX(None, emsx_conn)
    flag, info = trade_emsx4.verifyExecution(check_order2)

    emsx_conn.close()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


