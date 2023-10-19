# test to start a EMSX Session and service

import EMSX_connection as emsx
import EMSX_wrapper as ew
from datetime import datetime

d_service="//blp/emapisvc_beta"


def main():

    # testConnection = ESMX.ESMX(d_service)
    # request = testConnection.service.createRequest("CreateOrder")

    emsx_conn = emsx.EMSXconnect(d_service)

    start = datetime.now()

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
    check_route = trade_emsx.routeOrder()

    end = datetime.now()
    myTime = end-start
    print(myTime)

    order_info = trade_emsx.getInfo()


    params = {
        "ticker": "UCG IM Equity",
        "side": "BUY",
        "qty": 10,
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
    filled_qty, filled_perc , info = trade_emsx4.verifyExecution(check_order2)

    emsx_conn.close()

    emsx_w = ew.EMSX(d_service)

    start = datetime.now()
    params = {
        "ticker": "ISP IM Equity",
        "side": "BUY",
        "qty": 1,
        "ordType": "MKT",
        "broker": "BMTB",  # IMIB, BB, BMTB, EFIX, IMIB, API
        "notes": "testing order"
    }

    emsx_w.createOrder(params)
    emsx_w.routeOrder()
    end = datetime.now()
    myTime = end-start
    print(myTime)

    info_w = emsx_w.getInfo()
    f_qty, f_perc, info_w2 = emsx_w.verifyExecution()


    param_list = list()
    for i in range(150):
        params_group = {
            "ticker": "ISP IM Equity",
            "side": "BUY",
            "qty": 1,
            "ordType": "LMT", # MKT, LMT, ...
            "broker": "BMTB",  # IMIB, BB, BMTB, EFIX, IMIB, API
            "notes": "testing order",
            "EMSX_LIMIT_PRICE": 2.4
        }
        param_list.append(params_group)

    start = datetime.now()
    orderId_tuple = emsx_w.createGroupOrder(param_list)
    end = datetime.now()
    myTime = end - start
    print(myTime)

    routedId_list = emsx_w.routeGroupOrder()
    end = datetime.now()
    myTime = end - start
    print(myTime)

    emsx_w.closeConnection()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


