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
        "broker": "IMIB",
        "notes": "testing order"
    }

    trade_emsx = emsx.TradeEMSX(params,emsx_conn)

    check = trade_emsx.createOrder()

    emsx_conn.close()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


