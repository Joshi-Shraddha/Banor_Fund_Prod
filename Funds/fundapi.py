from pathlib import Path
import sys
import logging
import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from funds import Fund_Send
from sqlalchemy.ext.declarative import declarative_base
from configuration import database_dev
import requests
import os
import webbrowser
import xml.etree.ElementTree as ET

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

from Trading.Trading_popups import trading_popups
from Trading.suggestionbox_icons import OrderReceivedOnTradingDesk, OrdersApproving, OrdersRejecting, OrdersCancel,Disable_connection
from Trading.Fx_hedging_1 import fx_hedging, currency_calculation

app = Flask(__name__)
Base = declarative_base()

CORS(app)

template_dir = os.path.abspath(r"C:\Users\Aress\Projects\_build\html\index.html")
app = Flask(__name__, template_folder=template_dir)


class DatabaseHandler(logging.Handler):

    def __init__(self, fund, logger_sqlalchemy):
        super().__init__()
        self.fund = fund
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.setLevel(logging.DEBUG)
        self.logger = logger_sqlalchemy

    def emit(self, record):
        conn = database_dev()
        cursor = conn.cursor()
        self.format(record)
        trace = None
        log_time = datetime.datetime.strptime(record.__dict__['asctime'], "%Y-%m-%d %H:%M:%S,%f")
        logger = record.__dict__['name']
        level = record.__dict__['levelname']
        msg = record.__dict__['msg'],
        pathname = record.__dict__['pathname'],
        line_no = record.__dict__['lineno']
        trace = str(pathname) + str(line_no)

        fund_name = self.fund
        if msg:
            Message = msg[0]
        else:
            Message = "NULL"

        self.logger.handlers = []

        insert_query = (
            '''INSERT INTO [outsys_prod].[Banor].[Fund_Log] (Logger, Level, Message, DateTime,FundName,Trace) VALUES (%s,%s,%s,%s,%s,%s)''')
        cursor.execute(insert_query, (logger, level, Message, log_time, fund_name, trace))
        conn.commit()
        return "return Logging"


@app.route('/query_records', methods=['GET', 'POST'])
def query_records():
    """
    This is an api to called funds script.

    """
    req_data = request.get_json()
    try:
        print(req_data)
        side = req_data.get("side")
        cashOrSwap = req_data.get("cashOrSwap")
        securityType = req_data.get("securityType")
        tickerIsin = req_data.get("tickerIsin")
        sStockName = req_data.get("stockName")
        lastPrice = req_data.get("lastPrice")
        weight_Target = req_data.get("weight_Target")
        multiplier = req_data.get("multiplier")
        instructions = req_data.get("instructions")
        settleDate = req_data.get("settleDate")
        suggestedBroker = req_data.get("suggestedBroker")
        limit = req_data.get("limit")
        urgency = req_data.get("urgency")
        expiry = req_data.get("expiry")
        userComment = req_data.get("userComment")
        quantity = req_data.get("quantity")
        fund = req_data.get("fund")
        fund_id = req_data.get("id")
        userId = req_data.get("userId")
        userName = req_data.get("userName")
        fundNumber = req_data.get("fundNumber")
        FundCurrency = req_data.get("fundCurrency")
        ShowQuantityBox = req_data.get("ShowQuantityBox")

        response = Fund_Send(sStockName=sStockName, TickerISIN=tickerIsin, MultiplierQuantity=multiplier,
                             lastPrice=lastPrice, TradeQuantityPreciseIndicated=quantity, fund_id=fund_id, side=side,
                             cashOrSwap=cashOrSwap, userName=userName, securityType=securityType,
                             weight_Target=weight_Target, instructions=instructions,
                             settleDate=settleDate, suggestedBroker=suggestedBroker, limit=limit,
                             userId=userId, urgency=urgency,
                             expiry=expiry, userComment=userComment, fund=fund, FundNumber=fundNumber,
                             FundCurrency=FundCurrency,
                             ShowQuantityBox=ShowQuantityBox)
        if response == 'TickerISIN matching records not founds':
            return jsonify({"Result": response}), 400
        return jsonify({"Result": response})
    except Exception as e:
        print(str(e))
        fund = req_data.get("fund")
        logger_sqlalchemy = logging.getLogger(__name__)
        logger_sqlalchemy.setLevel(logging.INFO)
        logger_sqlalchemy.addHandler(DatabaseHandler(fund, logger_sqlalchemy))
        logger_sqlalchemy.error(str(e))

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error on line no. : {exc_tb.tb_lineno} - {exc_type} - {fname}")

        exception_json = {"status": 400, "error": str(e), "message": "Api down please try after sometime..!!"}
        return jsonify(exception_json),400


def get_bbg_records_from_db(TickerISIN):
    """

    This is an api for fetch name,currency and  last price from db.

    :param TickerISIN: TickerISIN
    :return: name,currency and  last price

    """
    try:
        conn = database_dev()
        cursor = conn.cursor()
        query = (
                "select top 1 * from [outsys_prod].DBO.[OSUSR_BOL_CONTROLTICKER_ISIN]  where [TICKERREQUESTED] ='" + str(
            TickerISIN) + "' order by DATETIME  desc")
        cursor.execute(query)
        rec = cursor.fetchone()
        try:
            Name = rec[5]
            print('Name: ', Name)
        except:
            Name = 'Null'
        try:
            currency = rec[7]
            print('currency: ', currency)
        except:
            currency = 'Null'
        try:
            Last_price = rec[18]
            print('Last_price: ', Last_price)
        except:
            Last_price = 'Null'

        if Last_price == 'Null' and currency == 'Null' and Name == 'Null':
            return {"status": 400, "message": "Bloomberg api down please try after sometime..!!"},400

        context = {"Name": Name, "currency": currency, "Last_price": str(Last_price)}
        return context

    except Exception as e:
        print(str(e))
        exception_json = {"status": 400, "error": str(e), "message": "Bloomberg api down please try after sometime..!!"}
        return jsonify(exception_json)


@app.route('/get_bbg_records', methods=['GET'])
def get_bbg_records():
    """

    This is an api for fetch name,currency and  last price from bbg.
    :return: name,currency and  last price.

    """
    TickerISIN = request.args['TickerISIN']
    try:
        context = {}
        try:
            response = requests.get("http://172.16.20.90:8080/InstrumentInfo", params={"Code": TickerISIN})
            tree = response.text
            mytree = ET.ElementTree(ET.fromstring(tree))
            root = mytree.getroot()
            if root:
                for att in root:
                    try:
                        NAME = att.find('NAME').text
                        print('NAME: ', NAME)
                    except:
                        NAME = 'Null'

                    try:
                        CRNCY = att.find('CRNCY').text
                        print('CRNCY: ', CRNCY)
                    except:
                        CRNCY = 'Null'
                    try:
                        PX_LAST = att.find('PX_LAST').text
                        print('PX_LAST: ', PX_LAST)
                    except:
                        PX_LAST = '0'

                    context = {"Name": NAME, "currency": CRNCY, "Last_price": str(PX_LAST)}
            else:
                context = get_bbg_records_from_db(TickerISIN)
                # context = {"Result": "BBG Workstation down"}
        except:
            context = get_bbg_records_from_db(TickerISIN)
        return jsonify(context), 200
    except Exception as e:
        return jsonify({"Error": str(e)}), 400


@app.route('/treader_edit_pop', methods=['GET', 'POST'])
def treader_edit_pop():
    """
    Edit popup api

    """
    req_data = request.get_json()
    try:
        print(req_data)
        OrderID = req_data.get("OrderID")
        BrokerID = req_data.get("BrokerID")
        ChangeBroker = req_data.get("ChangeBroker")
        InsertExecution = req_data.get("InsertExecution")
        ExecutedQuantity = req_data.get("ExecutedQuantity")
        ExecutionPrice = req_data.get("ExecutionPrice")
        NothingDone = req_data.get("NothingDone")
        ValueDate = req_data.get("ValueDate")
        user_id = req_data.get("user_id")
        user_name = req_data.get("username")
        fund_id = req_data.get("fund_id")
        fund_number = req_data.get("fund_number")
        Multiplier = req_data.get("Multiplier")
        response = trading_popups(OrderID=OrderID, BrokerID=BrokerID, ChangeBroker=ChangeBroker,
                                  InsertExecution=InsertExecution, Multiplier=Multiplier,
                                  ExecutedQuantity=ExecutedQuantity, ExecutionPrice=ExecutionPrice,
                                  NothingDone=NothingDone, fund_number=fund_number,
                                  ValueDate=ValueDate, user_id=user_id, user_name=user_name, fund_id=fund_id)
        return jsonify(response), 200
    except Exception as e:
        print(str(e))
        return jsonify({"Error": str(e)}), 400


@app.route('/OrderReceivedOnTradingDesk', methods=['GET'])
def OrderReceivedOnTradingDeskApi():
    """
    OrderReceivedOnTradingDeskApi process api which set order status Working suggestion.

    """
    try:
        order_id = request.args['orderId']
        Username = request.args['username']
        response = OrderReceivedOnTradingDesk(OrderID=order_id, Username=Username)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/approving_order', methods=['PUT'])
def Approving_order():
    """
    Approving order stage api.
    """
    try:
        req_data = request.get_json()
        order_id = req_data['orderId']
        Username = req_data['username']
        user_id = req_data['user_id']
        response = OrdersApproving(order_id=order_id, UserId=user_id, Username=Username)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/rejecting_order', methods=['POST'])
def Reject_order():
    """
    Rejecting order stage api.

    """
    try:
        req_data = request.get_json()
        order_id = req_data['orderId']
        Username = req_data['username']
        user_id = req_data['user_id']
        response = OrdersRejecting(order_id=order_id, UserId=user_id, Username=Username)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/cancel_order', methods=['POST'])
def Cancel_order():
    """
    Cancel order stage api.

    """
    try:
        req_data = request.get_json()
        order_id = req_data['orderId']
        Username = req_data['username']
        response = OrdersCancel(order_id=order_id,Username=Username)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/fx_hedging', methods=['POST'])
def fx_hedging_api():
    try:
        req_data = request.get_json()
        FundID = req_data['fundId']
        FundCode = req_data['fundCode']
        curncy1 = req_data['curncy1']
        curncy2 = req_data['curncy2']
        side = req_data['side']
        amount = req_data['amount']
        amountCounterValue = req_data['amountCounterValue']
        bbg_PxLast = req_data['bbg_PxLast']
        tradeDate = req_data['tradeDate']
        ValueDate = req_data['valueDate']
        executionPrice = req_data['executionPrice']
        swap = req_data['swap']
        hedgingPurposes = req_data['hedgingPurposes']
        userId = req_data['userId']
        EnableSwap = req_data['EnableSwap']
        Username = req_data['userName']
        fundName = req_data['fundName']
        Limit = 0  # ?

        response = fx_hedging(FundID=FundID, FundCode=FundCode, Amount=amount, EnableSwap=EnableSwap,
                              ValueDate=ValueDate,userId=userId, Curncy1=curncy1, Curncy2=curncy2,
                              TradeDate=tradeDate, Limit=Limit, DB_LastPrice=bbg_PxLast, ExecutionPrice=executionPrice,
                              HedgingPurposes=hedgingPurposes,side=side,Username=Username,fundName=fundName)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/documents', methods=['GET'])
def get_documents():
    """
    display developer doc api.

    """
    try:
        my_file = r"C:\Users\Aress\Projects\_build\html\index.html"
        return webbrowser.open(my_file)
    except Exception as e:
        return jsonify(error=str(e), msg='Document not available for now, please try after sometimes..!!'), 400


@app.route('/get_currency_record', methods=['POST'])
def get_currency_record():
    try:
        req_data = request.get_json()
        curncy1 = req_data['curncy1']
        curncy2 = req_data['curncy2']
        response = currency_calculation(curncy1=curncy1, curncy2=curncy2)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


@app.route('/disable_connection', methods=['POST'])
def disable_connection():
    try:
        req_data = request.get_json()
        id_dis = req_data['id_dis']
        UserId = req_data['UserId']
        response = Disable_connection(id_dis=id_dis, UserId=UserId)
        return jsonify({"Result": response}), 200
    except Exception as e:
        return jsonify(error=str(e)), 400


if __name__ == "__main__":
    app.run(host="172.16.5.21", port=5024, threaded=True)
    # app.run(host="localhost", port=5010, threaded=True)
