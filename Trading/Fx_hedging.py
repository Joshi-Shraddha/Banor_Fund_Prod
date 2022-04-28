import requests
import xml.etree.ElementTree as ET
from Funds.configuration import database_dev_env, get_current_time, get_current_date,get_current_date_time
from Funds.email_send_service import Email_Send_ServiceEmail


def fx_CurrencyConvention(Currency_1, Currency_2):
    if Currency_1 == "EUR":
        Curr_1_Points = 10
    if Currency_1 == "GBP":
        Curr_1_Points = 9
    if Currency_1 == "AUD":
        Curr_1_Points = 8
    if Currency_1 == "NZD":
        Curr_1_Points = 7
    if Currency_1 == "USD":
        Curr_1_Points = 6

    if Currency_2 == "EUR":
        Curr_2_Points = 10
    if Currency_2 == "GBP":
        Curr_2_Points = 9
    if Currency_2 == "AUD":
        Curr_2_Points = 8
    if Currency_2 == "NZD":
        Curr_2_Points = 7
    if Currency_2 == "USD":
        Curr_2_Points = 6

    if Curr_1_Points > Curr_2_Points:
        FXTicker_Convention = ("% s %s" % ("%s%s" % (Currency_1, Currency_2), "Curncy"))
    elif Curr_2_Points > Curr_1_Points:
        FXTicker_Convention = ("% s %s" % ("%s%s" % (Currency_2, Currency_1), "Curncy"))
    else:
        pass
    return FXTicker_Convention


def Hex_To_Decimal(string):
    """
    :param string: input para.
    :return:  convert Hex to decimal
    """
    IntakeValue = string
    # IntakeValue = Index(string,"E",startIndex:0,searchFromEnd:False,ignoreCase:True)
    if IntakeValue < 0:
        Decimal = float(IntakeValue)
    else:
        Decimal = 0
    return Decimal


def bbg_call(Ticker):
    response = requests.get("http://172.16.20.90:8080/InstrumentInfo", params={"Code": Ticker})

    if response.status_code != 200:
        print('BGG is down!')
        return "BGG is down"
    tree = response.text
    mytree = ET.ElementTree(ET.fromstring(tree))
    root = mytree.getroot()

    if not root:
        return "BGG is down"

    values = []
    print("Getting BBG data...........")
    for att in root:
        try:
            SECURITY_NAME = att.find('SECURITY_NAME').text  ###SECURITY_NAME 2
            print('SECURITY_NAME: ', SECURITY_NAME)
            values.append(SECURITY_NAME)
        except:
            SECURITY_NAME = 'Null'
            values.append(SECURITY_NAME)

        try:
            CRNCY = att.find('CRNCY').text  ##CRNCY          4
            print('CRNCY: ', CRNCY)
            values.append(CRNCY)
        except:
            CRNCY = 'Null'
            values.append(CRNCY)

        try:
            COUNTRY_FULL_NAME = att.find('COUNTRY_FULL_NAME').text  ##COUNTRY_FULL_NAME  6
            print('COUNTRY_FULL_NAME: ', COUNTRY_FULL_NAME)
            values.append(COUNTRY_FULL_NAME)
        except:
            COUNTRY_FULL_NAME = 'Null'
            values.append(COUNTRY_FULL_NAME)

        try:
            SECURITY_TYP = att.find('SECURITY_TYP').text  ##SECURITY_TYP   3
            print('SECURITY_TYP: ', SECURITY_TYP)
            values.append(SECURITY_TYP)
        except:
            SECURITY_TYP = 'Null'
            values.append(SECURITY_TYP)

        try:
            SETTLE_DT = att.find('SETTLE_DT').text  ##SETTLE_DT          7
            print('SETTLE_Date: ', SETTLE_DT)
            values.append(SETTLE_DT)
        except:
            SETTLE_DT = 'Null'
            values.append(SETTLE_DT)

        try:
            PX_LAST = att.find('PX_LAST').text  ###price      1
            print('PX_LAST: ', PX_LAST)
            values.append(PX_LAST)
        except:
            PX_LAST = 0
            values.append(PX_LAST)
    return values


def get_rec_from_db(TickerISIN):
    conn = database_dev_env()
    cursor = conn.cursor()
    query = ("SELECT TOP (1) [ID],[DATE],[TICKER],[NAME],[PX_LAST],[SECURITY_TYP],[SETTLE_DT],[MARKET_SECOTR_DES],[CURRENCY1],[CURRENCY2],[TIME]"
             " FROM [outsys_prod_qa].[dbo].[OSUSR_BOL_CURRENCY] Where TICKER = '" + str(TickerISIN) + "' order by DATE  desc")
    cursor.execute(query)
    rec = cursor.fetchone()
    print(rec)
    values = []
    try:
        SECURITY_NAME = rec[3]  ###SECURITY_NAME 2
        print('SECURITY_NAME: ', SECURITY_NAME)
        values.append(SECURITY_NAME)
    except:
        SECURITY_NAME = 'Null'
        values.append(SECURITY_NAME)

    try:
        CRNCY = rec[8]
        print('CRNCY: ', CRNCY)
        values.append(CRNCY)
    except:
        CRNCY = 'Null'
        values.append(CRNCY)
    try:
        COUNTRY_FULL_NAME = rec[14]
        print('COUNTRY_FULL_NAME: ', COUNTRY_FULL_NAME)
        values.append(COUNTRY_FULL_NAME)
    except:
        COUNTRY_FULL_NAME = 'Null'
        values.append(COUNTRY_FULL_NAME)

    try:
        SECURITY_TYP = rec[5]  ##SECURITY_TYP   3
        print('SECURITY_TYP: ', SECURITY_TYP)
        values.append(SECURITY_TYP)
    except:
        SECURITY_TYP = 'Null'
        values.append(SECURITY_TYP)
    try:
        SETTLE_DT = rec[6]
        # date_format(SETTLE_DT)
        print('SETTLE_Date: ', SETTLE_DT)
        values.append(SETTLE_DT)
    except:
        SETTLE_DT = 'Null'
        values.append(SETTLE_DT)

    try:
        PX_LAST = rec[4]
        print('PX_LAST: ', PX_LAST)
        values.append(PX_LAST)
    except:
        PX_LAST = 0
        values.append(PX_LAST)

    return values


def Http_Server_wst01(sTickerISIN):
    TickerISIN = sTickerISIN
    if (TickerISIN == "" or TickerISIN == " " or TickerISIN == "  "
            or TickerISIN == " Equity" or TickerISIN == " Corp" or
            TickerISIN == " Index" or TickerISIN == " Govt" or
            TickerISIN.find(" Govt CORP", 0) >= 0 or
            TickerISIN.find(" Equity CORP", 0) >= 0 or
            len(TickerISIN) < 9):
        return None
    # values = findbyBBGws01(TickerISIN)
    try:
        values = bbg_call(TickerISIN)
    except:
        values = get_rec_from_db(TickerISIN)

    value_dict = {}
    if values != 'BBG Workstation Down':
        SecurityName = values[0].upper() if values[0] else values[0]
        CRNCY = values[1]
        COUNTRY_FULL_NAME = values[2]
        SECURITY_TYP = values[3]
        SETTLE_DT = values[4]
        PX_LAST = str(Hex_To_Decimal(values[5]))
        value_dict['SecurityName'] = SecurityName
        value_dict['Crncy'] = CRNCY
        value_dict['Country'] = COUNTRY_FULL_NAME
        value_dict['Security_typ'] = SECURITY_TYP
        value_dict['Px_Last'] = PX_LAST
        value_dict['Settle_Date'] = SETTLE_DT
    return value_dict


def fx_FundForwardCreation(FundID, FundSimpleCode, ForwardID, ExecutionPrice, Amount, Side, Currency1, Currency2,
                           TradeDate, ValueDate, BBG_PxLast_OnScreen, UserID, Swap, SwapID, HedgingPurposes, Limit,
                           LongLeg, ShortLeg,fundName):
    conn = database_dev_env()
    cursor = conn.cursor()
    FXTicker_Convention = fx_CurrencyConvention(Currency_1=Currency1, Currency_2=Currency2)
    Http_Server_wst01_repose = Http_Server_wst01(sTickerISIN=FXTicker_Convention)
    SecurityName = Http_Server_wst01_repose['SecurityName']
    Crncy = Http_Server_wst01_repose['Crncy']
    Country = Http_Server_wst01_repose['Country']
    Security_typ = Http_Server_wst01_repose['Security_typ']
    Settle_Date = Http_Server_wst01_repose['Settle_Date']
    Px_Last = Http_Server_wst01_repose['Px_Last']

    # SecurityName = 'EUR-USD X-RATE'
    # Crncy = 'SD CUR'
    # Country = 'Itely'
    # Security_typ = 'CROSS'
    # Settle_Date = '2021-10-27 00:00:00.000'
    # Px_Last = '1.16460000'

    Ticker_ISIN = FXTicker_Convention
    # BBG_PxLast_OnScreen = Http_Server_wst01_repose['Px_Last']
    BBG_PxLast_OnScreen = '1.16460000'

    ForwardID = 'NULL' if ForwardID is None else ForwardID
    FundID = 'NULL' if FundID is None else FundID
    select_q = (
            "SELECT [ENFORWARD].[ID] o0, [ENFORWARD].[CURNCY1] o1, [ENFORWARD].[CURNCY2] o2, [ENFORWARD].[SIDE] o3, [ENFORWARD].[AMOUNT] o4, [ENFORWARD].[AMOUNTCOUNTERVALUE] o5, [ENFORWARD].[TRADEDATE] o6, [ENFORWARD].[VALUEDATE] o7, [ENFORWARD].[EXECUTIONPRICE] o8, [ENFORWARD].[PROFITANDLOSS] o9, [ENFORWARD].[DATE] o10, [ENFORWARD].[FUNDID] o11, [ENFORWARD].[DB_LASTPRICE] o12, [ENFORWARD].[TICKER_ISIN] o13, [ENFORWARD].[PERFORMANCE] o14, [ENFORWARD].[EXECUTED] o15, [ENFORWARD].[EXPIRED] o16, [ENFORWARD].[WEIGHT] o17, [ENFORWARD].[FUNDSIMPLECODE] o18, [ENFORWARD].[TIMECREATION] o19, [ENFORWARD].[DATECREATION] o20, [ENFORWARD].[USERID] o21, [ENFORWARD].[ORDERID] o22, [ENFORWARD].[SWAP] o23, [ENFORWARD].[SWAPID] o24, [ENFORWARD].[HEDGINGPURPOSES] o25, [ENFORWARD].[ORDERCREATED] o26, [ENFORWARD].[BDL] o27, [ENFORWARD].[BNP] o28, [ENFORWARD].[BROKERSHORTCODE] o29, [ENFORWARD].[FUNDNAME] o30, [ENFORWARD].[FUNDSPECIFICCLASSNAME] o31, [ENFORWARD].[LIMIT] o32, [ENFORWARD].[BBG_SECURITY_NAME] o33, [ENFORWARD].[BBG_CRNCY] o34, [ENFORWARD].[BBG_COUNTRY] o35, [ENFORWARD].[BBG_SECURITYTYP] o36, [ENFORWARD].[BBG_SETTLEDATE] o37, [ENFORWARD].[BBG_PXLAST] o38, [ENFORWARD].[BBG_BASECRNCY] o39, [ENFORWARD].[LONGLEG] o40, [ENFORWARD].[SHORTLEG] o41, [ENFORWARD].[FXACCOUNTNAME] o42, [ENFORWARD].[CASA4FUND_FUNDNAME] o43, [ENFORWARD].[BNRBROKER] o44, [ENFORWARD].[CASA4FUNDS_BROKER] o45, [ENFORWARD].[ISIN] o46 "
            "FROM [outsys_prod_qa].DBO.[OSUSR_SKP_FORWARD] [ENFORWARD] "
            "WHERE ([ENFORWARD].[ID] = '" + str(ForwardID) + "') AND ([ENFORWARD].[FUNDID] = '" + str(
        FundID) + "')")
    print(select_q)
    cursor.execute(select_q)
    Forward = cursor.fetchall()
    if FundID is None:
        pass
    else:
        try:
            select2_q = (
                    "SELECT [ENFUNDS].[ID] o0, [ENFUNDS].[SIMPLENAME] o1, [ENFUNDS].[NAME] o2, [ENFUNDS].[TICKER] o3, [ENFUNDS].[CRNCY] o4, [ENFUNDS].[CLASS] o5, [ENFUNDS].[FUND_CODE] o6, [ENFUNDS].[ID_ISIN] o7, [ENFUNDS].[FRONT_LOAD_FEE] o8, [ENFUNDS].[BACK_LOAD_FEE] o9, [ENFUNDS].[MNG_FEE] o10, [ENFUNDS].[PERF_FEE] o11, [ENFUNDS].[MNG_FEETILLJUNE14] o12, [ENFUNDS].[PERC_FEETILLJUNE14] o13, [ENFUNDS].[COLLOCAMENTOUBS] o14, [ENFUNDS].[COLLOCAMENTOBANORSIM] o15, [ENFUNDS].[MINFIRSTTIME_INVESTMENT] o16, [ENFUNDS].[STRATEGY] o17, [ENFUNDS].[GEOFOCUSREGION] o18, [ENFUNDS].[ASSETCLASS] o19, [ENFUNDS].[INCEPTIONDATE] o20, [ENFUNDS].[SICAV] o21, [ENFUNDS].[BENCHMARK] o22, [ENFUNDS].[DETAILSCUSTOMBENCHMARKS] o23, [ENFUNDS].[SETTLEMENT] o24, [ENFUNDS].[MORNINGSTARCATEGORY] o25, [ENFUNDS].[CUTOFF] o26, [ENFUNDS].[DEALINGPERIOD] o27, [ENFUNDS].[ADVISOR] o28, [ENFUNDS].[FX_BDL_ACCOUNT] o29, [ENFUNDS].[FX_BNP_ACCOUNT] o30, [ENFUNDS].[BASECURRENCY] o31, [ENFUNDS].[CASA4FUND_FUNDNAME] o32, [ENFUNDS].[JPMORGANACCOUNT] o33, [ENFUNDS].[UBSACCOUNT] o34 "
                    "FROM [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS] [ENFUNDS] WHERE ([ENFUNDS].[ID] = " + str(
                FundID) + ") ORDER BY [ENFUNDS].[NAME] ASC ")
            cursor.execute(select2_q)
            GetFundById = cursor.fetchall()
            if GetFundById[0][21].find("ARISTEA", 0):
                BNP = True
                BrokerShortCode = "BSSL"
                BnrBroker = "BNP"
                Casa4Funds_Broker = "BNP LUX"

                for i, _ in enumerate(GetFundById):
                    gfbi = GetFundById[i]
                    FundSpecificName = gfbi[2]
                    Fx_BDL_Account = gfbi[25]
                    FxAccountName = gfbi[29] if Fx_BDL_Account != "" else gfbi[30]
                    Casa4Funds_FundNAme = gfbi[32]
                    ISIN = gfbi[7]
                    BrokerID = int(745)  # check it is same or get vary
            elif GetFundById[0][21].find("BANOR", 0):
                BDL = True
                BrokerShortCode = "BDLX"
                BnrBroker = "BDL"
                Casa4Funds_Broker = "BDL"

            q = ("BEGIN  SELECT DISTINCT( [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS].[SIMPLENAME]),  [outsys_prod_qa].DBO.["
                 "OSUSR_38P_FUNDS].[FUND_CODE],  [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS].[FX_BDL_ACCOUNT],  "
                 "[outsys_prod_qa].DBO.[OSUSR_38P_FUNDS].[FX_BNP_ACCOUNT],  [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS].["
                 "CASA4FUND_FUNDNAME],  [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS].[ID_ISIN],  [outsys_prod_qa].DBO.["
                 "OSUSR_38P_FUNDS].[SICAV]  FROM  [outsys_prod_qa].DBO.[OSUSR_38P_FUNDS] "
                 "WHERE [FUND_CODE]  = '" + str(FundSimpleCode) + "' END")
            conn.execcute(q)
            FundList = conn.fetchall()
            if GetFundById[0][21].find("BANOR", 0) >= 0:
                BDL = True
                BrokerShortCode = "BDLX"
                BnrBroker = "BDL"
                Casa4Funds_Broker = "BDL"
                ISIN = "MAIN ACCOUNT"
                BDL = True
                BrokerID = int(53)
                # FundSimpleName =
                # FxAccountName =
                # Casa4Funds_FundNAme

            else:
                BNP = True
                BrokerShortCode = "BSSL"
                BnrBroker = "BNP"
                Casa4Funds_Broker = "BNP LUX"

                ISIN = "MAIN ACCOUNT"
                BDL = True
                BrokerID = int(745)
                # FundSimpleName =
                # FxAccountName =
                # Casa4Funds_FundNAme
        except:
            BrokerShortCode = "BDLX"
            BnrBroker = "BDL"
            Casa4Funds_Broker = "BDL"
            ISIN = "MAIN ACCOUNT"
            BDL = True
            BNP = True
            BrokerID = int(53)
            FundSpecificName = ""
            Fx_BDL_Account = ""
            FxAccountName = ""
            Casa4Funds_FundNAme = ""
            FundSimpleName = fundName
    if Forward:
        TimeCreation = get_current_time()
        DateCreation = get_current_date()

    if ExecutionPrice != 0 and BBG_PxLast_OnScreen != 0:
        if Side == "BUY":
            Performance = ((float(BBG_PxLast_OnScreen) / ExecutionPrice) - 1)
        else:
            Performance = ((float(BBG_PxLast_OnScreen) / ExecutionPrice) - 1) * -1

    if ExecutionPrice != 0 and BBG_PxLast_OnScreen != 0:
        pL = Amount * Performance
        Performance = Performance
        ProfitAndLoss = pL

    Amount = Amount
    Side = Side
    Curncy1 = Currency1
    Curncy2 = Currency2
    TradeDate = TradeDate
    ValueDate = ValueDate
    ExecutionPrice = ExecutionPrice
    FundID = FundID
    DB_LastPrice = BBG_PxLast_OnScreen
    Ticker_ISIN = Ticker_ISIN
    Expired = False
    Executed = True
    AmountCounterValue = Amount * ExecutionPrice
    FundSimpleCode = FundSimpleCode
    UserID = UserID
    Swap = Swap
    HedgingPurposes = HedgingPurposes
    FundName = FundSimpleName
    FundSpecificClassName = FundSpecificName
    BDL = BDL
    BNP = BNP
    BrokerShortCode = BrokerShortCode
    bbg_Security_Name = SecurityName
    bbg_Crncy = Crncy
    bbg_Country = Country
    bbg_SecurityTyp = Security_typ
    bbg_SettleDate = Settle_Date
    bbg_PxLast = Px_Last
    Limit = Limit
    LongLeg = LongLeg
    ShortLeg = ShortLeg
    FxAccountName = FxAccountName
    Casa4Fund_FundName = Casa4Funds_FundNAme
    BnrBroker = BnrBroker
    Casa4Funds_Broker = Casa4Funds_Broker
    ISIN = ISIN
    date = get_current_date_time()

    if Forward:

        sql = "UPDATE [outsys_prod_qa].DBO.[OSUSR_SKP_FORWARD] SET Amount = %s, Side = %s, Curncy1 = %s, Curncy2 = %s,TradeDate = %s," \
              "ValueDate=%s,ExecutionPrice=%s,FundID=%s,DB_LastPrice=%s,Ticker_ISIN=%s,Expired=%s,Executed=%s," \
              "AmountCounterValue=%s,FundSimpleCode=%s,UserID=%s,Swap=%s,SwapID=%s,HedgingPurposes=%s,FundName=%s,FundSpecificClassName=%s," \
              "BDL=%s,BNP=%s,BrokerShortCode=%s,bbg_Security_Name=%s,bbg_Crncy=%s,bbg_Country=%s,Security_typ=%s,bbg_SettleDate=%s," \
              "bbg_PxLast=%s,Limit=%s,LongLeg=%s,ShortLeg=%s,FxAccountName=%s,Casa4Fund_FundName=%s,BnrBroker=%s,Casa4Funds_Broker=%s,ISIN=%s,TradeDate where [ID] = '" + str(
            ForwardID) + "') AND ([FUNDID] = '" + str(
            FundID) + "')"
        val = (Amount, Side, Curncy1, Curncy2, TradeDate, ValueDate, ExecutionPrice, FundID, DB_LastPrice,
               Ticker_ISIN, Expired, Executed, AmountCounterValue, FundSimpleCode, UserID, Swap, SwapID,
               HedgingPurposes, FundName, FundSpecificClassName, BDL, BNP, BrokerShortCode, bbg_Security_Name,
               bbg_Crncy, bbg_Country, Security_typ, bbg_SettleDate, bbg_PxLast, Limit, LongLeg, ShortLeg,
               FxAccountName, Casa4Fund_FundName, BnrBroker, Casa4Funds_Broker, ISIN)

        cursor.execute(sql, val)
        conn.commit()
        pcForwardIDout = ForwardID
    else:
        try:
            Createfwd = (
                    "BEGIN TRANSACTION \n INSERT INTO [outsys_prod_qa].DBO.[OSUSR_SKP_FORWARD](Amount, Side, Curncy1, Curncy2, "
                    "ExecutionPrice,FundID,DB_LastPrice,Ticker_ISIN,Expired,Executed,AmountCounterValue,FundSimpleCode,UserID, "
                    "Swap,SwapID,HedgingPurposes,FundName,FundSpecificClassName,BDL,BNP,BrokerShortCode,bbg_Security_Name,"
                    "bbg_Crncy,bbg_Country,bbg_SettleDate,bbg_PxLast,Limit,LongLeg,ShortLeg,FxAccountName,"
                    "Casa4Fund_FundName,BnrBroker,Casa4Funds_Broker,ISIN,ValueDate,TradeDate,DATE)values(" + str(
                Amount) + ",'" + str(Side) + "','" + str(Curncy1) + "','" + str(Curncy2) + "','" + str(ExecutionPrice) + "',"+str(FundID)+",'" + str(
                DB_LastPrice) + "','" + str(Ticker_ISIN) + "','" + str(Expired) + "','" + str(Executed) + "','" + str(
                AmountCounterValue) + "','" + str(FundSimpleCode) + "','" + str(UserID) + "','" + str(
                Swap) + "','" + str(SwapID) + "','" + str(HedgingPurposes) + "','" + str(FundName) + "','" + str(
                FundSpecificClassName) + "','" + str(BDL) + "','" + str(BNP) + "','" + str(
                BrokerShortCode) + "','" + str(bbg_Security_Name) + "','" + str(bbg_Crncy) + "','" + str(
                bbg_Country) + "','" + str(bbg_SettleDate) + "','" + str(bbg_PxLast) + "','" + str(Limit) + "','" + str(
                LongLeg) + "','" + str(ShortLeg) + "','" + str(FxAccountName) + "','" + str(
                Casa4Fund_FundName) + "','" + str(BnrBroker) + "','" + str(Casa4Funds_Broker) + "','" + str(
                ISIN) + "','"+str(ValueDate)+"','"+str(TradeDate)+"','"+str(date)+"')")
            print(Createfwd)
            cursor.execute(Createfwd)
            conn.commit()
            pcForwardIDout = cursor.lastrowid
            return pcForwardIDout
        except Exception as error:
            print("Database insertion Failed !: {}".format(error))
            conn.rollback()
            return error
        finally:
            conn.close()


def fx_hedging(FundID, FundCode, Amount, ValueDate, userId, EnableSwap, Curncy1, Curncy2, TradeDate, Limit,
               DB_LastPrice, ExecutionPrice, HedgingPurposes, side, Username,fundName):
    conn = database_dev_env()
    cursor = conn.cursor()
    DB_LastPriceBox = True
    pcForward_Amount = True
    pcForward_AmountCounterValue = True
    pcForward_Curncy1 = True
    pcForward_Curncy2 = True
    pcForward_ExecutionPrice = True
    pcForward_ProfitAndLoss = True
    pcForward_TradeDate = True
    pcForward_ValueDate = True
    MissingSideShortLeg = False
    MissingSideLongLeg = False
    DoubleFundSelection = False
    ForwardID = 0
    SideLongLeg = side
    SideShortLeg = side

    if FundID is None and FundCode == 0:
        return None
    else:
        if ValueDate is None:
            return None
        else:
            get_user_role = (
                    "select * from [outsys_prod_qa].DBO.[ossys_User_Role] where USER_ID = '" + str(userId) + "' ")
            cursor.execute(get_user_role)
            user_rec = cursor.fetchall()
            if user_rec:
                pass
            else:
                Email_Send_ServiceEmail(Username=Username)

        if FundCode != 0 and FundID is not None:
            DoubleFundSelection = True
        else:
            if EnableSwap:
                if SideLongLeg == "":
                    MissingSideLongLeg = True
                    return None

            if SideShortLeg == "":
                MissingSideShortLeg = True
                return None
            else:
                if Amount == 0:
                    pcForward_Amount = False
                    return None
                else:
                    if isinstance(Curncy1, str) == "":
                        pcForward_Curncy1 = False
                        return "Invalid Curncy1 type "
                    else:
                        if isinstance(Curncy2, str) == "":
                            pcForward_Curncy2 = False
                            return "Invalid Curncy2 type "
                        else:
                            if isinstance(ValueDate, str) is None:
                                pcForward_ValueDate = False
                                return "Invalid ValueDate type"
                            else:
                                sql_q = ("BEGIN  SELECT MAX( [outsys_prod_qa].DBO.[OSUSR_SKP_FORWARD].[SWAPID]) "
                                         "FROM  [outsys_prod_qa].DBO.[OSUSR_SKP_FORWARD] END")
                                print(sql_q)
                                cursor.execute(sql_q)
                                SQL1 = cursor.fetchall()
                                # From SQL1 take swap-id and increment it.
                                SwapID = SQL1[0][0] + 1

                                Loading = True
                                if EnableSwap:
                                    fx_response = fx_FundForwardCreation(FundID=FundID, FundSimpleCode=FundCode, ForwardID=ForwardID,
                                                           ExecutionPrice=ExecutionPrice, Amount=Amount,
                                                           Side=SideLongLeg, Currency1=Curncy1, Currency2=Curncy2,
                                                           TradeDate=TradeDate, ValueDate=ValueDate,
                                                           BBG_PxLast_OnScreen=DB_LastPrice, UserID=userId,
                                                           Swap=EnableSwap, SwapID=SwapID,
                                                           HedgingPurposes=HedgingPurposes, Limit=Limit, LongLeg=True,
                                                           ShortLeg=False,fundName=fundName)
                                else:
                                    fx_response = fx_FundForwardCreation(FundID=FundID, FundSimpleCode=FundCode, ForwardID=ForwardID,
                                                           ExecutionPrice=ExecutionPrice, Amount=Amount,
                                                           Side=SideShortLeg, Currency1=Curncy1, Currency2=Curncy2,
                                                           TradeDate=TradeDate, ValueDate=ValueDate,
                                                           BBG_PxLast_OnScreen=DB_LastPrice, UserID=userId,
                                                           Swap=EnableSwap, SwapID=SwapID,
                                                           HedgingPurposes=HedgingPurposes, Limit=Limit, LongLeg=False,
                                                           ShortLeg=True,fundName=fundName)
    return str(fx_response)


def currency_calculation(curncy1,curncy2):
    conn = database_dev_env()
    cursor = conn.cursor()
    query = ("select top(1) * FROM [outsys_prod_qa].[dbo].[OSUSR_BOL_CURRENCY] "
             "where CURRENCY1= '"+str(curncy1)+"' and  CURRENCY2 = '"+str(curncy2)+"'  order by DATE desc")
    cursor.execute(query)
    rec = cursor.fetchone()
    #print(rec)
    px_last = rec[4]
    return str(px_last)