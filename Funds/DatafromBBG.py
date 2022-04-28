import requests
import xml.etree.ElementTree as ET


def findbyBBGws01(Ticker):
    """
    In this function, BBG workstation api get triggered to collect the data.

    :param Ticker: TickerISIN
    :return:  values list.
    """
    # Ticker = 'AAPL US Equity'
    print('findbyBBGws01')
    print('Ticker: ', Ticker)
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
            TICKER = att.find('TICKER').text  ###ticker    0
            print('TICKER: ', TICKER)
            values.append(TICKER)
        except:
            TICKER = 'Null'
            values.append(TICKER)
        try:
            PX_LAST = att.find('PX_LAST').text  ###price      1
            print('PX_LAST: ', PX_LAST)
            values.append(PX_LAST)
        except:
            PX_LAST = 0
            values.append(PX_LAST)
        try:
            SECURITY_NAME = att.find('SECURITY_NAME').text  ###SECURITY_NAME 2
            print('SECURITY_NAME: ', SECURITY_NAME)
            values.append(SECURITY_NAME)
        except:
            SECURITY_NAME = 'Null'
            values.append(SECURITY_NAME)
        try:
            SECURITY_TYP = att.find('SECURITY_TYP').text  ##SECURITY_TYP   3
            print('SECURITY_TYP: ', SECURITY_TYP)
            values.append(SECURITY_TYP)
        except:
            SECURITY_TYP = 'Null'
            values.append(SECURITY_TYP)
        try:
            CRNCY = att.find('CRNCY').text  ##CRNCY          4
            print('CRNCY: ', CRNCY)
            values.append(CRNCY)
        except:
            CRNCY = 'Null'
            values.append(CRNCY)
        try:
            ID_ISIN = att.find('ID_ISIN').text  ## isin           5
            print('ID_ISIN: ', ID_ISIN)
            values.append(ID_ISIN)
        except:
            ID_ISIN = 'Null'
            values.append(ID_ISIN)
        try:
            COUNTRY_FULL_NAME = att.find('COUNTRY_FULL_NAME').text  ##COUNTRY_FULL_NAME  6
            print('COUNTRY_FULL_NAME: ', COUNTRY_FULL_NAME)
            values.append(COUNTRY_FULL_NAME)
        except:
            COUNTRY_FULL_NAME = 'Null'
            values.append(COUNTRY_FULL_NAME)
        try:
            SETTLE_DT = att.find('SETTLE_DT').text  ##SETTLE_DT          7
            print('SETTLE_Date: ', SETTLE_DT)
            values.append(SETTLE_DT)
        except:
            SETTLE_DT = 'Null'
            values.append(SETTLE_DT)
        try:
            CNTRY_OF_RISK = att.find('CNTRY_OF_RISK').text  ##CNTRY_OF_RISK   8
            print('CNTRY_OF_RISK: ', CNTRY_OF_RISK)
            values.append(CNTRY_OF_RISK)
        except:
            CNTRY_OF_RISK = 'Null'
            values.append(CNTRY_OF_RISK)
        try:
            DELTA = att.find('DELTA').text  ##DELTA  9
            print('DELTA: ', DELTA)
            values.append(DELTA)
        except:
            DELTA = 1
            values.append(DELTA)
        try:
            FUT_CONT_SIZE = att.find('FUT_CONT_SIZE').text  # FUT_CONT_SIZE    10
            print('FUT_CONT_SIZE: ', FUT_CONT_SIZE)
            values.append(FUT_CONT_SIZE)
        except:
            FUT_CONT_SIZE = 0
            values.append(FUT_CONT_SIZE)
        try:
            INDUSTRY_SECTOR = att.find('INDUSTRY_SECTOR').text  # INDUSTRY_SECTOR  12
            print('INDUSTRY_SECTOR: ', INDUSTRY_SECTOR)
            values.append(INDUSTRY_SECTOR)
        except:
            INDUSTRY_SECTOR = 'Null'
            values.append(INDUSTRY_SECTOR)
        try:
            ID_BB_ULTIMATE_PARENT_CO = att.find('ID_BB_ULTIMATE_PARENT_CO').text  # parentcompany id  13
            print('ID_BB_ULTIMATE_PARENT_CO: ', ID_BB_ULTIMATE_PARENT_CO)
            values.append(ID_BB_ULTIMATE_PARENT_CO)
        except:
            ID_BB_ULTIMATE_PARENT_CO = 0
            values.append(ID_BB_ULTIMATE_PARENT_CO)
        try:
            REGISTERED_COUNTRY_LOCATION = att.find('REGISTERED_COUNTRY_LOCATION').text  ##reg_cntry_loc 14
            print('REGISTERED_COUNTRY_LOCATION: ', REGISTERED_COUNTRY_LOCATION)
            values.append(REGISTERED_COUNTRY_LOCATION)
        except:
            REGISTERED_COUNTRY_LOCATION = 'Null'
            values.append(REGISTERED_COUNTRY_LOCATION)
        try:
            EQY_SH_OUT = att.find('EQY_SH_OUT').text  # issue oustanding value 15
            print('EQY_SH_OUT: ', EQY_SH_OUT)
            values.append(EQY_SH_OUT)
        except:
            EQY_SH_OUT = 0
            values.append(EQY_SH_OUT)
        try:
            COUNTRY_OF_LARGEST_REVENUE = att.find('COUNTRY_OF_LARGEST_REVENUE').text  ##cntry_largeset_rev  16
            print('COUNTRY_OF_LARGEST_REVENUE: ', COUNTRY_OF_LARGEST_REVENUE)
            values.append(COUNTRY_OF_LARGEST_REVENUE)
        except:
            COUNTRY_OF_LARGEST_REVENUE = 'Null'
            values.append(COUNTRY_OF_LARGEST_REVENUE)
        try:
            UNDERLYING_ISIN = att.find('UNDERLYING_ISIN').text  # underlying isin  18
            print('UNDERLYING_ISIN: ', UNDERLYING_ISIN)
            values.append(UNDERLYING_ISIN)
        except:
            UNDERLYING_ISIN = 'Null'
            values.append(UNDERLYING_ISIN)
        try:
            AMT_OUTSTANDING = att.find('AMT_OUTSTANDING').text  # AMT_OUTSTANDING value 19
            print('AMT_OUTSTANDING: ', AMT_OUTSTANDING)
            values.append(AMT_OUTSTANDING)
        except:
            AMT_OUTSTANDING = 0
            values.append(AMT_OUTSTANDING)
        try:
            VOLUME_AVG_20D = att.find('VOLUME_AVG_20D').text  # VOLUME_AVG_20D value  20
            print('VOLUME_AVG_20D: ', VOLUME_AVG_20D)
            values.append(VOLUME_AVG_20D)
        except:
            VOLUME_AVG_20D = 0
            values.append(VOLUME_AVG_20D)
        try:
            OPT_CONT_SIZE = att.find('OPT_CONT_SIZE').text  # OPT_CONT_SIZE value  21
            print('OPT_CONT_SIZE: ', OPT_CONT_SIZE)
            values.append(OPT_CONT_SIZE)
        except:
            OPT_CONT_SIZE = 0
            values.append(OPT_CONT_SIZE)
        try:
            ISSUER_INDUSTRY = att.find('ISSUER_INDUSTRY').text  # ISSUER_INDUSTRY value  22
            print('ISSUER_INDUSTRY: ', ISSUER_INDUSTRY)
            values.append(ISSUER_INDUSTRY)
        except:
            ISSUER_INDUSTRY = 'Null'
            values.append(ISSUER_INDUSTRY)
        try:
            CAPITAL_CONTINGENT_SECURITY = att.find(
                'CAPITAL_CONTINGENT_SECURITY').text  # CAPITAL_CONTINGENT_SECURITY value 23
            print('CAPITAL_CONTINGENT_SECURITY: ', CAPITAL_CONTINGENT_SECURITY)
            values.append(CAPITAL_CONTINGENT_SECURITY)
        except:
            CAPITAL_CONTINGENT_SECURITY = 'Null'
            values.append(CAPITAL_CONTINGENT_SECURITY)
        #########################################################################
        try:
            COUNTRY = att.find('COUNTRY').text  # COUNTRY value 24
            print('COUNTRY: ', COUNTRY)
            values.append(COUNTRY)
        except:
            COUNTRY = 'Null'
            values.append(COUNTRY)
        try:
            COUNTRY_ISO = att.find('COUNTRY_ISO').text  # COUNTRY_ISO value 25
            print('COUNTRY_ISO: ', COUNTRY_ISO)
            values.append(COUNTRY_ISO)
        except:
            COUNTRY_ISO = 'Null'
            values.append(COUNTRY_ISO)
        try:
            CUR_MKT_CAP = att.find('CUR_MKT_CAP').text  # CUR_MKT_CAP value 26
            print('CUR_MKT_CAP: ', CUR_MKT_CAP)
            values.append(CUR_MKT_CAP)
        except:
            CUR_MKT_CAP = 0
            values.append(CUR_MKT_CAP)
        try:
            PARENT_TICKER_EXCHANGE = att.find('PARENT_TICKER_EXCHANGE').text  # PARENT_TICKER_EXCHANGE value 27
            print('PARENT_TICKER_EXCHANGE: ', PARENT_TICKER_EXCHANGE)
            values.append(PARENT_TICKER_EXCHANGE)
        except:
            PARENT_TICKER_EXCHANGE = 'Null'
            values.append(PARENT_TICKER_EXCHANGE)
        try:
            CHG_PCT_YTD = att.find('CHG_PCT_YTD').text  # CHG_PCT_YTD value 28
            print('CHG_PCT_YTD: ', CHG_PCT_YTD)
            values.append(CHG_PCT_YTD)
        except:
            CHG_PCT_YTD = 0
            values.append(CHG_PCT_YTD)
        try:
            CPN = att.find('CPN').text  # CPN value 29  coupon
            print('CPN: ', CPN)
            values.append(CPN)
        except:
            CPN = 0
            values.append(CPN)
        try:
            INT_ACC = att.find('INT_ACC').text  # INT_ACC value 30  accruedinterest
            print('INT_ACC: ', INT_ACC)
            values.append(INT_ACC)
        except:
            INT_ACC = 0
            values.append(INT_ACC)
        try:
            EQY_ALPHA = att.find('EQY_ALPHA').text  # EQY_ALPHA value 31
            print('EQY_ALPHA: ', EQY_ALPHA)
            values.append(EQY_ALPHA)
        except:
            EQY_ALPHA = 0
            values.append(EQY_ALPHA)
        try:
            EQY_BETA = att.find('EQY_BETA').text  # EQY_BETA value 32
            print('EQY_BETA: ', EQY_BETA)
            values.append(EQY_BETA)
        except:
            EQY_BETA = 0
            values.append(EQY_BETA)
        try:
            EQY_PRIM_EXCH_SHRT = att.find('EQY_PRIM_EXCH_SHRT').text  # EQY_PRIM_EXCH_SHRT value 33
            print('EQY_PRIM_EXCH_SHRT: ', EQY_PRIM_EXCH_SHRT)
            values.append(EQY_PRIM_EXCH_SHRT)
        except:
            EQY_PRIM_EXCH_SHRT = 'Null'
            values.append(EQY_PRIM_EXCH_SHRT)
        try:
            ID_BB_ULTIMATE_PARENT_CO_NAME = att.find(
                'ID_BB_ULTIMATE_PARENT_CO_NAME').text  # ID_BB_ULTIMATE_PARENT_CO_NAME value 34
            print('ID_BB_ULTIMATE_PARENT_CO_NAME: ', ID_BB_ULTIMATE_PARENT_CO_NAME)
            values.append(ID_BB_ULTIMATE_PARENT_CO_NAME)
        except:
            ID_BB_ULTIMATE_PARENT_CO_NAME = 'Null'
            values.append(ID_BB_ULTIMATE_PARENT_CO_NAME)
        try:
            INDUSTRY_GROUP = att.find('INDUSTRY_GROUP').text  # INDUSTRY_GROUP value 35
            print('INDUSTRY_GROUP: ', INDUSTRY_GROUP)
            values.append(INDUSTRY_GROUP)
        except:
            INDUSTRY_GROUP = 'Null'
            values.append(INDUSTRY_GROUP)
        try:
            SHORT_NAME = att.find('SHORT_NAME').text  # SHORT_NAME value 36
            print('SHORT_NAME: ', SHORT_NAME)
            values.append(SHORT_NAME)
        except:
            SHORT_NAME = 'Null'
            values.append(SHORT_NAME)
        try:
            MARKET_SECTOR_DES = att.find('MARKET_SECTOR_DES').text  # MARKET_SECTOR_DES value 37
            print('MARKET_SECTOR_DES: ', MARKET_SECTOR_DES)
            values.append(MARKET_SECTOR_DES)
        except:
            MARKET_SECTOR_DES = 0
            values.append(MARKET_SECTOR_DES)
        try:
            MATURITY = att.find('MATURITY').text  # MATURITY date value 38
            print('MATURITY: ', MATURITY)
            values.append(MATURITY)
        except:
            MATURITY = 'Null'
            values.append(MATURITY)
        try:
            NAME = att.find('NAME').text  # NAME value 39
            print('NAME: ', NAME)
            values.append(NAME)
        except:
            NAME = 'Null'
            values.append(NAME)
        try:
            OPT_PUT_CALL = att.find('OPT_PUT_CALL').text  # OPT_PUT_CALL value 40
            print('OPT_PUT_CALL: ', OPT_PUT_CALL)
            values.append(OPT_PUT_CALL)
        except:
            OPT_PUT_CALL = 0
            values.append(OPT_PUT_CALL)
        try:
            OPT_STRIKE_PX = att.find('OPT_STRIKE_PX').text  # OPT_STRIKE_PX value 41
            print('OPT_STRIKE_PX: ', OPT_STRIKE_PX)
            values.append(OPT_STRIKE_PX)
        except:
            OPT_STRIKE_PX = 0
            values.append(OPT_STRIKE_PX)
        try:
            PAYMENT_RANK = att.find('PAYMENT_RANK').text  # PAYMENT_RANK value 42
            print('PAYMENT_RANK: ', PAYMENT_RANK)
            values.append(PAYMENT_RANK)
        except:
            PAYMENT_RANK = 'Null'
            values.append(PAYMENT_RANK)
        try:
            VOLATILITY_30D = att.find('VOLATILITY_30D').text  # VOLATILITY_30D value 43
            print('VOLATILITY_30D: ', VOLATILITY_30D)
            values.append(VOLATILITY_30D)
        except:
            VOLATILITY_30D = 0
            values.append(VOLATILITY_30D)
        try:
            VOLUME_AVG_30D = att.find('VOLUME_AVG_30D').text  # VOLUME_AVG_30D value  44
            print('VOLUME_AVG_30D: ', VOLUME_AVG_30D)
            values.append(VOLUME_AVG_30D)
        except:
            VOLUME_AVG_30D = 0
            values.append(VOLUME_AVG_30D)
        try:
            ERROR = att.find('ERROR').text  ###ERROR      value  44
            print('ERROR: ', ERROR)
            values.append(ERROR)
        except:
            ERROR = ''
            values.append(ERROR)

        ############################################## Extra added for api ######################

        try:
            TICKER_AND_EXCH_CODE = att.find('TICKER_AND_EXCH_CODE').text
            print('TICKER_AND_EXCH_CODE: ', TICKER_AND_EXCH_CODE)
            values.append(TICKER_AND_EXCH_CODE)
        except:
            TICKER_AND_EXCH_CODE = ''
            values.append(TICKER_AND_EXCH_CODE)

        try:
            TickerIsinUpperCase = att.find('TickerIsinUpperCase').text
            print('TickerIsinUpperCase: ', TickerIsinUpperCase)
            values.append(TickerIsinUpperCase)
        except:
            TickerIsinUpperCase = Ticker.upper()
            values.append(TickerIsinUpperCase)

        try:
            MaturityDate = att.find('MaturityDate').text
            print('MaturityDate: ', MaturityDate)
            values.append(MaturityDate)
        except:
            MaturityDate = ''
            values.append(MaturityDate)

        try:
            STARTTIME = att.find('STARTTIME').text
            print('STARTTIME: ', STARTTIME)
            values.append(STARTTIME)
        except:
            STARTTIME = ''
            values.append(STARTTIME)

        try:
            EndTime = att.find('EndTime').text
            print('EndTime: ', EndTime)
            values.append(EndTime)
        except:
            EndTime = ''
            values.append(EndTime)

        try:
            UnderlayPrcie = att.find('UnderlayPrcie').text
            print('UnderlayPrcie: ', UnderlayPrcie)
            values.append(UnderlayPrcie)
        except:
            UnderlayPrcie = 0
            values.append(UnderlayPrcie)

        try:
            lot_size = att.find('lot_size').text
            print('lot_size: ', lot_size)
            values.append(lot_size)
        except:
            lot_size = ''
            values.append(lot_size)

        try:
            Maturity_Date_Estimated = att.find('Maturity_Date_Estimated').text
            print('Maturity_Date_Estimated: ', Maturity_Date_Estimated)
            values.append(Maturity_Date_Estimated)
        except:
            Maturity_Date_Estimated = ''
            values.append(Maturity_Date_Estimated)

        try:
            Next_Call_Dt = att.find('Next_Call_Dt').text
            print('Next_Call_Dt: ', Next_Call_Dt)
            values.append(Next_Call_Dt)
        except:
            Next_Call_Dt = ''
            values.append(Next_Call_Dt)

    return values


# findbyBBGws01('AAPL US Equity')

# @app.route('/getbbgvalues', methods=['POST'])
# def bbgvalue():
#     from getbbg import findbyBBG
#     req_data = request.get_json()
#     Ticker = req_data.get("tickerid")
#     datavalues = findbyBBG(Ticker)
#     datakeys = ['PX_LAST','SECURITY_NAME','SECURITY_TYP','CRNCY','ID_ISIN','COUNTRY_FULL_NAME','SETTLE_Date']
#     bbgdata= dict(zip(datakeys,datavalues))
#     return bbgdata
