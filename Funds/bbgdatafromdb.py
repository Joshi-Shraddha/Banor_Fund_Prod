from configuration import database_dev


def get_bbg_records(TickerISIN, fund):
    """
    This function called to fetch data from database if bbg workstation down.

    :param TickerISIN: TickerISIN
    :param fund: fund
    :return: return list of values from database.

    """
    print("get_bbg_records")
    from funds import logg_Handler
    conn = database_dev()
    cursor = conn.cursor()
    query = (
        "select top 1 * from [OUTSYS_DEVELOP].DBO.[OSUSR_BOL_CONTROLTICKER_ISIN]  where [TICKERREQUESTED] ='" + str(TickerISIN) + "' order by DATETIME  desc")
    cursor.execute(query)
    rec = cursor.fetchone()
    print(rec)
    values = []
    try:
        try:
            TICKER = rec[1]
            print('TICKER: ', TICKER)
            values.append(TICKER)
        except:
            TICKER = 'Null'
            values.append(TICKER)
        try:
            PX_LAST = rec[18]
            print('PX_LAST: ', PX_LAST)
            values.append(PX_LAST)
        except:
            PX_LAST = 0
            values.append(PX_LAST)
        try:
            SECURITY_NAME = rec[4]  ###SECURITY_NAME 2
            print('SECURITY_NAME: ', SECURITY_NAME)
            values.append(SECURITY_NAME)
        except:
            SECURITY_NAME = 'Null'
            values.append(SECURITY_NAME)
        try:
            SECURITY_TYP = rec[11]  ##SECURITY_TYP   3
            print('SECURITY_TYP: ', SECURITY_TYP)
            values.append(SECURITY_TYP)
        except:
            SECURITY_TYP = 'Null'
            values.append(SECURITY_TYP)
        try:
            CRNCY = rec[7]
            print('CRNCY: ', CRNCY)
            values.append(CRNCY)
        except:
            CRNCY = 'Null'
            values.append(CRNCY)
        try:
            ID_ISIN = rec[9]
            print('ID_ISIN: ', ID_ISIN)
            values.append(ID_ISIN)
        except:
            ID_ISIN = 'Null'
            values.append(ID_ISIN)
        try:
            COUNTRY_FULL_NAME = rec[14]
            print('COUNTRY_FULL_NAME: ', COUNTRY_FULL_NAME)
            values.append(COUNTRY_FULL_NAME)
        except:
            COUNTRY_FULL_NAME = 'Null'
            values.append(COUNTRY_FULL_NAME)
        try:
            SETTLE_DT = rec[21]
            # date_format(SETTLE_DT)
            print('SETTLE_Date: ', SETTLE_DT)
            values.append(SETTLE_DT)
        except:
            SETTLE_DT = 'Null'
            values.append(SETTLE_DT)
        try:
            CNTRY_OF_RISK = rec[44]
            print('CNTRY_OF_RISK: ', CNTRY_OF_RISK)
            values.append(CNTRY_OF_RISK)
        except:
            CNTRY_OF_RISK = 'Null'
            values.append(CNTRY_OF_RISK)
        try:
            DELTA = rec[53]  ##DELTA  9
            print('DELTA: ', DELTA)
            values.append(DELTA)
        except:
            DELTA = 1
            values.append(DELTA)
        try:
            FUT_CONT_SIZE = rec[23]  # FUT_CONT_SIZE    10
            print('FUT_CONT_SIZE: ', FUT_CONT_SIZE)
            values.append(FUT_CONT_SIZE)
        except:
            FUT_CONT_SIZE = 0
            values.append(FUT_CONT_SIZE)
        try:
            INDUSTRY_SECTOR = rec[57]  # INDUSTRY_SECTOR  12
            print('INDUSTRY_SECTOR: ', INDUSTRY_SECTOR)
            values.append(INDUSTRY_SECTOR)
        except:
            INDUSTRY_SECTOR = 'Null'
            values.append(INDUSTRY_SECTOR)
        try:
            ID_BB_ULTIMATE_PARENT_CO = rec[24]  # parentcompany id  13
            print('ID_BB_ULTIMATE_PARENT_CO: ', ID_BB_ULTIMATE_PARENT_CO)
            values.append(ID_BB_ULTIMATE_PARENT_CO)
        except:
            ID_BB_ULTIMATE_PARENT_CO = 0
            values.append(ID_BB_ULTIMATE_PARENT_CO)
        try:
            REGISTERED_COUNTRY_LOCATION = rec[47]  ##reg_cntry_loc 14
            print('REGISTERED_COUNTRY_LOCATION: ', REGISTERED_COUNTRY_LOCATION)
            values.append(REGISTERED_COUNTRY_LOCATION)
        except:
            REGISTERED_COUNTRY_LOCATION = 'Null'
            values.append(REGISTERED_COUNTRY_LOCATION)
        try:
            EQY_SH_OUT = rec[41]  # issue oustanding value 15
            print('EQY_SH_OUT: ', EQY_SH_OUT)
            values.append(EQY_SH_OUT)
        except:
            EQY_SH_OUT = 0
            values.append(EQY_SH_OUT)
        try:
            COUNTRY_OF_LARGEST_REVENUE = rec[48]  ##cntry_largeset_rev  16
            print('COUNTRY_OF_LARGEST_REVENUE: ', COUNTRY_OF_LARGEST_REVENUE)
            values.append(COUNTRY_OF_LARGEST_REVENUE)
        except:
            COUNTRY_OF_LARGEST_REVENUE = 'Null'
            values.append(COUNTRY_OF_LARGEST_REVENUE)
        try:
            UNDERLYING_ISIN = rec[49]  # underlying isin  18
            print('UNDERLYING_ISIN: ', UNDERLYING_ISIN)
            values.append(UNDERLYING_ISIN)
        except:
            UNDERLYING_ISIN = 'Null'
            values.append(UNDERLYING_ISIN)
        try:
            AMT_OUTSTANDING = rec[42]  # AMT_OUTSTANDING value 19
            print('AMT_OUTSTANDING: ', AMT_OUTSTANDING)
            values.append(AMT_OUTSTANDING)
        except:
            AMT_OUTSTANDING = 0
            values.append(AMT_OUTSTANDING)
        try:
            VOLUME_AVG_20D = rec[43]  # VOLUME_AVG_20D value  20
            print('VOLUME_AVG_20D: ', VOLUME_AVG_20D)
            values.append(VOLUME_AVG_20D)
        except:
            VOLUME_AVG_20D = 0
            values.append(VOLUME_AVG_20D)
        try:
            OPT_CONT_SIZE = rec[22]  # OPT_CONT_SIZE value  21
            print('OPT_CONT_SIZE: ', OPT_CONT_SIZE)
            values.append(OPT_CONT_SIZE)
        except:
            OPT_CONT_SIZE = 0
            values.append(OPT_CONT_SIZE)
        try:
            ISSUER_INDUSTRY = rec[15]  # ISSUER_INDUSTRY value  22
            print('ISSUER_INDUSTRY: ', ISSUER_INDUSTRY)
            values.append(ISSUER_INDUSTRY)
        except:
            ISSUER_INDUSTRY = 'Null'
            values.append(ISSUER_INDUSTRY)
        try:
            CAPITAL_CONTINGENT_SECURITY = rec[46]
            print('CAPITAL_CONTINGENT_SECURITY: ', CAPITAL_CONTINGENT_SECURITY)
            values.append(CAPITAL_CONTINGENT_SECURITY)
        except:
            CAPITAL_CONTINGENT_SECURITY = 'Null'
            values.append(CAPITAL_CONTINGENT_SECURITY)
        try:
            COUNTRY = rec[8]  # COUNTRY value 24
            print('COUNTRY: ', COUNTRY)
            values.append(COUNTRY)
        except:
            COUNTRY = 'Null'
            values.append(COUNTRY)
        try:
            COUNTRY_ISO = rec[13]  # COUNTRY_ISO value 25
            print('COUNTRY_ISO: ', COUNTRY_ISO)
            values.append(COUNTRY_ISO)
        except:
            COUNTRY_ISO = 'Null'
            values.append(COUNTRY_ISO)
        try:
            CUR_MKT_CAP = rec[59]  # CUR_MKT_CAP value 26
            print('CUR_MKT_CAP: ', CUR_MKT_CAP)
            values.append(CUR_MKT_CAP)
        except:
            CUR_MKT_CAP = 0
            values.append(CUR_MKT_CAP)
        try:
            PARENT_TICKER_EXCHANGE = rec[16]  # PARENT_TICKER_EXCHANGE value 27
            print('PARENT_TICKER_EXCHANGE: ', PARENT_TICKER_EXCHANGE)
            values.append(PARENT_TICKER_EXCHANGE)
        except:
            PARENT_TICKER_EXCHANGE = 'Null'
            values.append(PARENT_TICKER_EXCHANGE)
        try:
            CHG_PCT_YTD = rec[17]
            print('CHG_PCT_YTD: ', CHG_PCT_YTD)
            values.append(CHG_PCT_YTD)
        except:
            CHG_PCT_YTD = 0
            values.append(CHG_PCT_YTD)
        try:
            CPN = rec[63]  # CPN value 29  coupon
            print('CPN: ', CPN)
            values.append(CPN)
        except:
            CPN = 0
            values.append(CPN)
        try:
            INT_ACC = rec[64]  # INT_ACC value 30  accruedinterest
            print('INT_ACC: ', INT_ACC)
            values.append(INT_ACC)
        except:
            INT_ACC = 0
            values.append(INT_ACC)
        try:
            EQY_ALPHA = rec[62]  # EQY_ALPHA value 31
            print('EQY_ALPHA: ', EQY_ALPHA)
            values.append(EQY_ALPHA)
        except:
            EQY_ALPHA = 0
            values.append(EQY_ALPHA)
        try:
            EQY_BETA = rec[61]  # EQY_BETA value 32
            print('EQY_BETA: ', EQY_BETA)
            values.append(EQY_BETA)
        except:
            EQY_BETA = 0
            values.append(EQY_BETA)
        try:
            EQY_PRIM_EXCH_SHRT = rec[20]  # EQY_PRIM_EXCH_SHRT value 33
            print('EQY_PRIM_EXCH_SHRT: ', EQY_PRIM_EXCH_SHRT)
            values.append(EQY_PRIM_EXCH_SHRT)
        except:
            EQY_PRIM_EXCH_SHRT = 'Null'
            values.append(EQY_PRIM_EXCH_SHRT)
        try:
            ID_BB_ULTIMATE_PARENT_CO_NAME = rec[24]  # ID_BB_ULTIMATE_PARENT_CO_NAME value 34
            print('ID_BB_ULTIMATE_PARENT_CO_NAME: ', ID_BB_ULTIMATE_PARENT_CO_NAME)
            values.append(ID_BB_ULTIMATE_PARENT_CO_NAME)
        except:
            ID_BB_ULTIMATE_PARENT_CO_NAME = 'Null'
            values.append(ID_BB_ULTIMATE_PARENT_CO_NAME)
        try:
            INDUSTRY_GROUP = rec[56]  # INDUSTRY_GROUP value 35
            print('INDUSTRY_GROUP: ', INDUSTRY_GROUP)
            values.append(INDUSTRY_GROUP)
        except:
            INDUSTRY_GROUP = 'Null'
            values.append(INDUSTRY_GROUP)
        try:
            SHORT_NAME = rec[6]  # SHORT_NAME value 36
            print('SHORT_NAME: ', SHORT_NAME)
            values.append(SHORT_NAME)
        except:
            SHORT_NAME = 'Null'
            values.append(SHORT_NAME)
        try:
            MARKET_SECTOR_DES = rec[10]  # MARKET_SECTOR_DES value 37
            print('MARKET_SECTOR_DES: ', MARKET_SECTOR_DES)
            values.append(MARKET_SECTOR_DES)
        except:
            MARKET_SECTOR_DES = 0
            values.append(MARKET_SECTOR_DES)
        try:
            MATURITY = rec[25]  # MATURITY date value 38
            print('MATURITY: ', MATURITY)
            values.append(MATURITY)
        except:
            MATURITY = 'Null'
            values.append(MATURITY)
        try:
            NAME = rec[5]  # NAME value 39
            print('NAME: ', NAME)
            values.append(NAME)
        except:
            NAME = 'Null'
            values.append(NAME)
        try:
            OPT_PUT_CALL = rec[54]  # OPT_PUT_CALL value 40
            print('OPT_PUT_CALL: ', OPT_PUT_CALL)
            values.append(OPT_PUT_CALL)
        except:
            OPT_PUT_CALL = 0
            values.append(OPT_PUT_CALL)
        try:
            OPT_STRIKE_PX = rec[50]  # OPT_STRIKE_PX value 41
            print('OPT_STRIKE_PX: ', OPT_STRIKE_PX)
            values.append(OPT_STRIKE_PX)
        except:
            OPT_STRIKE_PX = 0
            values.append(OPT_STRIKE_PX)
        try:
            PAYMENT_RANK = rec[45]  # PAYMENT_RANK value 42
            print('PAYMENT_RANK: ', PAYMENT_RANK)
            values.append(PAYMENT_RANK)
        except:
            PAYMENT_RANK = 'Null'
            values.append(PAYMENT_RANK)
        try:
            VOLATILITY_30D = rec[60]  # VOLATILITY_30D value 43
            print('VOLATILITY_30D: ', VOLATILITY_30D)
            values.append(VOLATILITY_30D)
        except:
            VOLATILITY_30D = 0
            values.append(VOLATILITY_30D)
        try:
            VOLUME_AVG_30D = rec[58]  # VOLUME_AVG_30D value  44
            print('VOLUME_AVG_30D: ', VOLUME_AVG_30D)
            values.append(VOLUME_AVG_30D)
        except:
            VOLUME_AVG_30D = 0
            values.append(VOLUME_AVG_30D)
        try:
            ERROR = rec[3]  ###ERROR      value  44
            print('ERROR: ', ERROR)
            values.append(ERROR)
        except:
            ERROR = ''
            values.append(ERROR)

        try:
            TICKER_AND_EXCH_CODE = rec[12]
            print('TICKER_AND_EXCH_CODE: ', TICKER_AND_EXCH_CODE)
            values.append(TICKER_AND_EXCH_CODE)
        except:
            TICKER_AND_EXCH_CODE = ''
            values.append(TICKER_AND_EXCH_CODE)

        try:
            TickerIsinUpperCase = rec[19]
            print('TickerIsinUpperCase: ', TickerIsinUpperCase)
            values.append(TickerIsinUpperCase)
        except:
            TickerIsinUpperCase = ''
            values.append(TickerIsinUpperCase)

        try:
            MaturityDate = rec[25]
            print('MaturityDate: ', MaturityDate)
            values.append(MaturityDate)
        except:
            MaturityDate = ''
            values.append(MaturityDate)

        try:
            STARTTIME = rec[26]
            print('STARTTIME: ', STARTTIME)
            values.append(STARTTIME)
        except:
            STARTTIME = ''
            values.append(STARTTIME)

        try:
            EndTime = rec[27]
            print('EndTime: ', EndTime)
            values.append(EndTime)
        except:
            EndTime = ''
            values.append(EndTime)

        try:
            UnderlayPrcie = rec[28]
            print('UnderlayPrcie: ', UnderlayPrcie)
            values.append(UnderlayPrcie)
        except:
            UnderlayPrcie = ''
            values.append(UnderlayPrcie)

        try:
            lot_size = rec[29]
            print('lot_size: ', lot_size)
            values.append(lot_size)
        except:
            lot_size = ''
            values.append(lot_size)

        try:
            Maturity_Date_Estimated = rec[51]
            print('Maturity_Date_Estimated: ', Maturity_Date_Estimated)
            values.append(Maturity_Date_Estimated)
        except:
            Maturity_Date_Estimated = ''
            values.append(Maturity_Date_Estimated)

        try:
            Next_Call_Dt = rec[52]
            print('Next_Call_Dt: ', Next_Call_Dt)
            values.append(Next_Call_Dt)
        except:
            Next_Call_Dt = ''
            values.append(Next_Call_Dt)

        logger_sqlalchemy = logg_Handler(fund, logger_name='bbg_call')
        logger_sqlalchemy.info("BBG api collect values from database")
        len(values)
        return values

    except Exception as e:
        logger_sqlalchemy = logg_Handler(fund, logger_name='bbg_call_Exception')
        logger_sqlalchemy.error(str(e))
        print(e)
        return None

# get_bbg_records()
