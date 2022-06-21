"""
Main script to call fund L/S type
@author: Shraddha Joshi
"""

from datetime import timedelta
from decimal import Decimal
from DatafromBBG import findbyBBGws01
from configuration import database_dev, get_current_date, get_current_time
from datetime import datetime
from bbgdatafromdb import get_bbg_records
from email_send_service import sendemail
import logging
from flask import jsonify


def logg_Handler(fund, logger_name):
    """

    This is log handler function used to create logger object.

    :param fund:  Fund No
    :param logger_name:  logger name

    :return: logger_sqlalchemy object.

    """
    print("logg_Handler")
    from fundapi import DatabaseHandler
    # logger_sqlalchemy = logging.getLogger(__name__)
    logger_sqlalchemy = logging.getLogger(logger_name)
    logger_sqlalchemy.propagate = False
    logger_sqlalchemy.setLevel(logging.DEBUG)
    logger_sqlalchemy.addHandler(DatabaseHandler(fund, logger_sqlalchemy))
    return logger_sqlalchemy


def Hex_To_Decimal(string):
    """
    :param string: input para.
    :return:  convert Hex to decimal
    """
    print(string, 'string to convert HEX to Decimal')
    IntakeValue = string
    # IntakeValue = Index(string,"E",startIndex:0,searchFromEnd:False,ignoreCase:True)
    if IntakeValue < 0:
        Decimal = float(IntakeValue)
    else:
        Decimal = 0
    return Decimal


def Http_Server_wst01(TickerISIN, fund):
    """
    Fetch data from bbg workstation. If workstation down fetching records from database.

    :param TickerISIN: TickerISIN (User input Ticker)
    :param fund: fund No (Fund Number to get fund)

    :return: return records into dict.

    """
    print("Http_Server_wst01")
    if (TickerISIN == "" or TickerISIN == " " or TickerISIN == "  "
            or TickerISIN == " Equity" or TickerISIN == " Corp" or
            TickerISIN == " Index" or TickerISIN == " Govt" or
            TickerISIN.find(" Govt CORP", 0) >= 0 or
            TickerISIN.find(" Equity CORP", 0) >= 0 or
            len(TickerISIN) < 9):
        return None
    try:
        values = findbyBBGws01(TickerISIN)
    except:
        values = get_bbg_records(TickerISIN, fund)
    # try:
    #     values = findbyBBGws01(TickerISIN)
    #     raise ValueError('BGG is down')
    # except (ValueError, IndexError):
    #     values = get_bbg_records(TickerISIN, fund)
    value_dict = {}
    try:
        if values != 'BBG Workstation Down':
            Error = values[39]
            err = Error
            SecurityName = values[2].upper()
            Name = values[2]
            Short_Name = values[34]
            Crncy = values[4]
            Country = values[22]
            Id_Isin = values[5]
            Market_sector_des = values[35]
            Security_typ = values[3]
            Ticker_And_Exch_Code = values[44]
            Country_Iso = values[23]
            Country_Full_Name = values[6]
            Issuer_Industry = values[20]
            Parent_Ticker_Exchange = values[25]
            Chg_Pct_Ytd = values[26]
            Px_Last = values[1]
            TickerIsinUpperCase = values[45]
            Eqy_Prim_Exch_Shrt = values[31]
            Settle_Date = values[7]
            Opt_Cont_Size = values[19]
            Fut_Cont_Size = values[10]
            MaturityDate = values[46]
            StartTime = values[47]
            EndTime = values[48]
            lot_size = values[50]
            UnderlayPrcie = str(Hex_To_Decimal(values[49]))
            Id_bb_Ultimate_Parent_Co = values[12]
            Id_bb_Ultimate_Parent_Co_Name = values[33]
            Eqy_sh_out = values[14]
            Amt_Outstanding = str(Hex_To_Decimal(values[17]))
            Volume_avg_20d = values[18]
            Cntry_of_Risk = values[8]
            Payment_Rank = values[40]
            Capital_Contingent_Security = values[21]
            Registered_Country_Location = values[13]
            Country_of_largest_Revenue = values[15]
            Underlying_Isin = values[16]
            Opt_strike_Px = str(Hex_To_Decimal(values[39]))
            Maturity_Date_Estimated = values[51]
            Next_Call_Dt = values[52]
            Delta = str(Hex_To_Decimal(values[9]))
            Opt_Put_Call = str(Hex_To_Decimal(values[38] if values[38] != '' else 0))
            Error = values[43]
            Industry_Group = values[33]
            Industry_Sector = values[11]
            Volume_avg_30D = values[42]
            Cur_Mkt_Cap = values[24]
            Volatility_30d = values[41]
            Eqy_Beta = values[30]
            Eqy_Alpha = values[29]
            Cpn = values[27]
            Int_Acc = str(Hex_To_Decimal(values[28]))

            value_dict['err'] = err
            value_dict['SecurityName'] = SecurityName
            value_dict['Name'] = Name
            value_dict['Short_Name'] = Short_Name
            value_dict['Crncy'] = Crncy
            value_dict['Country'] = Country
            value_dict['Id_Isin'] = Id_Isin
            value_dict['Market_sector_des'] = Market_sector_des
            value_dict['Security_typ'] = Security_typ
            value_dict['Ticker_And_Exch_Code'] = Ticker_And_Exch_Code
            value_dict['Country_Iso'] = Country_Iso
            value_dict['Country_Full_Name'] = Country_Full_Name
            value_dict['Issuer_Industry'] = Issuer_Industry
            value_dict['Parent_Ticker_Exchange'] = Parent_Ticker_Exchange
            value_dict['Chg_Pct_Ytd'] = Chg_Pct_Ytd
            value_dict['Px_Last'] = Px_Last
            value_dict['TickerIsinUpperCase'] = TickerIsinUpperCase
            value_dict['Eqy_Prim_Exch_Shrt'] = Eqy_Prim_Exch_Shrt
            value_dict['Settle_Date'] = Settle_Date
            value_dict['Opt_Cont_Size'] = Opt_Cont_Size
            value_dict['Fut_Cont_Size'] = Fut_Cont_Size
            value_dict['MaturityDate'] = MaturityDate
            value_dict['StartTime'] = StartTime
            value_dict['EndTime'] = EndTime
            value_dict['lot_size'] = lot_size
            value_dict['UnderlayPrcie'] = UnderlayPrcie
            value_dict['Id_bb_Ultimate_Parent_Co'] = Id_bb_Ultimate_Parent_Co
            value_dict['Id_bb_Ultimate_Parent_Co_Name'] = Id_bb_Ultimate_Parent_Co_Name
            value_dict['Eqy_sh_out'] = Eqy_sh_out
            value_dict['Amt_Outstanding'] = Amt_Outstanding
            value_dict['Volume_avg_20d'] = Volume_avg_20d
            value_dict['Cntry_of_Risk'] = Cntry_of_Risk
            value_dict['Payment_Rank'] = Payment_Rank
            value_dict['Capital_Contingent_Security'] = Capital_Contingent_Security
            value_dict['Registered_Country_Location'] = Registered_Country_Location
            value_dict['Country_of_largest_Revenue'] = Country_of_largest_Revenue
            value_dict['Underlying_Isin'] = Underlying_Isin
            value_dict['Opt_strike_Px'] = Opt_strike_Px
            value_dict['Maturity_Date_Estimated'] = Maturity_Date_Estimated
            value_dict['Next_Call_Dt'] = Next_Call_Dt
            value_dict['Delta'] = Delta
            value_dict['Opt_Put_Call'] = Opt_Put_Call
            value_dict['Error'] = Error
            value_dict['Industry_Group'] = Industry_Group
            value_dict['Industry_Sector'] = Industry_Sector
            value_dict['Volume_avg_30D'] = Volume_avg_30D
            value_dict['Cur_Mkt_Cap'] = Cur_Mkt_Cap
            value_dict['Volatility_30d'] = Volatility_30d
            value_dict['Eqy_Beta'] = Eqy_Beta
            value_dict['Eqy_Alpha'] = Eqy_Alpha
            value_dict['Cpn'] = Cpn
            value_dict['Int_Acc'] = Int_Acc
        return value_dict
    except:
        return "TickerISIN matching records not founds"


def col1_col2_with_auto_correction(Ticker, FundNumber, Col_1_Ready, CurrencyIN, ForcingInstrumentTypecol1, FundName):
    """
    This Function call inside Fund_Send function. This function assign col_1 and col_2 in case that they are in the portfolios.

    :param Ticker:  TickerISIN
    :param FundNumber:  FundNumber
    :param Col_1_Ready:  InstrumentType_1stCol
    :param CurrencyIN:  bbgCrncy
    :param ForcingInstrumentTypecol1:  ForcingTtype
    :param FundName:  fund

    :return: col_1, col_2, CurrencyOUT
    """
    fund = FundName
    print("col1_col2_with_auto_correction")
    try:
        conn = database_dev()
        cursor = conn.cursor()
        CurrDate = get_current_date()
        CurrencyOUT, col_1, col_2 = "", "", ""
        if ForcingInstrumentTypecol1:
            col_1 = Col_1_Ready
        else:
            if FundNumber == 2:
                query = (
                        "SELECT [ENNORTHAMERICALS].[ID] o0, [ENNORTHAMERICALS].[DATE] o1, [ENNORTHAMERICALS].[INSTRTYPE] o2, [ENNORTHAMERICALS].[SECURITYTYPE] o3, [ENNORTHAMERICALS].[QUANTITY] o4, [ENNORTHAMERICALS].[TICKER_ISIN] o5, [ENNORTHAMERICALS].[NAME] o6, [ENNORTHAMERICALS].[COST] o7, [ENNORTHAMERICALS].[WEIGHT_ACTUAL] o8, [ENNORTHAMERICALS].[WEIGHT_TARGET] o9, [ENNORTHAMERICALS].[PAIRTRADE] o10, [ENNORTHAMERICALS].[TRADINGINDEX] o11, [ENNORTHAMERICALS].[ORDERSTAGE] o12, [ENNORTHAMERICALS].[ORDERUPDATE] o13, [ENNORTHAMERICALS].[CURRENCY] o14, [ENNORTHAMERICALS].[DB_LASTPRICE] o15, [ENNORTHAMERICALS].[FX] o16, [ENNORTHAMERICALS].[FXDATETIME] o17, [ENNORTHAMERICALS].[FXTICKER] o18, [ENNORTHAMERICALS].[FUNDCURRENCY] o19, [ENNORTHAMERICALS].[COUNTERVALUELOCAL] o20, [ENNORTHAMERICALS].[COUNTERVALUEFUNDCRNCY] o21, [ENNORTHAMERICALS].[HIST_PX_LASTMONTH] o22, [ENNORTHAMERICALS].[HIST_DATE] o23, [ENNORTHAMERICALS].[BROKERCODE] o24, [ENNORTHAMERICALS].[BBG_PARENTCOID] o25"
                        " FROM [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] [ENNORTHAMERICALS] "
                        "WHERE ([ENNORTHAMERICALS].[DATE] = (convert(datetime, substring('" + CurrDate + "', 1, 10), 120))) AND ([ENNORTHAMERICALS].[TICKER_ISIN] = '" + Ticker + "') ORDER BY [ENNORTHAMERICALS].[NAME] ASC ")
                cursor.execute(query)
                GetNorthAmericaLsByTickerISIN = cursor.fetchall()
                for i, _ in enumerate(GetNorthAmericaLsByTickerISIN):
                    NAGetRec = GetNorthAmericaLsByTickerISIN[i]
                    col_1 = NAGetRec[2]
                    col_2 = NAGetRec[3]
            else:
                if FundNumber == 3:
                    query = (
                            "SELECT [ENGREATERCHINALS].[ID] o0, [ENGREATERCHINALS].[DATE] o1, [ENGREATERCHINALS].[INSTRTYPE] o2, [ENGREATERCHINALS].[SECURITYTYPE] o3, [ENGREATERCHINALS].[QUANTITY] o4, [ENGREATERCHINALS].[TICKER_ISIN] o5, [ENGREATERCHINALS].[NAME] o6, [ENGREATERCHINALS].[COST] o7, [ENGREATERCHINALS].[WEIGHT_ACTUAL] o8, [ENGREATERCHINALS].[WEIGHT_TARGET] o9, [ENGREATERCHINALS].[PAIRTRADE] o10, [ENGREATERCHINALS].[ORDERSTAGE] o11, [ENGREATERCHINALS].[ORDERUPDATE] o12, [ENGREATERCHINALS].[CURRENCY] o13, [ENGREATERCHINALS].[DB_LASTPRICE] o14, [ENGREATERCHINALS].[FX] o15, [ENGREATERCHINALS].[FXDATETIME] o16, [ENGREATERCHINALS].[FXTICKER] o17, [ENGREATERCHINALS].[FUNDCURRENCY] o18, [ENGREATERCHINALS].[COUNTERVALUELOCAL] o19, [ENGREATERCHINALS].[COUNTERVALUEFUNDCRNCY] o20, [ENGREATERCHINALS].[HIST_PX_LASTMONTH] o21, [ENGREATERCHINALS].[HIST_DATE] o22, [ENGREATERCHINALS].[BROKERCODE] o23, [ENGREATERCHINALS].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] [ENGREATERCHINALS] WHERE ([ENGREATERCHINALS].[DATE] = (convert(datetime, substring('" + CurrDate + "', 1, 10), 120))) AND ([ENGREATERCHINALS].[TICKER_ISIN] = '" + Ticker + "') ORDER BY [ENGREATERCHINALS].[NAME] ASC ")
                    cursor.execute(query)
                    GetGreaterChinaLsByTickerISIN = cursor.fetchall()
                    for i, _ in enumerate(GetGreaterChinaLsByTickerISIN):
                        NAGetRec = GetGreaterChinaLsByTickerISIN[i]
                        col_1 = NAGetRec[2]
                        col_2 = NAGetRec[3]
                else:
                    if FundNumber == 1:
                        query = (
                                "SELECT [ENITALYLS].[ID] o0, [ENITALYLS].[DATE] o1, [ENITALYLS].[INSTRTYPE] o2, [ENITALYLS].[SECURITYTYPE] o3, [ENITALYLS].[QUANTITY] o4, [ENITALYLS].[TICKER_ISIN] o5, [ENITALYLS].[NAME] o6, [ENITALYLS].[COST] o7, [ENITALYLS].[WEIGHT_ACTUAL] o8, [ENITALYLS].[WEIGHT_TARGET] o9, [ENITALYLS].[PAIRTRADE] o10, [ENITALYLS].[ORDERSTAGE] o11, [ENITALYLS].[ORDERUPDATE] o12, [ENITALYLS].[CURRENCY] o13, [ENITALYLS].[DB_LASTPRICE] o14, [ENITALYLS].[FUNDCURRENCY] o15, [ENITALYLS].[FX] o16, [ENITALYLS].[FXDATETIME] o17, [ENITALYLS].[FXTICKER] o18, [ENITALYLS].[COUNTERVALUELOCAL] o19, [ENITALYLS].[COUNTERVALUEFUNDCRNCY] o20, [ENITALYLS].[HIST_PX_LASTMONTH] o21, [ENITALYLS].[HIST_DATE] o22, [ENITALYLS].[BROKERCODE] o23, [ENITALYLS].[BBG_PARENTCOID] o24 "
                                "FROM [outsys_prod].DBO.[OSUSR_38P_ITALYLS] [ENITALYLS] WHERE ([ENITALYLS].[DATE] = (convert(datetime, substring('" + CurrDate + "', 1, 10), 120))) AND ([ENITALYLS].[TICKER_ISIN] = '" + Ticker + "') ORDER BY [ENITALYLS].[NAME] ASC ")
                        cursor.execute(query)
                        GetItalyLsByDate = cursor.fetchall()
                        for i, _ in enumerate(GetItalyLsByDate):
                            NAGetRec = GetItalyLsByDate[i]
                            col_1 = NAGetRec[2]
                            col_2 = NAGetRec[3]
                    if FundNumber == 5:
                        query = (
                                "SELECT [ENEUROPEANVALUE].[ID] o0, [ENEUROPEANVALUE].[DATE] o1, [ENEUROPEANVALUE].[INSTRTYPE] o2, [ENEUROPEANVALUE].[SECURITYTYPE] o3, [ENEUROPEANVALUE].[QUANTITY] o4, [ENEUROPEANVALUE].[TICKER_ISIN] o5, [ENEUROPEANVALUE].[NAME] o6, [ENEUROPEANVALUE].[COST] o7, [ENEUROPEANVALUE].[WEIGHT_ACTUAL] o8, [ENEUROPEANVALUE].[WEIGHT_TARGET] o9, [ENEUROPEANVALUE].[PAIRTRADE] o10, [ENEUROPEANVALUE].[ORDERSTAGE] o11, [ENEUROPEANVALUE].[ORDERUPDATE] o12, [ENEUROPEANVALUE].[CURRENCY] o13, [ENEUROPEANVALUE].[DB_LASTPRICE] o14, [ENEUROPEANVALUE].[FX] o15, [ENEUROPEANVALUE].[FXDATETIME] o16, [ENEUROPEANVALUE].[FXTICKER] o17, [ENEUROPEANVALUE].[FUNDCURRENCY] o18, [ENEUROPEANVALUE].[COUNTERVALUELOCAL] o19, [ENEUROPEANVALUE].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROPEANVALUE].[HIST_PX_LASTMONTH] o21, [ENEUROPEANVALUE].[HIST_DATE] o22, [ENEUROPEANVALUE].[BBG_PARENTCOID] o23, [ENEUROPEANVALUE].[BROKERCODE] o24 "
                                "FROM [outsys_prod].DBO.[OSUSR_38P_EUROPEANVALUE] [ENEUROPEANVALUE] WHERE ([ENEUROPEANVALUE].[DATE] = (convert(datetime, substring('" + CurrDate + "', 1, 10), 120))) AND ([ENEUROPEANVALUE].[TICKER_ISIN] = '" + Ticker + "') ORDER BY [ENEUROPEANVALUE].[NAME] ASC ")
                        cursor.execute(query)
                        GetEuropeanValuesByTickerISIN = cursor.fetchall()
                        for i, _ in enumerate(GetEuropeanValuesByTickerISIN):
                            NAGetRec = GetEuropeanValuesByTickerISIN[i]
                            col_1 = NAGetRec[2]
                            col_2 = NAGetRec[3]
                    else:
                        if FundNumber == 4:
                            query = (
                                    "SELECT [ENEUROBOND].[ID] o0, [ENEUROBOND].[DATE] o1, [ENEUROBOND].[INSTRTYPE] o2, [ENEUROBOND].[SECURITYTYPE] o3, [ENEUROBOND].[QUANTITY] o4, [ENEUROBOND].[TICKER_ISIN] o5, [ENEUROBOND].[NAME] o6, [ENEUROBOND].[COST] o7, [ENEUROBOND].[WEIGHT_ACTUAL] o8, [ENEUROBOND].[WEIGHT_TARGET] o9, [ENEUROBOND].[PAIRTRADE] o10, [ENEUROBOND].[ORDERSTAGE] o11, [ENEUROBOND].[ORDERUPDATE] o12, [ENEUROBOND].[CURRENCY] o13, [ENEUROBOND].[DB_LASTPRICE] o14, [ENEUROBOND].[FX] o15, [ENEUROBOND].[FXDATETIME] o16, [ENEUROBOND].[FXTICKER] o17, [ENEUROBOND].[FUNDCURRENCY] o18, [ENEUROBOND].[COUNTERVALUELOCAL] o19, [ENEUROBOND].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROBOND].[ENDMONTH_PX] o21, [ENEUROBOND].[HIST_PX_LASTMONTH] o22, [ENEUROBOND].[HIST_DATE] o23, [ENEUROBOND].[BBG_PARENTCOID] o24 "
                                    "FROM [outsys_prod].DBO.[OSUSR_BOL_EUROBOND] [ENEUROBOND] WHERE ([ENEUROBOND].[DATE] = (convert(datetime, substring('" + CurrDate + "', 1, 10), 120))) AND ([ENEUROBOND].[TICKER_ISIN] = '" + Ticker + "') ORDER BY [ENEUROBOND].[DATE] ASC ")
                            cursor.execute(query)
                            GetEuroBondByTickerISIN = cursor.fetchall()
                            for i, _ in enumerate(GetEuroBondByTickerISIN):
                                EUGetRec = GetEuroBondByTickerISIN[i]
                                if EUGetRec[2] != Col_1_Ready:
                                    pass
                                else:
                                    Feedback_Message8 = "This suggestion has a different INSTRUMENT TYPE of the position " \
                                                        "that we hold in the portfolio, the INSTRUMENT " \
                                                        "TYPE that will be used is: " + EUGetRec[2] + " "
                                    return Feedback_Message8
                                col_1 = EUGetRec[2]
                                col_2 = EUGetRec[3]

                if col_1 == "":
                    col_1 = Col_1_Ready

        if CurrencyIN == "":
            query = (
                    "SELECT [ENEQUITY].[ID] o0, [ENEQUITY].[DATE] o1, [ENEQUITY].[TICKER] o2, [ENEQUITY].[NAME] o3, [ENEQUITY].[SECURITYNAME] o4, [ENEQUITY].[ID_ISIN] o5, [ENEQUITY].[CNTRYOFRISK] o6, [ENEQUITY].[CRNCY] o7, [ENEQUITY].[CUR_MKT_CAP] o8, [ENEQUITY].[DVDEXDT] o9, [ENEQUITY].[EQY_DVD_YLD_IND] o10, [ENEQUITY].[GICS_SUB_INDUSTRY_NAME] o11, [ENEQUITY].[INDUSTRY_SECTOR] o12, [ENEQUITY].[LAST_PRICE] o13, [ENEQUITY].[PRIMARY_EXCHANGE_NAME] o14, [ENEQUITY].[PE_RATIO] o15, [ENEQUITY].[PX_TO_BOOK_RATIO] o16, [ENEQUITY].[PX_TO_SALES_RATIO] o17, [ENEQUITY].[CIE_DES_BULK] o18, [ENEQUITY].[COMPANY_WEB_ADDRESS] o19, [ENEQUITY].[EQY_BETA] o20, [ENEQUITY].[VOLUME_AVG_10D] o21, [ENEQUITY].[VOLUME_AVG_20D] o22, [ENEQUITY].[VOLUME_AVG_30D] o23, [ENEQUITY].[VOLUME_AVG_3M] o24, [ENEQUITY].[VOLUME_AVG_6M] o25, [ENEQUITY].[VOLATILITY_30D] o26, [ENEQUITY].[VOLATILITY_60D] o27, [ENEQUITY].[VOLATILITY_90D] o28, [ENEQUITY].[CHG_PCT_5D] o29, [ENEQUITY].[CHG_PCT_1D] o30, [ENEQUITY].[MARKET_SECTOR_DES] o31, [ENEQUITY].[GICS_SECTOR_NAME] o32, [ENEQUITY].[SECURITY_TYP] o33, [ENEQUITY].[SETTLE_DT] o34, [ENEQUITY].[ID_BB_ULTIMATE_PARENT_CO] o35, [ENEQUITY].[HIST_PX_ENDMONTH] o36, [ENEQUITY].[EQY_SH_OUT] o37, [ENEQUITY].[CNTRY_OF_RISK] o38, [ENEQUITY].[REGISTERED_COUNTRY_LOCATION] o39, [ENEQUITY].[COUNTRY_OF_LARGEST_REVENUE] o40, [ENEQUITY].[INDUSTRY_GROUP] o41, [ENEQUITY].[EQY_ALPHA] o42, [ENEQUITY].[FUND_GEO_FOCUS] o43, [ENEQUITY].[FUND_ASSET_CLASS_FOCUS] o44 "
                    "FROM [outsys_prod].DBO.[OSUSR_38P_EQUITY] [ENEQUITY] WHERE ([ENEQUITY].[TICKER] = '" + Ticker + "') ORDER BY [ENEQUITY].[DATE] DESC ")
            cursor.execute(query)
            GetEquitiesByTicker2 = cursor.fetchall()
            if not GetEquitiesByTicker2:
                query = (
                        "SELECT [ENDERIVATIVE].[ID] o0, [ENDERIVATIVE].[DATE] o1, [ENDERIVATIVE].[TICKER] o2, [ENDERIVATIVE].[NAME] o3, [ENDERIVATIVE].[CNTRY_OF_RISK] o4, [ENDERIVATIVE].[CRNCY] o5, [ENDERIVATIVE].[OPT_STRIKE_PX] o6, [ENDERIVATIVE].[OPT_EXPIRE_DT] o7, [ENDERIVATIVE].[OPT_PUT_CALL] o8, [ENDERIVATIVE].[ID_ISIN] o9, [ENDERIVATIVE].[COUNTRY] o10, [ENDERIVATIVE].[LAST_PRICE] o11, [ENDERIVATIVE].[OPT_UNDL_TICKER] o12, [ENDERIVATIVE].[OPT_UNDL_PX] o13, [ENDERIVATIVE].[OPT_DELTA_MID] o14, [ENDERIVATIVE].[OPT_CONT_SIZE] o15, [ENDERIVATIVE].[FUT_CONT_SIZE] o16, [ENDERIVATIVE].[PRIMARY_EXCHANGE_NAME] o17, [ENDERIVATIVE].[VOLUME_AVG_3M] o18, [ENDERIVATIVE].[VOLATILITY_90D] o19, [ENDERIVATIVE].[FUT_GEN_ROLL_DT] o20, [ENDERIVATIVE].[OPT_THETA] o21, [ENDERIVATIVE].[VEGA] o22, [ENDERIVATIVE].[OPT_RHO] o23, [ENDERIVATIVE].[GAMMA] o24, [ENDERIVATIVE].[MARKET_SECTOR_DES] o25, [ENDERIVATIVE].[SECURITY_TYP] o26, [ENDERIVATIVE].[SETTLE_DT] o27, [ENDERIVATIVE].[VOLUME_AVG_10D] o28, [ENDERIVATIVE].[VOLUME_AVG_20D] o29, [ENDERIVATIVE].[ID_BB_ULTIMATE_PARENT_CO] o30, [ENDERIVATIVE].[ISSUER_INDUSTRY] o31, [ENDERIVATIVE].[REGISTERED_COUNTRY_LOCATION] o32, [ENDERIVATIVE].[COUNTRY_OF_LARGEST_REVENUE] o33, [ENDERIVATIVE].[UNDERLYING_ISIN] o34, [ENDERIVATIVE].[INDUSTRY_GROUP] o35, [ENDERIVATIVE].[INDUSTRY_SECTOR] o36, [ENDERIVATIVE].[VOLUME_AVG_30D] o37, [ENDERIVATIVE].[CUR_MKT_CAP] o38 "
                        "FROM [outsys_prod].DBO.[OSUSR_BOL_DERIVATIVE] [ENDERIVATIVE] WHERE ([ENDERIVATIVE].[TICKER] = '" + Ticker + "') ORDER BY [ENDERIVATIVE].[DATE] DESC ")
                cursor.execute(query)
                GetDerivativesByTicker2 = cursor.fetchall()
                if not GetDerivativesByTicker2:
                    query = (
                            "SELECT [ENBOND].[ID] o0, [ENBOND].[DATE] o1, [ENBOND].[TICKER] o2, [ENBOND].[NAME] o3, [ENBOND].[CNTRY_OF_RISK] o4, [ENBOND].[CRCNY] o5, [ENBOND].[SECURITY_DES] o6, [ENBOND].[MATURITY] o7, [ENBOND].[CPN_TYP] o8, [ENBOND].[GICS_SUB_INDUSTRY_NAME] o9, [ENBOND].[ID_ISIN] o10, [ENBOND].[INDUSTRY_SECTOR] o11, [ENBOND].[PX_MID] o12, [ENBOND].[BB_COMPOSITE] o13, [ENBOND].[IS_SUBORDINATED] o14, [ENBOND].[INDUSTRY_GROUP] o15, [ENBOND].[INT_ACC] o16, [ENBOND].[CIE_DES_BULK] o17, [ENBOND].[COMPANY_WEB_ADDRESS] o18, [ENBOND].[YAS_ASW_SPREAD] o19, [ENBOND].[YLS_YTM_MID] o20, [ENBOND].[DUR_MID] o21, [ENBOND].[CPN] o22, [ENBOND].[MARKET_ISSUE] o23, [ENBOND].[NXT_CPN_DT] o24, [ENBOND].[RTG_MOODY] o25, [ENBOND].[CHG_PCT_1D] o26, [ENBOND].[CHG_PCT_5D] o27, [ENBOND].[RTG_SP] o28, [ENBOND].[MIN_PIECE] o29, [ENBOND].[MIN_INCREMENT] o30, [ENBOND].[PAYMENT_RANK] o31, [ENBOND].[AMT_ISSUED] o32, [ENBOND].[MARKET_SECTOR_DES] o33, [ENBOND].[GICS_SECTOR_NAME] o34, [ENBOND].[SECURITY_TYP] o35, [ENBOND].[SETTLE_DT] o36, [ENBOND].[VOLUME_AVG_10D] o37, [ENBOND].[VOLUME_AVG_20D] o38, [ENBOND].[ID_BB_ULTIMATE_PARENT_CO] o39, [ENBOND].[CAPITAL_CONTINGENT_SECURITY] o40, [ENBOND].[REGISTERED_COUNTRY_LOCATION] o41, [ENBOND].[COUNTRY_OF_LARGEST_REVENUE] o42, [ENBOND].[MATURITY_DATE_ESTIMATED] o43, [ENBOND].[VOLUME_AVG_30D] o44, [ENBOND].[CUR_MKT_CAP] o45, [ENBOND].[YELD] o46, [ENBOND].[ACCRUEDINTERESTS] o47, [ENBOND].[FUND_GEO_FOCUS] o48, [ENBOND].[FUND_ASSET_CLASS_FOCUS] o49 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_BOND] [ENBOND] WHERE ([ENBOND].[TICKER] = '" + Ticker + "') ORDER BY [ENBOND].[DATE] DESC ")
                    cursor.execute(query)
                    GetBondsByTicker2 = cursor.fetchall()
                    if GetBondsByTicker2:
                        for i, _ in enumerate(GetBondsByTicker2):
                            GBBT = GetBondsByTicker2[i]
                            CurrencyOUT = GBBT[5]
                else:
                    for i, _ in enumerate(GetDerivativesByTicker2):
                        GDBT = GetDerivativesByTicker2[i]
                        CurrencyOUT = GDBT[5]
            else:
                for i, _ in enumerate(GetEquitiesByTicker2):
                    GEBT = GetEquitiesByTicker2[i]
                    CurrencyOUT = GEBT[7]
        return col_1, col_2, CurrencyOUT
    except Exception as e:
        logger_sqlalchemy = logg_Handler(fund, logger_name="col1_col2_with_auto_correction Exception")
        logger_sqlalchemy.error(str(e))
        exception_json = {"status": 412, "error": str(e), "message": "Error in api "}
        return jsonify(exception_json)


def Col_1_and_2_Using_Ticker_2(Ticker, Col_1_Ready):
    """
    This function assign col_1 and col_2 in case that they are in the portfolios.

    :param Ticker: Ticker
    :param Col_1_Ready: InstrumentType_1stCol

    :return: col_2, col_1

    """
    col_2, col_1 = None, None
    print("Col_1_and_2_Using_Ticker_2")
    conn = database_dev()
    cursor = conn.cursor()

    WriteTicker = (
        "SELECT * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] WHERE ([ENSR].[ID] = (1)) ORDER BY [ENSR].[SUB_OR_RED] ASC ")
    cursor.execute(WriteTicker)
    GetWriteTickerRecords = cursor.fetchall()
    Ticker = GetWriteTickerRecords[0][3]
    UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_SR] set TICKERCHECK = '" + str(Ticker) + "' where ID = (1)")
    cursor.execute(UpdateSr)
    conn.commit()

    CheckIfOptionEquity = ("SELECT * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] WHERE (((([ENSR].[TICKERCHECK] "
                           "LIKE ((N'%' + N' C') + N'%')) AND ([ENSR].[TICKERCHECK] LIKE ((N'%' + N' Equity') + N'%'))) AND ([ENSR].[TICKERCHECK] LIKE"
                           " ((N'%' + N'/') + N'%'))) OR ((([ENSR].[TICKERCHECK] LIKE ((N'%' + N' P') + N'%')) AND ([ENSR].[TICKERCHECK] LIKE "
                           "((N'%' + N' Equity') + N'%'))) AND ([ENSR].[TICKERCHECK] LIKE ((N'%' + N'/') + N'%')))) ORDER BY [ENSR].[SUB_OR_RED] ASC ")
    cursor.execute(CheckIfOptionEquity)
    CheckIfOptionEquityRecords = cursor.fetchall()
    if CheckIfOptionEquityRecords:
        CheckIfOptionIndex = (
            "SELECT * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] WHERE (((([ENSR].[TICKERCHECK] LIKE ((N'%' + N' C') + N'%')) AND ([ENSR].[TICKERCHECK] LIKE (N'%' + N' Index'))) AND ([ENSR].[TICKERCHECK] LIKE ((N'%' + N'/') + N'%'))) OR ((([ENSR].[TICKERCHECK] LIKE ((N'%' + N' P') + N'%')) AND ([ENSR].[TICKERCHECK] "
            "LIKE (N'%' + N' Index'))) AND ([ENSR].[TICKERCHECK] LIKE ((N'%' + N'/') + N'%')))) ORDER BY [ENSR].[SUB_OR_RED] ASC")
        cursor.execute(CheckIfOptionIndex)
        CheckIfOptionIndexRecords = cursor.fetchall()
        if CheckIfOptionIndexRecords:
            CheckIfFuture = ("SELECT * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] WHERE ([ENSR].[TICKERCHECK] "
                             "LIKE ((N'%' + N' Index') + N'%')) ORDER BY [ENSR].[SUB_OR_RED] ASC ")
            cursor.execute(CheckIfFuture)
            CheckIfFutureRecords = cursor.fetchall()
            if CheckIfFutureRecords:
                CheckBond = ("SELECT * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] WHERE ([ENSR].[TICKERCHECK] "
                             "LIKE ((N'%' + N' Corp') + N'%')) ORDER BY [ENSR].[SUB_OR_RED] ASC ")
                cursor.execute(CheckBond)
                CheckBondRecords = cursor.fetchall()
                if not CheckBondRecords:
                    col_2 = "Bond"
                    col_1 = "C"
            else:
                col_2 = "Future Index"
                col_1 = "C"
        else:
            col_2 = "Option Equity"
            col_1 = "C"
    else:
        col_2 = "Option Equity"
        col_1 = "C"

    if col_1 == "":
        col_1 = Col_1_Ready

    return col_1, col_2


def Create_2ColumnActionReusable(Ticker, Name, SkeepDB, bbgSecurityType, TransactionType_1stCol):
    """

    :param Ticker: TickerSINI
    :param Name: Name
    :param SkeepDB:
    :param bbgSecurityType:
    :param TransactionType_1stCol:
    :return: Currency, DB_LastPrice, SecurityType_2stCol, idISIN, id_bb_Ultimate_Parent_Co, DatePrice, Country, Sector, \
           Coupon, Maturity, AccruedInterests, Yeld, NameOut

    """
    conn = database_dev()
    cursor = conn.cursor()
    Currency, DB_LastPrice, idISIN, id_bb_Ultimate_Parent_Co, DatePrice, Country, Sector, Coupon, Maturity, AccruedInterests, \
    Yeld, NameOut, Crncy = None, None, None, None, None, None, None, None, None, None, None, None, None
    if SkeepDB:
        Security_Typ = bbgSecurityType
    else:
        Equ_ByTicker_query = ("SELECT Top (1) * FROM [outsys_prod].DBO.[OSUSR_38P_EQUITY] [ENEQUITY] WHERE "
                              "([ENEQUITY].[TICKER] = " + "'" + str(Ticker) + "'" + ") ORDER BY [ENEQUITY].[DATE] DESC")

        cursor.execute(Equ_ByTicker_query)
        Equ_ByTicker = cursor.fetchall()
        if not Equ_ByTicker:
            Currencies_ByTicker_query = (
                    "SELECT Top (1) * FROM [outsys_prod].DBO.[OSUSR_BOL_CURRENCY] [ENCURRENCY] WHERE "
                    "([ENCURRENCY].[TICKER] = " + "'" + str(
                Ticker) + "'" + ") AND ([ENCURRENCY].[PX_LAST] != 0) ORDER BY [ENCURRENCY].[DATE] DESC")
            cursor.execute(Currencies_ByTicker_query)
            Currencies_ByTicker = cursor.fetchall()
            if not Currencies_ByTicker:
                Derivatives_ByTicker_query = (
                        "SELECT [ENCURRENCY].[ID] o0, [ENCURRENCY].[DATE] o1, [ENCURRENCY].[TIME] o2,[ENCURRENCY].[TICKER] o3, [ENCURRENCY].[NAME] o4, [ENCURRENCY].[PX_LAST] o5, [ENCURRENCY].[MARKET_SECOTR_DES] o6, "
                        "[ENCURRENCY].[SECURITY_TYP] o7, [ENCURRENCY].[SETTLE_DT] o8, [ENCURRENCY].[CURRENCY1] o9, [ENCURRENCY].[CURRENCY2] o10 FROM [outsys_prod].DBO.[OSUSR_BOL_CURRENCY] [ENCURRENCY] "
                        "WHERE ([ENCURRENCY].[TICKER] = " + "'" + str(
                    Ticker) + "'" + ") AND ([ENCURRENCY].[PX_LAST] != 0) ORDER BY [ENCURRENCY].[DATE] DESC")
                cursor.execute(Derivatives_ByTicker_query)
                Derivatives_ByTicker = cursor.fetchall()
                if not Derivatives_ByTicker:
                    BondsBy_Ticker_Query = (
                            "SELECT [ENDERIVATIVE].[ID] o0, [ENDERIVATIVE].[DATE] o1,[ENDERIVATIVE].[TICKER] o2, [ENDERIVATIVE].[NAME] o3, "
                            "[ENDERIVATIVE].[CNTRY_OF_RISK] o4, [ENDERIVATIVE].[CRNCY] o5, [ENDERIVATIVE].[OPT_STRIKE_PX] o6, [ENDERIVATIVE].[OPT_EXPIRE_DT] o7, "
                            "[ENDERIVATIVE].[OPT_PUT_CALL] o8, [ENDERIVATIVE].[ID_ISIN] o9, [ENDERIVATIVE].[COUNTRY] o10, [ENDERIVATIVE].[LAST_PRICE] o11, "
                            "[ENDERIVATIVE].[OPT_UNDL_TICKER] o12, [ENDERIVATIVE].[OPT_UNDL_PX] o13,[ENDERIVATIVE].[OPT_DELTA_MID] o14, [ENDERIVATIVE].[OPT_CONT_SIZE] o15, "
                            "[ENDERIVATIVE].[FUT_CONT_SIZE] o16, [ENDERIVATIVE].[PRIMARY_EXCHANGE_NAME] o17,[ENDERIVATIVE].[VOLUME_AVG_3M] o18, [ENDERIVATIVE].[VOLATILITY_90D] o19,"
                            " [ENDERIVATIVE].[FUT_GEN_ROLL_DT] o20, [ENDERIVATIVE].[OPT_THETA] o21,[ENDERIVATIVE].[VEGA] o22, [ENDERIVATIVE].[OPT_RHO] o23, "
                            "[ENDERIVATIVE].[GAMMA] o24, [ENDERIVATIVE].[MARKET_SECTOR_DES] o25, "
                            "[ENDERIVATIVE].[SECURITY_TYP] o26, [ENDERIVATIVE].[SETTLE_DT] o27, "
                            "[ENDERIVATIVE].[VOLUME_AVG_10D] o28, [ENDERIVATIVE].[VOLUME_AVG_20D] o29, "
                            "[ENDERIVATIVE].[ID_BB_ULTIMATE_PARENT_CO] o30, [ENDERIVATIVE].[ISSUER_INDUSTRY] o31, "
                            "[ENDERIVATIVE].[REGISTERED_COUNTRY_LOCATION] o32, "
                            "[ENDERIVATIVE].[COUNTRY_OF_LARGEST_REVENUE] o33,[ENDERIVATIVE].[UNDERLYING_ISIN] o34, [ENDERIVATIVE].[INDUSTRY_GROUP] o35,"
                            " [ENDERIVATIVE].[INDUSTRY_SECTOR] o36, [ENDERIVATIVE].[VOLUME_AVG_30D] o37,[ENDERIVATIVE].[CUR_MKT_CAP] o38 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_DERIVATIVE] [ENDERIVATIVE] WHERE ([ENDERIVATIVE].[TICKER] = " + "'" + str(
                        Ticker) + "'" + ") ORDER BY [ENDERIVATIVE].[DATE] DESC ")
                    cursor.execute(BondsBy_Ticker_Query)
                    BondsBy_Ticker = cursor.fetchall()
                    if not BondsBy_Ticker:
                        pass
                    else:
                        for q, _ in enumerate(BondsBy_Ticker):
                            Security_Typ = BondsBy_Ticker[q][35]
                            isBond = True
                            Currency = BondsBy_Ticker[q][5]
                            DB_LastPrice = BondsBy_Ticker[q][12]
                            CntryOfRisk = BondsBy_Ticker[q][4]
                            Crncy = BondsBy_Ticker[q][5]
                            idISIN = BondsBy_Ticker[q][10]
                            id_bb_Ultimate_Parent_Co = BondsBy_Ticker[q][39]
                            DatePrice = BondsBy_Ticker[q][1]
                            Yeld = BondsBy_Ticker[q][46]
                            AccruedInterests = BondsBy_Ticker[q][47]
                            Country = BondsBy_Ticker[q][4]
                            Maturity = BondsBy_Ticker[q][7]
                            Coupon = BondsBy_Ticker[q][22]
                            Sector = BondsBy_Ticker[q][11]
                            NameOut = BondsBy_Ticker[q][3]
                else:
                    for j, _ in enumerate(Derivatives_ByTicker):
                        Security_Typ = Derivatives_ByTicker[j][26]
                        isDerivative = True
                        Currency = Derivatives_ByTicker[j][5]
                        DB_LastPrice = Derivatives_ByTicker[j][11]
                        CntryOfRisk = Derivatives_ByTicker[j][4]
                        Crncy = Derivatives_ByTicker[j][5]
                        idISIN = Derivatives_ByTicker[j][9]
                        id_bb_Ultimate_Parent_Co = Derivatives_ByTicker[j][30]
                        DatePrice = Derivatives_ByTicker[j][1]
                        Country = Derivatives_ByTicker[j][4]
                        NameOut = Derivatives_ByTicker[j][3]

            else:
                for k, _ in enumerate(Currencies_ByTicker):
                    Security_Typ = Currencies_ByTicker[k][7]
                    isCurrency = True
                    DatePrice = Currencies_ByTicker[k][1]
                    NameOut = Currencies_ByTicker[k][4]
                    DB_LastPrice = Currencies_ByTicker[k][5]

        else:
            for i, _ in enumerate(Equ_ByTicker):
                Security_Typ = Equ_ByTicker[i][28]
                isEquity = True
                Currency = Equ_ByTicker[i][5]
                DB_LastPrice = Equ_ByTicker[i][12]
                CntryOfRisk = Equ_ByTicker[i][4]
                Crncy = Equ_ByTicker[i][5]
                idISIN = Equ_ByTicker[i][10]
                id_bb_Ultimate_Parent_Co = Equ_ByTicker[i][35]
                DatePrice = Equ_ByTicker[i][1]
                Sector = Equ_ByTicker[i][11]
                Country = Equ_ByTicker[i][4]
                NameOut = Equ_ByTicker[i][3]
    if "Corp".upper() in Ticker.upper() or "Govt".upper() in Ticker.upper():
        SecurityType_2stCol = "Bond"
    elif "Equity".upper() in Ticker.upper() and "ETP".upper() in Security_Typ.upper() or "ETF".upper() in Name.upper():
        SecurityType_2stCol = "ETF"
    elif "Equity".upper() in Ticker.upper() and "Open-End Fund".upper() in Security_Typ.upper() or "Equity".upper() in Ticker.upper() and "Closed-End Fund".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Fund"
    elif "Comdty".upper() in Ticker.upper() and "Financial commodity future.".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Future Bond"
    elif "Equity".upper() in Ticker.upper() and "IS".upper() in Ticker.upper() and "Common Stock".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Future Equity"
    elif "Index".upper() in Ticker.upper() and "Index Future".upper() in Security_Typ.upper() or "Index".upper() in Ticker.upper() and "Physical index future.".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Future Index"
    elif Ticker == "There is no reason to use it, we wil use Forward instead that is exactly the same thing!!!!":
        SecurityType_2stCol = "Future Currency"
    elif Ticker == "We have never traded these instruments!":
        SecurityType_2stCol = "Option Bond"
    elif "Equity".upper() in Ticker.upper() and "/" in Ticker and " P".upper() in Ticker.upper() or "Equity".upper() in Ticker.upper() and "/" in Ticker and " C".upper() in Ticker.upper() or "Equity Option".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Option Equity"
    elif "Index".upper() in Ticker.upper() and "/" in Ticker or "Index Option".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Option Index"
    elif Ticker == "We never traded this kind of instruments":  # 11
        SecurityType_2stCol = "Option Currency"
    elif "Equity".upper() in Ticker.upper() and "Equity WRT".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Warrant"
    elif TransactionType_1stCol == "FX":  # 13
        SecurityType_2stCol = "Forward"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and \
            "CNY".upper() in Crncy.upper() and "Common Stock".upper() in Security_Typ.upper or "Savings Share".upper() \
            in Security_Typ.upper() or "Dutch Cert".upper() in Security_Typ.upper() or "Foreign Sh.".upper() \
            in Security_Typ.upper() or "Unit".upper() in Security_Typ.upper():  # 14
        SecurityType_2stCol = "Equity"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and "REIT".upper() in Security_Typ.upper():  # 15
        SecurityType_2stCol = "Equity - REIT"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and "ADR".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Equity - ADR"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and "Savings Share".upper() in Security_Typ.upper():
        SecurityType_2stCol = "Equity - Savings Share"
    elif "Equity".upper() in Ticker.upper() and " CH".upper() in Ticker.upper() and "CN".upper() in CntryOfRisk.upper() \
            and "USD".upper() in Crncy.upper() or "Equity".upper() in Ticker.upper() and " CH".upper() in Ticker.upper() \
            and "CN".upper() in CntryOfRisk.upper() and "HKD".upper() in Crncy.upper() or "Equity".upper() in \
            Ticker.upper() and " C1".upper() in Ticker.upper() and "CN".upper() in CntryOfRisk.upper() \
            and "HKD".upper() in Crncy.upper() or "USD".upper() in Crncy.upper():
        SecurityType_2stCol = "Equity B-Shares"
    elif "Equity".upper() in Ticker.upper() and " CH".upper() in Ticker.upper() and "CN".upper() in CntryOfRisk.upper \
            and "CNY".upper() in Crncy.upper() or "Equity".upper() in Ticker.upper() and " C1".upper() in Ticker.upper() \
            and "CN".upper() in CntryOfRisk.upper() and "CNY".upper() in Crncy.upper():
        SecurityType_2stCol = "Equity A-Shares"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and "MLP".upper() in Security_Typ:
        SecurityType_2stCol = "Equity - MLP"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker.upper() and "Preference".upper() in Security_Typ.upper():  # 21
        SecurityType_2stCol = "Equity Preference"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker.upper() and " CH".upper() in Ticker.upper() \
            and "Right".upper() in Security_Typ.upper():  # 22
        SecurityType_2stCol = "Equity - Right"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and " CH".upper() in Ticker.upper() and \
            "Receipt".upper() in Ticker.upper() and "ETP".upper() and Security_Typ.upper():  # 23
        SecurityType_2stCol = "Equity - ETP"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and "ADR".upper() in Security_Typ.upper():  # 24
        SecurityType_2stCol = "Equity - ADR"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and "Receipt".upper() in Security_Typ.upper():  # 25
        SecurityType_2stCol = "Equity - Receipt"
    elif "Equity".upper() in Ticker.upper() and "Fund of Funds".upper() in Security_Typ.upper():  # 26
        SecurityType_2stCol = "Fund of Funds"
    elif "Equity".upper() in Ticker.upper() and "Hedge Fund".upper() in Security_Typ.upper():  # 27
        SecurityType_2stCol = "Hedge Fund"
    elif "Equity".upper() in Ticker.upper() and "/" + "/" in Ticker and "GDR".upper() in Security_Typ.upper():  # 28
        SecurityType_2stCol = "Equity - GDR"
    elif "Equity".upper() in Ticker.upper() and "SINGLE STOCK FUTURE".upper() in Security_Typ.upper():  # 29
        SecurityType_2stCol = "Single stock future"
    else:
        SecurityType_2stCol = ""
    return Currency, DB_LastPrice, SecurityType_2stCol, idISIN, id_bb_Ultimate_Parent_Co, DatePrice, Country, Sector, \
           Coupon, Maturity, AccruedInterests, Yeld, NameOut


def Col_1_and_2_Using_Weight_3(Ticker, Weight, CashORswap, StockName, BbgSecurityTyp, FundName):
    """
    This function assign col_1 and col_2 in case that they are in the portfolios.

    :param Ticker: TickerISIN
    :param Weight: WeightTarget
    :param CashORswap: gcashORswap
    :param StockName: sStockName
    :param BbgSecurityTyp: bbg_Security_Typ
    :param FundName: fund

    :return:col_1, col_2

    """
    fund = FundName
    print("Col_1_and_2_Using_Weight_3")
    try:
        Name = StockName
        SkeepDB = False
        bbgSecurityType = BbgSecurityTyp
        TransactionType_1stCol = ""  # Unknown variable need to find its its root
        if Weight < 0 or CashORswap == "SWAP":
            col_1 = "SW"
        else:
            col_1 = "C"

        Currency, DB_LastPrice, SecurityType_2stCol, idISIN, id_bb_Ultimate_Parent_Co, DatePrice, Country, Sector, Coupon, \
        Maturity, AccruedInterests, Yeld, NameOut = Create_2ColumnActionReusable(Ticker, Name, SkeepDB,
                                                                                 bbgSecurityType,
                                                                                 TransactionType_1stCol)
        col_2 = SecurityType_2stCol
        return col_1, col_2
    except Exception as e:
        logger_sqlalchemy = logg_Handler(fund, logger_name="Col_1_and_2_Using_Weight_3 Exception")
        logger_sqlalchemy.error(str(e))
        exception_json = {"status": 412, "error": str(e),
                          "message": "Error Message in Col_1_and_2_Using_Weight_3 process"}
        return jsonify(exception_json)


def Order_Type_A_B(IDcontactSuggestedBroker):
    """

    This function check for BrokerType_A_B

    :param IDcontactSuggestedBroker:  IDcontactSuggestedBroker

    :return: BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost

    """
    if IDcontactSuggestedBroker is not None or IDcontactSuggestedBroker != 0:
        BrokerType_A_B = "B"
        NeedComment = True
        BrokerSelReason = "Selected by the user, additional information needed."
        ExecutorFactor_Cost = 0
    else:
        BrokerType_A_B = "A"
        NeedComment = False
        BrokerSelReason = "System automatic selection"
        ExecutorFactor_Cost = 10
    return BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost


def Trading_Equity_Fund_ETF_Cash(FundNumber, Ticker, IDcontactSuggestedBroker, Col_2, PortfolioBrokerCode):
    """

    This function is used for Trading_Equity_Fund_ETF_Cash process

    :param FundNumber:  FundNumber
    :param Ticker: TickerISIN
    :param IDcontactSuggestedBroker: IDcontactSuggestedBroker
    :param Col_2:  ProductType_2ndCol
    :param PortfolioBrokerCode:  PortfolioBrokerCode
    :return:  Trading_Equity_Fund_ETF_Cash

    """

    print("Trading_Equity_Fund_ETF_Cash")
    conn = database_dev()
    cursor = conn.cursor()
    IDcontactForBroker = None
    BrokerName, BrokerShortCode, Custodian, Account, BrokerID, JP_MorganAccount, C4FBrokerName = None, None, None, None, None, None, None
    if FundNumber == 1:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
                Custodian = "BDL"
                Account = 1747967
            else:
                IDcontactForBroker = int(4)
                Custodian = "BDL"
                Account = 1967945
    elif FundNumber == 2:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
            elif "US".upper() in Ticker.upper():
                SearchByName = "Goldman Sachs ELECTRONIC"
            else:
                SearchByName = "Goldman Sachs ELECTRONIC"

    elif FundNumber == 3:
        if PortfolioBrokerCode != "":
            pass
        else:
            if " US".upper() in Ticker.upper():
                SearchByName = "Goldman Sachs ELECTRONIC"
            elif Col_2 == "Equity A-Shares" or Col_2 == "Equity B-Shares" or " CH ".upper() in Ticker.upper():
                SearchByName = "CICC"
                IDcontactForBroker = None
                Account = 2259651
            else:
                IDcontactForBroker = int(34)
                Custodian = "BDL"
                Account = 2259651
    elif FundNumber == 5:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
                Custodian = "BDL"
                Account = 1747967
            else:
                SearchByName = "Goldman Sachs ELECTRONIC"
    elif FundNumber == 4:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
                Custodian = "BDL"
                Account = 1747967
            else:
                IDcontactForBroker = int(10)
                Custodian = "BDL"
                Account = "1747967"
    elif FundNumber == 8:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(44)
                Custodian = "BNP"
            else:
                IDcontactForBroker = int(10)
                Custodian = "BNP"
    elif FundNumber == 6:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
            else:
                SearchByName = "Goldman Sachs"
    elif FundNumber == 10:
        if PortfolioBrokerCode != "":
            pass
        else:
            if "Us".upper() in Ticker.upper():
                SearchByName = "Goldman Sachs DESK"
    elif FundNumber == 11 or FundNumber == 12:
        if PortfolioBrokerCode != "":
            pass
        else:
            if Col_2 == "Fund":
                IDcontactForBroker = int(56)
            else:
                SearchByName = "Goldman Sachs"
    else:
        subject = "La determinazione del broker Equity cash non ha funzionato"
        title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
        sendemail(subject, title)

    if IDcontactSuggestedBroker is not None:
        if IDcontactSuggestedBroker is not None:
            IDcontactForBroker = IDcontactSuggestedBroker
            SearchByName = ""
    else:
        if PortfolioBrokerCode != "":
            ByBROKER_CODE_query = (
                    "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, "
                    "[ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9,[ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, "
                    "[ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, "
                    "[ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21,[ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, "
                    "[ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, "
                    "[ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33,[ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, "
                    "[ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39,[ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42,"
                    " [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                    "WHERE ([ENBROKER].[SHORTCODE] = " + "'" + str(
                PortfolioBrokerCode) + "'" + ") AND ([ENBROKER].[RELATIONTIPE] = N'Broker') "
                                             "ORDER BY [ENBROKER].[NAME] ASC ")
            cursor.execute(ByBROKER_CODE_query)
            ByBROKER_CODE = cursor.fetchall()
            for i, _ in enumerate(ByBROKER_CODE):
                IDcontactForBroker = ByBROKER_CODE[i][0]

    IDcontactForBroker = 0 if IDcontactForBroker is None else IDcontactForBroker

    GetContactById_query = (
            "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, "
            "[ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, "
            "[ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9"
            " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] WHERE "
            "([ENBROKER].[RELATIONTIPE] = N'Broker') AND ( [ENBROKER].[ID] = " + IDcontactForBroker + ") ORDER BY [ENBROKER].[NAME] ASC")
    cursor.execute(GetContactById_query)
    GetContactById = cursor.fetchall()
    for j, _ in enumerate(GetContactById):
        BrokerName = GetContactById[j][3]
        BrokerShortCode = GetContactById[j][5]
        C4FBrokerName = GetContactById[j][4]
        BrokerID = GetContactById[j][0]

    GetFundById_query = "SELECT [ENFUNDS].[ID] o0, [ENFUNDS].[SIMPLENAME] o1, [ENFUNDS].[NAME] o2, " \
                        "[ENFUNDS].[TICKER] o3, [ENFUNDS].[CRNCY] o4, [ENFUNDS].[CLASS] o5, [ENFUNDS].[FUND_CODE] o6, " \
                        "[ENFUNDS].[ID_ISIN] o7, [ENFUNDS].[FRONT_LOAD_FEE] o8, [ENFUNDS].[BACK_LOAD_FEE] o9, " \
                        "[ENFUNDS].[MNG_FEE] o10, [ENFUNDS].[PERF_FEE] o11, [ENFUNDS].[MNG_FEETILLJUNE14] o12," \
                        " [ENFUNDS].[PERC_FEETILLJUNE14] o13, [ENFUNDS].[COLLOCAMENTOUBS] o14, " \
                        "[ENFUNDS].[COLLOCAMENTOBANORSIM] o15, [ENFUNDS].[MINFIRSTTIME_INVESTMENT] o16, " \
                        "[ENFUNDS].[STRATEGY] o17, [ENFUNDS].[GEOFOCUSREGION] o18, [ENFUNDS].[ASSETCLASS] o19, " \
                        "[ENFUNDS].[INCEPTIONDATE] o20, [ENFUNDS].[SICAV] o21, [ENFUNDS].[BENCHMARK] o22, " \
                        "[ENFUNDS].[DETAILSCUSTOMBENCHMARKS] o23, [ENFUNDS].[SETTLEMENT] o24, " \
                        "[ENFUNDS].[MORNINGSTARCATEGORY] o25, [ENFUNDS].[CUTOFF] o26, [ENFUNDS].[DEALINGPERIOD] o27, " \
                        "[ENFUNDS].[ADVISOR] o28, [ENFUNDS].[FX_BDL_ACCOUNT] o29, [ENFUNDS].[FX_BNP_ACCOUNT] o30, " \
                        "[ENFUNDS].[BASECURRENCY] o31, [ENFUNDS].[CASA4FUND_FUNDNAME] o32, [ENFUNDS].[JPMORGANACCOUNT] o33, " \
                        "[ENFUNDS].[UBSACCOUNT] o34 FROM [outsys_prod].DBO.[OSUSR_38P_FUNDS] [ENFUNDS] WHERE " \
                        "([ENFUNDS].[FUND_CODE] = " + str(FundNumber) + ") ORDER BY [ENFUNDS].[NAME] ASC "
    cursor.execute(GetFundById_query)
    GetFundById = cursor.fetchall()
    for k, _ in enumerate(GetFundById):
        JP_MorganAccount = GetFundById[k][33]
    BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost = Order_Type_A_B(IDcontactSuggestedBroker)
    BrokerType_A_B = BrokerType_A_B
    NeedComment = NeedComment
    BrokerSelReason = BrokerSelReason
    ExecutorFactor_Cost = ExecutorFactor_Cost
    return BrokerName, BrokerShortCode, Custodian, Account, C4FBrokerName, BrokerID, \
           BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost, JP_MorganAccount


def Trading_Bond_Cash(FundNumber, Ticker, IDcontactSuggestedBroker, PortfolioBrokerCode):
    """

    This fuction get called when InstrumentType_1stCol is C and  ProductType_2ndCol is Bond.

    :param FundNumber: FundNumber
    :param Ticker: Ticker
    :param IDcontactSuggestedBroker: IDcontactSuggestedBroker
    :param PortfolioBrokerCode: PortfolioBrokerCode

    :return: Trading_Bond_Cash

    """
    print("Trading_Bond_Cash")
    conn = database_dev()
    cursor = conn.cursor()
    IDcontactForBroker, BrokerName, BrokerShortCode, Custodian, C4FBrokerName, Account, BrokerID = None, None, None, None, None, None, None
    if FundNumber == 1:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "1967945"
    elif FundNumber == 2:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "2259669"
    elif FundNumber == 3:
        if Ticker == "%" + "US" + "%":
            IDcontactForBroker = int(10)
            Custodian = "BDL"
            Account = "2259651"
        else:
            IDcontactForBroker = int(34)
            Custodian = "BDL"
            Account = "061781QB"
    elif FundNumber == 5:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "1747967"
    elif FundNumber == 4:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "1795430"
    elif FundNumber == 8:
        IDcontactForBroker = int(10)
        Custodian = "BNP"
    elif FundNumber == 10:
        pass
    elif FundNumber == 6:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "2259669"
    elif FundNumber == 11:
        IDcontactForBroker = int(10)
        Custodian = "BDL"
        Account = "2259669"
    elif FundNumber == 12:
        IDcontactForBroker = int(10)
        Custodian = "PKB"
    elif FundNumber == 13:
        IDcontactForBroker = int(10)
        Custodian = "BNP"
    else:
        subject = "La determinazione del broker Equity cash non ha funzionato"
        title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
        sendemail(subject, title)

    if IDcontactSuggestedBroker is not None:
        IDcontactForBroker = IDcontactSuggestedBroker
    else:
        IDcontactForBroker = 0

    GetContactById_query = (
            "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, "
            "[ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS]"
            " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] WHERE "
            "([ENBROKER].[ID] = " + IDcontactForBroker + ") ORDER BY [ENBROKER].[NAME] ASC")
    cursor.execute(GetContactById_query)
    GetContactById = cursor.fetchall()

    BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost = Order_Type_A_B(IDcontactSuggestedBroker)
    for k, _ in enumerate(GetContactById):
        BrokerName = GetContactById[k][3]
        BrokerShortCode = GetContactById[k][5]
        C4FBrokerName = GetContactById[k][4]
        BrokerID = GetContactById[k][0]

    BrokerType_A_B = BrokerType_A_B
    NeedComment = NeedComment
    BrokerSelReason = BrokerSelReason
    ExecutorFactor_Cost = ExecutorFactor_Cost
    return BrokerName, BrokerShortCode, Custodian, C4FBrokerName, Account, BrokerID, BrokerType_A_B, NeedComment, \
           BrokerSelReason, ExecutorFactor_Cost


def Trading_Swap(FundNumber, IDcontactSuggestedBroker, PortfolioBrokerCode):
    """

    :param FundNumber:  FundNumber
    :param IDcontactSuggestedBroker:  IDcontactSuggestedBroker
    :param PortfolioBrokerCode:  PortfolioBrokerCode

    :return:  BrokerName, BrokerShortCode, Custodian, Account, C4FBroker, BrokerID, BrokerType_A_B, \
                   NeedComment, BrokerSelReason, ExecutorFactor_Cost, JP_MorganAccount

    """
    print("Trading_Swap")
    conn = database_dev()
    cursor = conn.cursor()
    SearchByName, SearchByName = None, None
    IDcontactForBroker, BrokerName, BrokerShortCode, Custodian, Account, C4FBroker, BrokerID, JP_MorganAccount = None, None, None, None, None, None, None, None
    if FundNumber == 1:
        if PortfolioBrokerCode == "":
            IDcontactForBroker = int(34)
            Custodian = "MSIL"
            Account = "061781QA1"
    elif FundNumber == 2:
        if PortfolioBrokerCode == "":
            IDcontactForBroker = int(34)
            Account = "061781QC7"
            Custodian = "MSIL"
    elif FundNumber == 3:
        if PortfolioBrokerCode == "":
            IDcontactForBroker = int(34)
            Custodian = "MSIL"
            Account = "061781QB9"
    elif FundNumber == 8:
        if PortfolioBrokerCode == "":
            SearchByName = "UBS"
    elif FundNumber == 6:
        if PortfolioBrokerCode == "":
            IDcontactForBroker = int(34)
            Custodian = "MSIL"
            Account = "061781QA1"
    elif FundNumber == 11:
        if PortfolioBrokerCode == "":
            if IDcontactSuggestedBroker is not None:
                IDcontactForBroker = IDcontactSuggestedBroker
                SearchByName = ""
            else:
                IDcontactForBroker = int(34)
                Custodian = "MSIL"
                Account = "061781QA1"

    elif FundNumber == 10 or FundNumber == 12:
        if PortfolioBrokerCode == "":
            IDcontactForBroker = int(735)
    else:
        subject = "La determinazione del broker Equity cash non ha funzionato"
        title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
        sendemail(subject, title)

    if IDcontactSuggestedBroker is not None:
        IDcontactForBroker = IDcontactSuggestedBroker
        SearchByName = ""
    else:
        if PortfolioBrokerCode != "":
            ByBROKER_CODE_query = (
                    "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2,"
                    " [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5 "
                    "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] WHERE "
                    "([ENBROKER].[SHORTCODE] = " + PortfolioBrokerCode + ") AND ([ENBROKER].[RELATIONTIPE] = N'Broker') ORDER BY [ENBROKER].[NAME] ASC ")
            cursor.execute(ByBROKER_CODE_query)
            ByBROKER_CODE = cursor.fetchall()
            for i, _ in enumerate(ByBROKER_CODE):
                IDcontactForBroker = ByBROKER_CODE[i][0]

    if IDcontactSuggestedBroker is None and SearchByName == "" and IDcontactForBroker is None:
        pass
    else:
        GetFundById_query = ("SELECT [ENFUNDS].[ID] o0, [ENFUNDS].[SIMPLENAME] o1, [ENFUNDS].[NAME] o2, [ENFUNDS].[TICKER] o3, [ENFUNDS].[CRNCY] o4, [ENFUNDS].[CLASS] o5, [ENFUNDS].[FUND_CODE] o6, [ENFUNDS].[ID_ISIN] o7, [ENFUNDS].[FRONT_LOAD_FEE] o8, [ENFUNDS].[BACK_LOAD_FEE] o9, [ENFUNDS].[MNG_FEE] o10, [ENFUNDS].[PERF_FEE] o11, [ENFUNDS].[MNG_FEETILLJUNE14] o12, [ENFUNDS].[PERC_FEETILLJUNE14] o13, [ENFUNDS].[COLLOCAMENTOUBS] o14, [ENFUNDS].[COLLOCAMENTOBANORSIM] o15, [ENFUNDS].[MINFIRSTTIME_INVESTMENT] o16, [ENFUNDS].[STRATEGY] o17, [ENFUNDS].[GEOFOCUSREGION] o18, [ENFUNDS].[ASSETCLASS] o19, [ENFUNDS].[INCEPTIONDATE] o20, [ENFUNDS].[SICAV] o21, [ENFUNDS].[BENCHMARK] o22, [ENFUNDS].[DETAILSCUSTOMBENCHMARKS] o23, [ENFUNDS].[SETTLEMENT] o24, [ENFUNDS].[MORNINGSTARCATEGORY] o25, [ENFUNDS].[CUTOFF] o26, [ENFUNDS].[DEALINGPERIOD] o27, [ENFUNDS].[ADVISOR] o28, [ENFUNDS].[FX_BDL_ACCOUNT] o29, [ENFUNDS].[FX_BNP_ACCOUNT] o30, [ENFUNDS].[BASECURRENCY] o31, [ENFUNDS].[CASA4FUND_FUNDNAME] o32, [ENFUNDS].[JPMORGANACCOUNT] o33, [ENFUNDS].[UBSACCOUNT] o34 "
                             "FROM [outsys_prod].DBO.[OSUSR_38P_FUNDS] [ENFUNDS] WHERE ([ENFUNDS].[FUND_CODE] = '"+str(
            FundNumber)+"') ORDER BY [ENFUNDS].[NAME] ASC ")
        cursor.execute(GetFundById_query)
        GetFundById = cursor.fetchall()
        for j, _ in enumerate(GetFundById):
            JP_MorganAccount = GetFundById[j][33]

        GetContactById_query = (
                "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, "
                "[ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5,"
                " [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, "
                "[ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10 FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                "WHERE ([ENBROKER].[RELATIONTIPE] = N'Broker') AND ([ENBROKER].[ID] = " + str(
            IDcontactForBroker) + ") ORDER BY [ENBROKER].[NAME] ASC ")
        cursor.execute(GetContactById_query)
        GetContactById = cursor.fetchall()
        for k, _ in enumerate(GetContactById):
            BrokerName = GetContactById[k][3]
            BrokerShortCode = GetContactById[k][5]
            C4FBroker = GetContactById[k][4]
            BrokerID = GetContactById[k][0]
        BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost = Order_Type_A_B(IDcontactSuggestedBroker)
        BrokerType_A_B = BrokerType_A_B
        NeedComment = NeedComment
        BrokerSelReason = BrokerSelReason
        ExecutorFactor_Cost = ExecutorFactor_Cost
    return BrokerName, BrokerShortCode, Custodian, Account, C4FBroker, BrokerID, BrokerType_A_B, \
           NeedComment, BrokerSelReason, ExecutorFactor_Cost, JP_MorganAccount


def Trading_Derivative_Cash(FundNumber, PortfolioBrokerCode):
    """

    This function calculating values of IDcontactForBroker,Custodian,Account based on fundNumber

    :param FundNumber: FundNumber
    :param PortfolioBrokerCode: PortfolioBrokerCode

    :return:  IDcontactForBroker,Custodian,Account based on fundNumber

    """
    try:
        print("Trading_Derivative_Cash")
        conn = database_dev()
        cursor = conn.cursor()
        IDcontactForBroker, IDcontactSuggestedBroker = None, None
        Custodian, Account, C4FBroker, BrokerName, BrokerShortCode, C4FBrokerName, BrokerID, BrokerType_A_B, NeedComment, \
        BrokerSelReason, ExecutorFactor_Cost = None, None, None, None, None, None, None, None, None, None, None
        if FundNumber == 1:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = "22701395"
        elif FundNumber == 2:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = 22773802
        elif FundNumber == 3:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = 22773801
        elif FundNumber == 5:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = "22773804"
        elif FundNumber == 4:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = "22773803"
        elif FundNumber == 8:
            IDcontactForBroker = int(18)
            Custodian = "NE"
        elif FundNumber == 10:
            pass
        elif FundNumber == 6:
            IDcontactForBroker = int(18)
            Custodian = "NE"
            Account = 22773802
        elif FundNumber == 11:
            IDcontactForBroker = int(18)
            Custodian = "NE"
        elif FundNumber == 12:
            IDcontactForBroker = int(18)
            Custodian = "NE"
        else:
            subject = "La determinazione del broker Equity cash non ha funzionato"
            title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
            # sendemail(subject, title)

        IDcontactForBroker = 0 if IDcontactForBroker is None else IDcontactForBroker
        GetContactById_query = (
                "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1,[ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4,"
                " [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, "
                "[ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10 FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                "WHERE ([ENBROKER].[ID] = " + str(IDcontactForBroker) + ") ORDER BY [ENBROKER].[NAME] ASC ")
        cursor.execute(GetContactById_query)
        GetContactById = cursor.fetchall()
        for d, _ in enumerate(GetContactById):
            BrokerName = GetContactById[d][3]
            BrokerShortCode = GetContactById[d][5]
            C4FBrokerName = GetContactById[d][4]
            BrokerID = GetContactById[d][0]

        BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost = Order_Type_A_B(IDcontactSuggestedBroker)
        BrokerType_A_B = BrokerType_A_B
        NeedComment = NeedComment
        BrokerSelReason = BrokerSelReason
        ExecutorFactor_Cost = ExecutorFactor_Cost
        return BrokerName, BrokerShortCode, C4FBrokerName, BrokerID, BrokerType_A_B, NeedComment, BrokerSelReason, \
               ExecutorFactor_Cost, Custodian, Account, C4FBroker
    except Exception as e:
        import sys
        import os
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(e))
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error on line no. : {exc_tb.tb_lineno} - {exc_type} - {fname}")
        raise 400


def CodesGeneration_Arbor(Col_1, Col_2):
    """
    This function return ArborProductType

    :param Col_1: Col_1
    :param Col_2: Col_2

    :return: ArborProductType

    """

    print("CodesGeneration_Arbor")
    ArborProductType = None
    if Col_1 == "C" and Col_2 == "Option Equity" or Col_1 == "C" and Col_2 == "Option Index":
        ArborProductType = "OP"

    elif Col_2 == "Future Index" and Col_1 == "C" or Col_2 == "Single stock future" and Col_1 == "C":
        ArborProductType = "FU"

    elif Col_1 == "C" and Col_2 == "Bond":
        ArborProductType = "Bond"

    elif Col_1 == "SW":
        ArborProductType = "CFD"

    elif Col_1 == "C" and Col_2 == "Equity" or Col_1 == "C" and Col_2 == "Equity - REIT" or Col_1 == "C" and Col_2 == "Equity - Savings Share" or Col_1 == "C" and Col_2 == "Equity - ADR" or Col_1 == "C" and Col_2 == "ETF" or Col_1 == "C" and Col_2 == "Warrant" or Col_1 == "C" and Col_2 == "Equity - MLP" or Col_1 == "C" and Col_2 == "Equity Preference" or Col_1 == "C" and Col_2 == "Equity A-Shares" or Col_1 == "C" and Col_2 == "Equity B-Shares":
        ArborProductType = "CFD"

    elif Col_1 == "C" and Col_2 == "Fund":
        ArborProductType = "Fund"

    return ArborProductType


def Swap_Exposure(FundCode):
    """
    :param FundCode: FundCode
    :return: ExposureTradeID
    """
    conn = database_dev()
    cursor = conn.cursor()
    CurDate = get_current_date()
    now = datetime.now()
    BrokerExposure = ("SELECT Max([ENBROKEREXPOSURE].[EXPOSURETRADEID]) [EXPOSURETRADEIDMAX]"
                      " FROM [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE] [ENBROKEREXPOSURE]")

    cursor.execute(BrokerExposure)
    resultBrokerExposure = cursor.fetchall()

    ExposureTradeID = resultBrokerExposure[0][0] + 1

    GetBrokerExposures = (
        "SELECT [ENBROKEREXPOSURE].[ID] o0, [ENBROKEREXPOSURE].[DATETIME] o1, [ENBROKEREXPOSURE].[FUNDID] o2, [ENBROKEREXPOSURE].[BROKERID] o3, [ENBROKEREXPOSURE].[ORDERID] o4, [ENBROKEREXPOSURE].[WEIGHT] o5, [ENBROKEREXPOSURE].[BROKERSHORTCODE] o6, [ENBROKEREXPOSURE].[EXPOSURETRADEID] o7, [ENBROKEREXPOSURE].[SYSTEMCOMMENT] o8, [ENBROKEREXPOSURE].[FUNDNAME] o9"
        " FROM [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE] [ENBROKEREXPOSURE]")

    cursor.execute(GetBrokerExposures)
    resultGetBrokerExposures = cursor.fetchall()

    for k, _ in enumerate(resultGetBrokerExposures):
        resGetBrokerExposures = resultGetBrokerExposures[k]

        if FundCode == 4 or FundCode == 5 or FundCode == 7 or FundCode == 8 or FundCode == 11 or FundCode == 12:
            return None
        else:
            Nav = (" SELECT  [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS].[QUANTITY] AS Q"
                   " FROM  [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS]"
                   " WHERE [INSTRTYPE] = 'NAV' AND [DATE] = '" + str(CurDate) + "' ")

            cursor.execute(Nav)
            resultNav = cursor.fetchall()

            if resultNav == []:
                rNav = 0
            else:
                rNav = resultNav[0][0]
            # GREATER CHINA
            GreaterChina = (" SELECT [BROKERCODE], "
                            " SUM (ABS([WEIGHT_ACTUAL])) AS WeightTot "
                            " FROM  [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] "
                            " WHERE  [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS].[DATE] = '" + str(
                CurDate) + "'  and  [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS].[INSTRTYPE] = 'SW' "
                           " GROUP BY [BROKERCODE] ")
            cursor.execute(GreaterChina)
            resultGreaterChina = cursor.fetchall()

            if resultGreaterChina != []:

                for i, _ in enumerate(resultGreaterChina):
                    resGreaterChina = resultGreaterChina[i]
                    BrokerShortCode = resGreaterChina[0]
                    WeightTot = resGreaterChina[1]

                    FundID = 3

                    GetBrokersByShortCode = (
                            "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                            "WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerShortCode) + "') "
                                                                                         "ORDER BY [ENBROKER].[NAME] ASC ")

                    cursor.execute(GetBrokersByShortCode)
                    resultGetBrokersByShortCode = cursor.fetchall()

                    if resultGetBrokersByShortCode != []:
                        for j, _ in enumerate(resultGetBrokersByShortCode):
                            resGetBrokersByShortCode = resultGetBrokersByShortCode[j]
                            BE_BrokerId = resGetBrokersByShortCode[0]
                            BE_FundId = FundID
                            BE_ShortCode = BrokerShortCode
                            BE_Weight = WeightTot
                            BE_DateTime = now.strftime("%m-%d-%Y %H:%M:%S.000")
                            BE_ExposureTradeID = ExposureTradeID
                            BE_FundName = 'Greater China LS'

                            if BrokerShortCode == '':
                                BrokerShortCode = ' - No Broker defined - '
                            else:
                                CreateBrokerExposure = (
                                        "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE](BROKERID, FUNDID, BROKERSHORTCODE, WEIGHT, DATETIME, EXPOSURETRADEID, FUNDNAME)values(" + str(
                                    BE_BrokerId) + "," + str(BE_FundId) + ",'" + str(BE_ShortCode) + "','" + str(
                                    BE_Weight) + "','" + str(BE_DateTime) + "'," + str(
                                    BE_ExposureTradeID) + ",'" + str(BE_FundName) + "')")
                                cursor.execute(CreateBrokerExposure)
                                conn.commit()


            else:
                pass
            # AMERICA
            Nav2 = (" (SELECT  [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS].[QUANTITY] AS Q"
                    " FROM  [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS]"
                    " WHERE [INSTRTYPE] = 'NAV' AND [DATE] = '" + str(CurDate) + "') ")

            cursor.execute(Nav2)
            resultNav2 = cursor.fetchall()

            if resultNav2 == []:
                rNav2 = 0
            else:
                rNav2 = resultNav2[0][0]

            America = (" SELECT [BROKERCODE], "
                       " SUM (ABS([WEIGHT_ACTUAL])) AS WeightTot "
                       " FROM  [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] "
                       " WHERE  [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS].[DATE] = '" + str(
                CurDate) + "' and  [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS].[INSTRTYPE] = 'SW' "
                           " GROUP BY [BROKERCODE] ")
            cursor.execute(America)
            resultAmerica = cursor.fetchall()
            if resultAmerica != []:
                for i, _ in enumerate(resultAmerica):
                    resAmerica = resultAmerica[i]
                    BrokerShortCode2 = resAmerica[0]
                    WeightTot2 = resAmerica[1]
                    FundID2 = 2
                    GetBrokersByShortCode2 = (
                            "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                            "WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerShortCode2) + "') "
                                                                                          "ORDER BY [ENBROKER].[NAME] ASC ")
                    cursor.execute(GetBrokersByShortCode2)
                    resultGetBrokersByShortCode2 = cursor.fetchall()
                    if resultGetBrokersByShortCode2 != []:
                        for j, _ in enumerate(resultGetBrokersByShortCode2):
                            resGetBrokersByShortCode2 = resultGetBrokersByShortCode2[j]
                            BE_BrokerId2 = resGetBrokersByShortCode2[0]
                            BE_FundId2 = FundID2
                            BE_ShortCode2 = BrokerShortCode2
                            BE_Weight2 = WeightTot2
                            BE_DateTime2 = now.strftime("%m-%d-%Y %H:%M:%S.000")
                            BE_ExposureTradeID2 = ExposureTradeID
                            BE_FundName2 = 'North America LS'
                            if BrokerShortCode2 == '':
                                BrokerShortCode2 = ' - No Broker defined - '
                            else:
                                CreateBrokerExposure2 = (
                                        "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE](BROKERID, FUNDID, BROKERSHORTCODE, WEIGHT, DATETIME, EXPOSURETRADEID, FUNDNAME)values(" + str(
                                    BE_BrokerId2) + "," + str(BE_FundId2) + ",'" + str(BE_ShortCode2) + "','" + str(
                                    BE_Weight2) + "','" + str(BE_DateTime2) + "'," + str(
                                    BE_ExposureTradeID2) + ",'" + str(BE_FundName2) + "')")
                                cursor.execute(CreateBrokerExposure2)
                                conn.commit()
                    else:
                        pass
            else:
                pass
            # Italy
            Nav3 = (" (SELECT  [outsys_prod].DBO.[OSUSR_38P_ITALYLS].[QUANTITY] AS Q"
                    " FROM  [outsys_prod].DBO.[OSUSR_38P_ITALYLS]"
                    " WHERE [INSTRTYPE] = 'NAV' AND [DATE] = '" + str(CurDate) + "') ")

            cursor.execute(Nav3)
            resultNav3 = cursor.fetchall()
            if resultNav3 == []:
                rNav3 = 0
            else:
                rNav3 = resultNav3[0][0]
            Italy = ("SELECT [BROKERCODE], SUM (ABS([WEIGHT_ACTUAL])) AS WeightTot  FROM  "
                     "[outsys_prod].DBO.[OSUSR_38P_ITALYLS] WHERE  [outsys_prod].DBO.[OSUSR_38P_ITALYLS].[DATE] ="
                     "  '" + str(CurDate) + "' and  [outsys_prod].DBO.[OSUSR_38P_ITALYLS].[INSTRTYPE] = 'SW' GROUP BY [BROKERCODE] ")
            cursor.execute(Italy)
            resultItaly = cursor.fetchall()
            if resultItaly != []:
                for i, _ in enumerate(resultItaly):
                    resItaly = resultItaly[i]
                    BrokerShortCode3 = resItaly[0]
                    WeightTot3 = resItaly[1]
                    FundID3 = 1
                    GetBrokersByShortCode3 = (
                            "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                            "WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerShortCode3) + "') "
                                                                                          "ORDER BY [ENBROKER].[NAME] ASC ")

                    cursor.execute(GetBrokersByShortCode3)
                    resultGetBrokersByShortCode3 = cursor.fetchall()
                    if resultGetBrokersByShortCode3 != []:
                        for j, _ in enumerate(resultGetBrokersByShortCode3):
                            resGetBrokersByShortCode3 = resultGetBrokersByShortCode3[j]
                            BE_BrokerId3 = resGetBrokersByShortCode3[0]
                            BE_FundId3 = FundID3
                            BE_ShortCode3 = BrokerShortCode3
                            BE_Weight3 = WeightTot3
                            BE_DateTime3 = now.strftime("%m-%d-%Y %H:%M:%S.000")
                            BE_ExposureTradeID3 = ExposureTradeID
                            BE_FundName3 = 'Italy LS'
                            if BrokerShortCode3 == '':
                                BrokerShortCode3 = ' - No Broker defined - '
                            else:
                                CreateBrokerExposure3 = (
                                        "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE](BROKERID, FUNDID, BROKERSHORTCODE, WEIGHT, DATETIME, EXPOSURETRADEID, FUNDNAME)values(" + str(
                                    BE_BrokerId3) + "," + str(BE_FundId3) + ",'" + str(BE_ShortCode3) + "','" + str(
                                    BE_Weight3) + "','" + str(BE_DateTime3) + "'," + str(
                                    BE_ExposureTradeID3) + ",'" + str(BE_FundName3) + "')")
                                cursor.execute(CreateBrokerExposure3)
                                conn.commit()
            else:
                pass
            # Frontiers
            Nav4 = (" SELECT  [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS].[QUANTITY] AS Q"
                    " FROM  [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS]"
                    " WHERE [INSTRTYPE] = 'NAV' AND [DATE] ='" + str(CurDate) + "' ")
            cursor.execute(Nav4)
            resultNav4 = cursor.fetchall()
            if resultNav4 == []:
                rNav4 = 0
            else:
                rNav4 = resultNav4[0][0]
            Frontiers = ("SELECT [BROKERCODE], SUM (ABS([WEIGHT_ACTUAL])) AS WeightTot FROM  "
                         "[outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS] WHERE  "
                         "[outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS].[DATE] = '" + str(CurDate) + "' and  "
                         "[outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS].[INSTRTYPE] = 'SW' GROUP BY [BROKERCODE]")
            cursor.execute(Frontiers)
            resultFrontiers = cursor.fetchall()
            if resultFrontiers != []:
                for i, _ in enumerate(resultFrontiers):
                    resFrontiers = resultFrontiers[i]
                    BrokerShortCode4 = resFrontiers[0]
                    WeightTot4 = resFrontiers[1]
                    FundID4 = 10
                    GetBrokersByShortCode4 = (
                            "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                            "WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerShortCode4) + "') "
                                                                                          "ORDER BY [ENBROKER].[NAME] ASC ")
                    cursor.execute(GetBrokersByShortCode4)
                    resultGetBrokersByShortCode4 = cursor.fetchall()
                    if resultGetBrokersByShortCode4 != []:
                        for j, _ in enumerate(resultGetBrokersByShortCode4):
                            resGetBrokersByShortCode4 = resultGetBrokersByShortCode4[j]
                            BE_BrokerId4 = resGetBrokersByShortCode4[0]
                            BE_FundId4 = FundID4
                            BE_ShortCode4 = BrokerShortCode4
                            BE_Weight4 = WeightTot4
                            BE_DateTime4 = now.strftime("%m-%d-%Y %H:%M:%S.000")
                            BE_ExposureTradeID4 = ExposureTradeID
                            BE_FundName4 = 'New Frontiers'
                            if BrokerShortCode4 == '':
                                BrokerShortCode4 = ' - No Broker defined - '
                            else:
                                CreateBrokerExposure4 = (
                                        "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE](BROKERID, FUNDID, BROKERSHORTCODE, WEIGHT, DATETIME, EXPOSURETRADEID, FUNDNAME)values(" + str(
                                    BE_BrokerId4) + "," + str(BE_FundId4) + ",'" + str(BE_ShortCode4) + "','" + str(
                                    BE_Weight4) + "','" + str(BE_DateTime4) + "'," + str(
                                    BE_ExposureTradeID4) + ",'" + str(BE_FundName4) + "')")
                                cursor.execute(CreateBrokerExposure4)
                                conn.commit()
                                # Rosemary
            Nav5 = (" SELECT  [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY].[QUANTITY] AS Q"
                    " FROM  [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY]"
                    " WHERE [INSTRTYPE] = 'NAV' AND [DATE] ='" + str(CurDate) + "' ")
            cursor.execute(Nav5)
            resultNav5 = cursor.fetchall()
            if resultNav5 == []:
                rNav5 = 0
            else:
                rNav5 = resultNav5[0][0]
            Rosemary = ("SELECT [BROKERCODE], SUM (ABS([WEIGHT_ACTUAL])) AS WeightTot  "
                        "FROM  [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY]  WHERE  "
                        "[outsys_prod].DBO.[OSUSR_BOL_ROSEMARY].[DATE] = '" + str(CurDate) + "' and  "
                        "[outsys_prod].DBO.[OSUSR_BOL_ROSEMARY].[INSTRTYPE] = 'SW' GROUP BY [BROKERCODE]  ")
            cursor.execute(Rosemary)
            resultRosemary = cursor.fetchall()
            if resultRosemary != []:
                for i, _ in enumerate(resultRosemary):
                    resRosemary = resultRosemary[i]
                    BrokerShortCode5 = resRosemary[0]
                    WeightTot5 = resRosemary[1]
                    FundID5 = 6
                    GetBrokersByShortCode5 = (
                            "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                            "WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerShortCode5) + "') "
                                                                                          "ORDER BY [ENBROKER].[NAME] ASC ")
                    cursor.execute(GetBrokersByShortCode5)
                    resultGetBrokersByShortCode5 = cursor.fetchall()
                    if resultGetBrokersByShortCode5 != []:
                        for j, _ in enumerate(resultGetBrokersByShortCode5):
                            resGetBrokersByShortCode5 = resultGetBrokersByShortCode5[j]
                            BE_BrokerId5 = resGetBrokersByShortCode5[0]
                            BE_FundId5 = FundID5
                            BE_ShortCode5 = BrokerShortCode5
                            BE_Weight5 = WeightTot5
                            BE_DateTime5 = now.strftime("%m-%d-%Y %H:%M:%S.000")
                            BE_ExposureTradeID5 = ExposureTradeID
                            BE_FundName5 = 'Rosemary'
                            if BrokerShortCode5 == '':
                                BrokerShortCode5 = ' - No Broker defined - '
                            else:
                                CreateBrokerExposure5 = (
                                        "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE](BROKERID, FUNDID, BROKERSHORTCODE, WEIGHT, DATETIME, EXPOSURETRADEID, FUNDNAME)values(" + str(
                                    BE_BrokerId5) + "," + str(BE_FundId5) + ",'" + str(BE_ShortCode5) + "','" + str(
                                    BE_Weight5) + "','" + str(BE_DateTime5) + "'," + str(
                                    BE_ExposureTradeID5) + ",'" + str(BE_FundName5) + "')")
                                cursor.execute(CreateBrokerExposure5)
                                conn.commit()
            else:
                pass

            FindMinimum = ("SELECT Min([ENBROKEREXPOSURE].[WEIGHT]) [WEIGHTMIN] "
                           "FROM ([outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE] [ENBROKEREXPOSURE] "
                           "Left JOIN [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] ON ([ENBROKEREXPOSURE].[BROKERID] = [ENBROKER].[ID]))  "
                           "WHERE ([ENBROKEREXPOSURE].[EXPOSURETRADEID] = " + str(
                ExposureTradeID) + ") AND ([ENBROKEREXPOSURE].[FUNDID] = '" + str(FundCode) + "' ")
            cursor.execute(FindMinimum)
            resultFindMinimum = cursor.fetchall()
            if resultFindMinimum != []:
                for l, _ in enumerate(resultFindMinimum):
                    resFindMinimum = resultFindMinimum[l]
                    MinimunWeight = resFindMinimum[0]
                    AssignComment = ("SELECT [ENBROKEREXPOSURE].[SYSTEMCOMMENT] o0 "
                                     "FROM ([outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE] [ENBROKEREXPOSURE]  "
                                     "Left JOIN [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] ON ([ENBROKEREXPOSURE].[BROKERID] = [ENBROKER].[ID])) "
                                     "WHERE ([ENBROKEREXPOSURE].[EXPOSURETRADEID] = " + str(
                        ExposureTradeID) + ") AND ([ENBROKEREXPOSURE].[WEIGHT] ='" + str(MinimunWeight) + "')")
                    cursor.execute(AssignComment)
                    resultAssignComment = cursor.fetchall()
                    if resultAssignComment == []:
                        return None
                    else:
                        SystemComment = "This broker should be used for new position"
                        UpdateBrokerExposure = (
                                "UPDATE [outsys_prod].DBO.[OSUSR_SKP_BROKEREXPOSURE] set SYSTEMCOMMENT = '" + str(
                            SystemComment) + "' "
                                             "Left JOIN [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] ON ([ENBROKEREXPOSURE].[BROKERID] = [ENBROKER].[ID])) "
                                             "WHERE ([ENBROKEREXPOSURE].[EXPOSURETRADEID] = " + str(
                            ExposureTradeID) + ") AND ([ENBROKEREXPOSURE].[WEIGHT] ='" + str(MinimunWeight) + "')")
                        cursor.execute(UpdateBrokerExposure)
                        conn.commit()
                        return ExposureTradeID


def Broker_Selection(FundCode, InstrType, SecurityType, Ticker, BrokerID_fromPortfolio, BrokerCode_fromPortfolio,
                     BrokerSuggestedByUser, FX, Bol_BNP, Bol_BDL):
    """
    This function is used to select broker. Based on BrokerID_fromPortfolio and BrokerCode_fromPortfolio

    :param FundCode:  FundCode
    :param InstrType:  InstrumentType_1stCol
    :param SecurityType:  ProductType_2ndCol
    :param Ticker:  TickerISIN
    :param BrokerID_fromPortfolio:
    :param BrokerCode_fromPortfolio:
    :param BrokerSuggestedByUser: IDcontactSuggestedBroker
    :param FX:  Boolean field False
    :param Bol_BNP:  Boolean field False
    :param Bol_BDL:  Boolean field False

    :return:  out_BrokerShortCode, out_BrokerID, out_BrokerName, out_C4F_BrokerName, out_JP_MorganAccount, BrokerType_A_B, \
           NeedComment, BrokerSelReason, ExecutorFactor_Cost, ExposureTradeID, out_UBS_Account, out_LeiReportingCode, \
           out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate

    """
    print("Broker_Selection")
    conn = database_dev()
    cursor = conn.cursor()
    out_BrokerShortCode, out_BrokerID, out_BrokerName, out_C4F_BrokerName, out_JP_MorganAccount, BrokerType_A_B, \
    NeedComment, BrokerSelReason, ExecutorFactor_Cost, ExposureTradeID, out_UBS_Account, out_LeiReportingCode, \
    out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate = None, None, None, None, None, None, None, None, None, None, None, None, None, None, ''
    IDcontactSuggestedBroker = BrokerSuggestedByUser
    Bol_BrokerAssigned = False
    BrokerId = None
    if InstrType == "SW":
        ExposureTradeIDResult = Swap_Exposure(FundCode)
        ExposureTradeID = ExposureTradeIDResult.ExposureTradeID

    res_from_Order_Type_A_B = Order_Type_A_B(IDcontactSuggestedBroker)

    if FX:
        if Bol_BNP:
            BNP = ("SELECT [ENBROKER].[ID] o0, [ENBROKER].[SHORTCODE] o1"
                   " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                   " WHERE ([ENBROKER].[ID] = " + str(BrokerSuggestedByUser) + ")"
                                                                               " ORDER BY [ENBROKER].[NAME] ASC ")
            cursor.execute(BNP)
            resultBNP = cursor.fetchall()

            if resultBNP:
                # Assign FX
                for i, _ in enumerate(resultBNP):
                    resBNP = resultBNP[i]
                    out_BrokerID = resBNP[0]
                    out_BrokerShortCode = resBNP[1]

                    # return out_BrokerID, out_BrokerShortCode
            else:
                pass

        else:
            if Bol_BDL:

                BDL = (" SELECT [ENBROKER].[ID] o0, [ENBROKER].[SHORTCODE] o1"
                       " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                       " WHERE ([ENBROKER].[ID] = " + str(BrokerSuggestedByUser) + ")"
                                                                                   " ORDER BY [ENBROKER].[NAME] ASC")

                cursor.execute(BDL)
                resultBDL = cursor.fetchall()

                if resultBDL:
                    # Assign FX
                    for i, _ in enumerate(resultBDL):
                        resBDL = resultBDL[i]
                        out_BrokerID = resBDL[0]
                        out_BrokerShortCode = resBDL[1]
                        # return out_BrokerID, out_BrokerShortCode
                else:

                    pass
            else:
                pass
    else:
        GetFundById = (
                "SELECT [ENFUNDS].[ID] o0, [ENFUNDS].[SIMPLENAME] o1, [ENFUNDS].[NAME] o2, [ENFUNDS].[TICKER] o3, [ENFUNDS].[CRNCY] o4, [ENFUNDS].[CLASS] o5, [ENFUNDS].[FUND_CODE] o6, [ENFUNDS].[ID_ISIN] o7, [ENFUNDS].[FRONT_LOAD_FEE] o8, [ENFUNDS].[BACK_LOAD_FEE] o9, [ENFUNDS].[MNG_FEE] o10, [ENFUNDS].[PERF_FEE] o11, [ENFUNDS].[MNG_FEETILLJUNE14] o12, [ENFUNDS].[PERC_FEETILLJUNE14] o13, [ENFUNDS].[COLLOCAMENTOUBS] o14, [ENFUNDS].[COLLOCAMENTOBANORSIM] o15, [ENFUNDS].[MINFIRSTTIME_INVESTMENT] o16, [ENFUNDS].[STRATEGY] o17, [ENFUNDS].[GEOFOCUSREGION] o18, [ENFUNDS].[ASSETCLASS] o19, [ENFUNDS].[INCEPTIONDATE] o20, [ENFUNDS].[SICAV] o21, [ENFUNDS].[BENCHMARK] o22, [ENFUNDS].[DETAILSCUSTOMBENCHMARKS] o23, [ENFUNDS].[SETTLEMENT] o24, [ENFUNDS].[MORNINGSTARCATEGORY] o25, [ENFUNDS].[CUTOFF] o26, [ENFUNDS].[DEALINGPERIOD] o27, [ENFUNDS].[ADVISOR] o28, [ENFUNDS].[FX_BDL_ACCOUNT] o29, [ENFUNDS].[FX_BNP_ACCOUNT] o30, [ENFUNDS].[BASECURRENCY] o31, [ENFUNDS].[CASA4FUND_FUNDNAME] o32, [ENFUNDS].[JPMORGANACCOUNT] o33, [ENFUNDS].[UBSACCOUNT] o34"
                " FROM [outsys_prod].DBO.[OSUSR_38P_FUNDS] [ENFUNDS]"
                " WHERE ([ENFUNDS].[FUND_CODE] = " + str(FundCode) + ")"
                                                                     " ORDER BY [ENFUNDS].[NAME] ASC ")

        cursor.execute(GetFundById)
        resultGetFundById = cursor.fetchall()
        if resultGetFundById:

            for j, _ in enumerate(resultGetFundById):
                resGetFundById = resultGetFundById[j]
                out_JP_MorganAccount = resGetFundById[33]
                out_UBS_Account = resGetFundById[34]

                BrokerType_A_B = res_from_Order_Type_A_B[0]

                NeedComment = res_from_Order_Type_A_B[1]
                BrokerSelReason = res_from_Order_Type_A_B[2]
                ExecutorFactor_Cost = res_from_Order_Type_A_B[3]

                if BrokerSuggestedByUser != None:

                    BrokerSuggested = (
                            " SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                            " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                            " WHERE ([ENBROKER].[ID] =" + str(BrokerSuggestedByUser) + ")"
                                                                                       " ORDER BY [ENBROKER].[NAME] ASC ")
                    cursor.execute(BrokerSuggested)
                    resultBrokerSuggested = cursor.fetchall()

                    if resultBrokerSuggested:
                        for k, _ in enumerate(resultBrokerSuggested):
                            resBrokerSuggested = resultBrokerSuggested[k]
                            out_BrokerID = resBrokerSuggested[0]
                            out_BrokerShortCode = resBrokerSuggested[5]
                            out_BrokerName = resBrokerSuggested[3]
                            out_C4F_BrokerName = resBrokerSuggested[4]
                            out_LeiReportingCode = resBrokerSuggested[37]
                            out_BrokerCodeLevEuro = resBrokerSuggested[38]
                            out_masterAgreement = resBrokerSuggested[39]
                            out_masterAgreementVersionDate = resBrokerSuggested[40]

                            # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate
                    else:
                        pass

                else:
                    list_Derivatives_Append = ["Option", "ETF", "Future", "Single Stock Future", "Future Bond"]

                    RuleOrder = 1

                    if BrokerID_fromPortfolio != None and InstrType == "SW" or BrokerCode_fromPortfolio != "" and InstrType == "SW":

                        if BrokerID_fromPortfolio == None:
                            GetBrokerByShortCode = (
                                    "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                    " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                    " WHERE ([ENBROKER].[SHORTCODE] = '" + str(BrokerCode_fromPortfolio) + "')"
                                                                                                           " ORDER BY [ENBROKER].[NAME] ASC ")

                            cursor.execute(GetBrokerByShortCode)
                            resultGetBrokerByShortCode = cursor.fetchall()

                            if resultGetBrokerByShortCode != []:
                                for k, _ in enumerate(resultGetBrokerByShortCode):
                                    resGetBrokerByShortCode = resultGetBrokerByShortCode[k]
                                    out_BrokerID = resGetBrokerByShortCode[0]
                                    out_BrokerShortCode = BrokerCode_fromPortfolio
                                    out_BrokerName = resGetBrokerByShortCode[3]
                                    out_C4F_BrokerName = resGetBrokerByShortCode[4]
                                    out_LeiReportingCode = resGetBrokerByShortCode[37]
                                    out_BrokerCodeLevEuro = resGetBrokerByShortCode[38]
                                    out_masterAgreement = resGetBrokerByShortCode[39]
                                    out_masterAgreementVersionDate = resGetBrokerByShortCode[40]

                                    # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate

                        else:
                            IdFromPortfolfio = (
                                    "SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                    " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                    " WHERE ([ENBROKER].[ID] =" + str(BrokerID_fromPortfolio) + ")"
                                                                                                " ORDER BY [ENBROKER].[NAME] ASC ")

                            cursor.execute(IdFromPortfolfio)
                            resultIdFromPortfolfio = cursor.fetchall()

                            if resultIdFromPortfolfio != []:
                                for k, _ in enumerate(resultIdFromPortfolfio):
                                    resIdFromPortfolfio = resultIdFromPortfolfio[k]
                                    out_BrokerID = resIdFromPortfolfio[0]
                                    out_BrokerShortCode = resIdFromPortfolfio[5]
                                    out_BrokerName = resIdFromPortfolfio[3]
                                    out_C4F_BrokerName = resIdFromPortfolfio[4]
                                    out_LeiReportingCode = resIdFromPortfolfio[37]
                                    out_BrokerCodeLevEuro = resIdFromPortfolfio[38]
                                    out_masterAgreement = resIdFromPortfolfio[39]
                                    out_masterAgreementVersionDate = resIdFromPortfolfio[40]

                                    # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate

                            else:
                                pass

                    else:
                        GetBrokerSelectionByFundCode = (
                                " SELECT [ENBROKERSELECTION].[ID] o0, [ENBROKERSELECTION].[FUNDSIMPLENAME] o1, [ENBROKERSELECTION].[FUND_CODE] o2, [ENBROKERSELECTION].[INSTRTYPE] o3, [ENBROKERSELECTION].[SECURITYTYPE] o4, [ENBROKERSELECTION].[BROKERID] o5, [ENBROKERSELECTION].[BROKERSELECTIONMETHOD] o6, [ENBROKERSELECTION].[RULEORDER] o7, [ENBROKERSELECTION].[PARTOFTICKER] o8, [ENBROKERSELECTION].[ANYSECURITYTYPE] o9, [ENBROKERSELECTION].[USEBROKERPORTFOLIO] o10, [ENBROKERSELECTION].[DERIVATIVES] o11"
                                " FROM [outsys_prod].DBO.[OSUSR_SKP_BROKERSELECTION] [ENBROKERSELECTION]"
                                " WHERE ([ENBROKERSELECTION].[FUND_CODE] = '" + str(
                            FundCode) + "') AND ([ENBROKERSELECTION].[INSTRTYPE] = '" + str(
                            InstrType) + "') AND ([ENBROKERSELECTION].[RULEORDER] = " + str(RuleOrder) + ")"
                                                                                                         " ORDER BY [ENBROKERSELECTION].[FUNDSIMPLENAME] ASC ")

                        cursor.execute(GetBrokerSelectionByFundCode)
                        resultGetBrokerSelectionByFundCode = cursor.fetchall()

                        if resultGetBrokerSelectionByFundCode:
                            for l, _ in enumerate(resultGetBrokerSelectionByFundCode):
                                resGetBrokerSelectionByFundCode = resultGetBrokerSelectionByFundCode[l]

                                if resGetBrokerSelectionByFundCode[10]:
                                    continue
                                elif resGetBrokerSelectionByFundCode[9]:
                                    PartOfTheTicker = resGetBrokerSelectionByFundCode[8]

                                    if PartOfTheTicker != "":
                                        if PartOfTheTicker.upper() in Ticker[0:].upper():
                                            BrokerId = resGetBrokerSelectionByFundCode[5]
                                            Bol_BrokerAssigned = True

                                    else:

                                        BrokerId = resGetBrokerSelectionByFundCode[5]
                                        Bol_BrokerAssigned = True

                                else:
                                    continue

                            if Bol_BrokerAssigned:
                                BrokerId = BrokerId if BrokerId else 'NULL'

                                GetBrokerById = (
                                        " SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                        " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                        " WHERE ([ENBROKER].[ID] =" + str(BrokerId) + ")"
                                                                                      " ORDER BY [ENBROKER].[NAME] ASC ")

                                cursor.execute(GetBrokerById)
                                resultGetBrokerById = cursor.fetchall()

                                if resultGetBrokerById != []:
                                    for m, _ in enumerate(resultGetBrokerById):
                                        resGetBrokerById = resultGetBrokerById[m]
                                        out_BrokerID = BrokerId
                                        out_BrokerShortCode = resGetBrokerById[5]
                                        out_BrokerName = resGetBrokerById[3]
                                        out_C4F_BrokerName = resGetBrokerById[4]
                                        out_LeiReportingCode = resGetBrokerById[37]
                                        out_BrokerCodeLevEuro = resGetBrokerById[38]
                                        out_masterAgreement = resGetBrokerById[39]
                                        out_masterAgreementVersionDate = resGetBrokerById[40]

                                        # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate
                            else:
                                for m, _ in enumerate(resultGetBrokerSelectionByFundCode):
                                    resGetBrokerSelectionByFundCode = resultGetBrokerSelectionByFundCode[m]

                                    if resGetBrokerSelectionByFundCode[10]:
                                        continue

                                    elif resGetBrokerSelectionByFundCode[9]:
                                        continue
                                    else:
                                        if resGetBrokerSelectionByFundCode[4].upper in SecurityType[0:].upper:
                                            SearchSecurityType = 1

                                            if SearchSecurityType >= 0:
                                                PartOfTheTicker = resGetBrokerSelectionByFundCode[8]
                                                if PartOfTheTicker != "":
                                                    if PartOfTheTicker.upper() in Ticker[0:].upper():
                                                        BrokerId = resGetBrokerSelectionByFundCode[5]
                                                        Bol_BrokerAssigned = True
                                                else:
                                                    BrokerId = resGetBrokerSelectionByFundCode[5]
                                                    Bol_BrokerAssigned = True


                                            else:
                                                continue

                                if Bol_BrokerAssigned == True:
                                    GetBrokerById = (
                                            " SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                            " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                            " WHERE ([ENBROKER].[ID] =" + str(BrokerId) + ")"
                                                                                          " ORDER BY [ENBROKER].[NAME] ASC ")

                                    cursor.execute(GetBrokerById)
                                    resultGetBrokerById = cursor.fetchall()

                                    if resultGetBrokerById != []:
                                        for m, _ in enumerate(resultGetBrokerById):
                                            resGetBrokerById = resultGetBrokerById[m]
                                            out_BrokerID = BrokerId
                                            out_BrokerShortCode = resGetBrokerById[5]
                                            out_BrokerName = resGetBrokerById[3]
                                            out_C4F_BrokerName = resGetBrokerById[4]
                                            out_LeiReportingCode = resGetBrokerById[37]
                                            out_BrokerCodeLevEuro = resGetBrokerById[38]
                                            out_masterAgreement = resGetBrokerById[39]
                                            out_masterAgreementVersionDate = resGetBrokerById[40]

                                            # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate
                                else:
                                    for n, _ in enumerate(resultGetBrokerSelectionByFundCode):
                                        resGetBrokerSelectionByFundCode = resultGetBrokerSelectionByFundCode[n]

                                        if resGetBrokerSelectionByFundCode[10]:
                                            continue
                                        elif resGetBrokerSelectionByFundCode[9]:
                                            continue
                                        elif resGetBrokerSelectionByFundCode[11]:
                                            PartOfTheTicker = resGetBrokerSelectionByFundCode[8]
                                            if PartOfTheTicker != "":
                                                if PartOfTheTicker.upper() in Ticker[0:].upper():
                                                    SearchSecurityType1 = 1
                                                else:
                                                    continue

                                            for o, _ in enumerate(list_Derivatives_Append):
                                                Append_Current_Element = list_Derivatives_Append[o]

                                                if Append_Current_Element.upper() in SecurityType[0:].upper():
                                                    IndexResult = 1
                                                else:
                                                    IndexResult = -1
                                                if IndexResult >= 0:
                                                    BrokerId = resGetBrokerSelectionByFundCode[5]
                                                    Bol_BrokerAssigned = True

                                    if Bol_BrokerAssigned == True:
                                        GetBrokerById = (
                                                " SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                                " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                                " WHERE ([ENBROKER].[ID] =" + str(BrokerId) + ")"
                                                                                              " ORDER BY [ENBROKER].[NAME] ASC ")

                                        cursor.execute(GetBrokerById)
                                        resultGetBrokerById = cursor.fetchall()

                                        if resultGetBrokerById != []:
                                            for m, _ in enumerate(resultGetBrokerById):
                                                resGetBrokerById = resultGetBrokerById[m]
                                                out_BrokerID = BrokerId
                                                out_BrokerShortCode = resGetBrokerById[5]
                                                out_BrokerName = resGetBrokerById[3]
                                                out_C4F_BrokerName = resGetBrokerById[4]
                                                out_LeiReportingCode = resGetBrokerById[37]
                                                out_BrokerCodeLevEuro = resGetBrokerById[38]
                                                out_masterAgreement = resGetBrokerById[39]
                                                out_masterAgreementVersionDate = resGetBrokerById[40]

                                                # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate

                                    elif RuleOrder > 10:
                                        GetBrokerById = (
                                                " SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, [ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14, [ENBROKER].[CONSISTENCYISSUE] o15, [ENBROKER].[UPDATEDBY] o16, [ENBROKER].[LASTSYSTOSALSEFORCECON] o17, [ENBROKER].[IDWITHISSUES] o18, [ENBROKER].[SF_ID] o19, [ENBROKER].[DELETEDONSF] o20, [ENBROKER].[SF_LASTUPDATING] o21, [ENBROKER].[HEADQUARTER] o22, [ENBROKER].[COMPANYSIZE] o23, [ENBROKER].[WEBSITE] o24, [ENBROKER].[DO_CREATE] o25, [ENBROKER].[DO_UPDATE] o26, [ENBROKER].[KEYWORD] o27, [ENBROKER].[EMAIL] o28, [ENBROKER].[ACTIVEINCONSISTENCY] o29, [ENBROKER].[CREATEDBY] o30, [ENBROKER].[ISDA] o31, [ENBROKER].[SWAPEXPOSURE] o32, [ENBROKER].[SWAPPOOL] o33, [ENBROKER].[DELETEDFROMCRM] o34, [ENBROKER].[SUPPLIER] o35, [ENBROKER].[BIC_CODE_LEVEURO] o36, [ENBROKER].[LEIREPORTINGCODE] o37, [ENBROKER].[BROKERCODE] o38, [ENBROKER].[MASTERAGREEMENT] o39, [ENBROKER].[MASTERAGREEMENTVERSION_DATE] o40, [ENBROKER].[LEIEXPIRYDATE] o41, [ENBROKER].[COUNTRY] o42, [ENBROKER].[SBJREPORTING] o43, [ENBROKER].[NDG] o44"
                                                " FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER]"
                                                " WHERE ([ENBROKER].[ID] =" + str(BrokerId) + ")"
                                                                                              " ORDER BY [ENBROKER].[NAME] ASC ")

                                        cursor.execute(GetBrokerById)
                                        resultGetBrokerById = cursor.fetchall()
                                        if resultGetBrokerById:
                                            for m, _ in enumerate(resultGetBrokerById):
                                                resGetBrokerById = resultGetBrokerById[m]
                                                out_BrokerID = BrokerId
                                                out_BrokerShortCode = resGetBrokerById[5]
                                                out_BrokerName = resGetBrokerById[3]
                                                out_C4F_BrokerName = resGetBrokerById[4]
                                                out_LeiReportingCode = resGetBrokerById[37]
                                                out_BrokerCodeLevEuro = resGetBrokerById[38]
                                                out_masterAgreement = resGetBrokerById[39]
                                                out_masterAgreementVersionDate = resGetBrokerById[40]
                                                # return out_BrokerID, out_BrokerShortCode, out_BrokerName, out_C4F_BrokerName, out_LeiReportingCode, out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate
                                    else:
                                        RuleOrder = RuleOrder + 1
                                continue
    return out_BrokerShortCode, out_BrokerID, out_BrokerName, out_C4F_BrokerName, out_JP_MorganAccount, BrokerType_A_B, \
           NeedComment, BrokerSelReason, ExecutorFactor_Cost, ExposureTradeID, out_UBS_Account, out_LeiReportingCode, \
           out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate


def CodesGeneration_Casa_4_Funds(Col_1, Col_2):
    """
    This function returns Casa4FundsCode

    :param Col_1: Col_1
    :param Col_2: Col_2

    :return:  Casa4FundsCode

    """
    print("CodesGeneration_Casa_4_Funds")
    Casa4FundsCode = None
    if Col_1 == "C" and Col_2 == "Option Equity":
        Casa4FundsCode = "Option Equity"

    elif Col_2 == "Future_Index" and Col_1 == "C":
        Casa4FundsCode = "Future Index"

    elif Col_1 == "C" and Col_2 == "Bond":
        Casa4FundsCode = "Bond"

    elif Col_1 == "SW":
        Casa4FundsCode = "Swap"

    elif Col_1 == "C" and Col_2 == "Equity" or Col_1 == "C" and Col_2 == "Equity - REIT" or Col_1 == "C" and Col_2 == "Equity - Savings Share" or Col_1 == "C" and Col_2 == "Equity - ADR" or Col_1 == "C" and Col_2 == "Warrant" or Col_1 == "C" and Col_2 == "Equity - MLP" or Col_1 == "C" and Col_2 == "Equity Preference" or Col_1 == "C" and Col_2 == "Receipt":
        Casa4FundsCode = "Equity"

    elif Col_1 == "C" and Col_2 == "Warrant":
        Casa4FundsCode = "Warrant"

    elif Col_1 == "C" and Col_2 == "Equity" or Col_1 == "C" and Col_2 == "Equity - REIT" or Col_1 == "C" and Col_2 == "Equity - Savings Share" or Col_1 == "C" and Col_2 == "Equity - ADR" or Col_1 == "C" and Col_2 == "ETF":
        Casa4FundsCode = "ETF"

    elif Col_1 == "C" and Col_2 == "Fund":
        Casa4FundsCode = "Fund"

    elif Col_1 == "C" and Col_2 == "FX":
        Casa4FundsCode = "FX"

    return Casa4FundsCode


def Value_Date_Generator(Col_1, Col_2, Ticker):
    """

    :param Col_1: Col_1
    :param Col_2: Col_2
    :param Ticker: TickerSINI
    :return:  t_plus

    """
    print("Value_Date_Generator")
    t_plus = 0
    if Col_2.upper().find("Equity".upper()) >= 0:  # Doubt
        pass
    # elif 1 == 1:
    #     print("End")
    #     pass
    elif Ticker.upper().find(" US") >= 0:
        t_plus = 3
    elif Ticker.upper().find(" HK") >= 0:
        if Col_1 == "C":
            t_plus = 2
        else:
            t_plus = 3
    elif Ticker.upper().find(" CH") >= 0:
        t_plus = 3
    else:
        t_plus = 2

    return t_plus


def Owl_Api(BBG_BrokerCode, Col_1, Col_2):
    """

    :param BBG_BrokerCode:  BBG_BrokerCode
    :param Col_1: Col_1
    :param Col_2:  Col_2
    :return:  API_Command
    """
    print("Owl_Api")
    API_Command = ""

    if BBG_BrokerCode != "":
        if Col_1 == "C" or Col_1 == "SW":
            if Col_2 == "Equity" or Col_2 == "Equity - REIT" or Col_2 == "Equity - ADR" or Col_2 == "Equity - Savings Share" or Col_2 == "Equity - Right" or Col_2 == "Equity - ETP" or Col_2 == "Equity - MLP" or Col_2 == "Equity B-Shares" or Col_2 == "Equity A-Shares" or Col_2 == "Equity Preference" or Col_2 == "Option Equity" or Col_2 == "Option Index" or Col_2 == "EQ" or Col_2 == "OP" or Col_2 == "CFD" or Col_2 == "FU" or Col_2 == "Future Index" or Col_2 == "Single stock future":
                API_Command = "WaitingOwlEmsx"
            elif Col_2 == "Bond":
                API_Command = "WaitingOwlTSOX"

    return API_Command


def Calculations(WeightTarget, WeightActual, FundCurrency, StockCurrency, ShowQuantityBox, PreciseQuantityIndicated,
                 Bbg_Px_Last, ProductType_2ndCol, NAV_or_PCvalue, QuantityToAvoidShortSelling, bbgCountryISO,
                 MEA_Multiplier, BrokeriD, Exchange, bbgFutContSize, bbgOptContSize, ExposureCalculationMethod,
                 UnderlyingPrice):
    """

    This function used to perform calculations based on WeightTarget,WeightActual and Quantity etc.

    How the value get calculated.


    1) CounterValueLocalCurrency = PreciseQuantityIndicated * Bbg_Px_Last / 100 \b
    2) Quantity = CounterValueLocalCurrency / (UnderlyingPrice / 100 * bbgFutContSize)
    3) CounterValueLocalCurrency = Hex_To_Decimal(CounterValueLocalCurrency)
    4) BrokerCommisionsBPS = CounterValueLocalCurrency * (BrokerBps / 10000)
    5) BrokerCommisionsCENT = QuantityRounded * Decimal(BrokerCentPerShare / 100)


    :param WeightTarget:  WeightTarget
    :param WeightActual:  WeightActual
    :param FundCurrency:  FundCurrency
    :param StockCurrency:  bbgCrncy
    :param ShowQuantityBox:  ShowQuantityBox
    :param PreciseQuantityIndicated:  TradeQuantityPreciseIndicated
    :param Bbg_Px_Last:  bbg_Px_Last
    :param ProductType_2ndCol:  ProductType_2ndCol
    :param NAV_or_PCvalue:  NAV
    :param QuantityToAvoidShortSelling:  QuantityToAvoidShortSelling
    :param bbgCountryISO:  bbg_Country_Iso
    :param MEA_Multiplier:  Multiplier
    :param BrokeriD:  BrokerID
    :param Exchange:  bbg_Exchange
    :param bbgFutContSize: bbg_FutContSize
    :param bbgOptContSize:  bbg_OptContSize
    :param ExposureCalculationMethod:  ExposureCalculationMethod
    :param UnderlyingPrice:  bbg_UnderlyingPrice

    :return:  CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded, \
           BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX

    """
    try:
        print("Calculations")
        conn = database_dev()
        cursor = conn.cursor()
        CounterValueFundCurrency, CounterValueLocalCurrency, BrokerCommisionsBPS, BrokerBps, \
        BrokerCentPerShare, BrokerCommisionsCENT, PotentialError = 0, 0, 0, 0, 0, 0, None
        QuantityRounded,Fx_FundCrncyVSfundCrncy = 0,0
        BrokeriD = 0 if BrokeriD is None else BrokeriD
        WeightTarget = Decimal(WeightTarget / 100)
        WeightActual = Decimal(WeightActual)
        if FundCurrency == StockCurrency:
            Fx_FundCrncyVSfundCrncy = 1
            FX = 1
        else:
            FX = 0
            query = (
                    "SELECT [ENCURRENCY].[ID] o0, [ENCURRENCY].[DATE] o1, [ENCURRENCY].[TIME] o2, [ENCURRENCY].[TICKER] o3,"
                    " [ENCURRENCY].[NAME] o4, [ENCURRENCY].[PX_LAST] o5, [ENCURRENCY].[MARKET_SECOTR_DES] o6, "
                    "[ENCURRENCY].[SECURITY_TYP] o7, [ENCURRENCY].[SETTLE_DT] o8, [ENCURRENCY].[CURRENCY1] o9, "
                    "[ENCURRENCY].[CURRENCY2] o10 FROM [outsys_prod].DBO.[OSUSR_BOL_CURRENCY] [ENCURRENCY]"
                    " WHERE ([ENCURRENCY].[CURRENCY1] = '" + FundCurrency + "') AND "
                                                                            "([ENCURRENCY].[CURRENCY2] = '" + StockCurrency + "') ORDER BY [ENCURRENCY].[DATE] DESC")
            cursor.execute(query)
            resultGet = cursor.fetchall()
            for i, _ in enumerate(resultGet):
                Fx_FundCrncyVSfundCrncy = resultGet[i].Px_Last
                FX = resultGet[i].Px_Last

        if ShowQuantityBox:
            if Bbg_Px_Last == 0:
                CounterValueLocalCurrency = Decimal(0)
                if ProductType_2ndCol == "Bond":
                    CounterValueLocalCurrency = Decimal((PreciseQuantityIndicated * Bbg_Px_Last) / 100)
                if StockCurrency == "GBp":
                    if ProductType_2ndCol == "Future Index":
                        CounterValueLocalCurrency = Decimal(
                            Decimal(PreciseQuantityIndicated * Bbg_Px_Last / 100) * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        CounterValueLocalCurrency = Decimal(
                            Decimal(PreciseQuantityIndicated * Bbg_Px_Last) / 100 * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        CounterValueLocalCurrency = Decimal(
                            Decimal(PreciseQuantityIndicated * Bbg_Px_Last) * bbgOptContSize)
                    else:
                        CounterValueLocalCurrency = Decimal(Decimal(PreciseQuantityIndicated * Bbg_Px_Last) / 100)
                else:
                    if ProductType_2ndCol == "Future Index":
                        CounterValueLocalCurrency = Decimal(PreciseQuantityIndicated * Bbg_Px_Last * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        CounterValueLocalCurrency = Decimal(PreciseQuantityIndicated * Bbg_Px_Last * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        CounterValueLocalCurrency = Decimal(PreciseQuantityIndicated * Bbg_Px_Last * bbgOptContSize)
                    else:
                        CounterValueLocalCurrency = Decimal(PreciseQuantityIndicated * Bbg_Px_Last)
            else:
                CounterValueLocalCurrency = 0

            CounterValueLocalCurrency = abs(Decimal(CounterValueLocalCurrency))

            if CounterValueLocalCurrency == 0:
                CounterValueFundCurrency = 0
                if Fx_FundCrncyVSfundCrncy == 0:
                    CounterValueFundCurrency = 0
                else:
                    CounterValueFundCurrency = Decimal(CounterValueLocalCurrency / Fx_FundCrncyVSfundCrncy)

            CounterValueFundCurrency = abs(Decimal(CounterValueFundCurrency))

        if WeightTarget == 0:
            if bbgFutContSize != 0:
                bbgFutContSize = 1
            CounterValueLocalCurrency = abs(
                Decimal((Decimal(Decimal(QuantityToAvoidShortSelling) * Bbg_Px_Last)) * bbgFutContSize))

            if Fx_FundCrncyVSfundCrncy == 0:
                CounterValueFundCurrency = 0
            else:
                CounterValueFundCurrency = abs(Decimal(CounterValueLocalCurrency / Fx_FundCrncyVSfundCrncy))

            Quantity = abs(Decimal(QuantityToAvoidShortSelling))

            try:
                if len(str(Quantity)) >= 6:
                    QuantityRounded1 = round(Decimal(Quantity), 4)
                elif len(str(Quantity)) >= 5:
                    QuantityRounded1 = round(Decimal(Quantity), 3)
                elif len(str(Quantity)) >= 4:
                    QuantityRounded1 = round(Decimal(Quantity), 2)
                else:
                    QuantityRounded1 = Decimal(Quantity)
            except:
                QuantityRounded1 = Decimal(Quantity)
            QuantityRounded = abs(QuantityRounded1)
        else:
            if Bbg_Px_Last == 0:
                CounterValueFundCurrency = 0
            else:
                CounterValueFundCurrency = Decimal(NAV_or_PCvalue * (WeightTarget - WeightActual))
            CounterValueFundCurrency = abs(Decimal(CounterValueFundCurrency))

            CounterValueLocalCurrency = abs(Decimal(CounterValueFundCurrency * Fx_FundCrncyVSfundCrncy))

            if CounterValueLocalCurrency == 0:
                Quantity = 0
            elif StockCurrency == "GBp" or StockCurrency == "ZAr":
                if ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Future Index" or \
                        ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Option Equity" or \
                        ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Option Index":
                    if ProductType_2ndCol == "Future Index":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice / 100 * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice / 100 * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice / 100 * bbgOptContSize)
                    else:
                        Quantity = 0
                elif ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Future Index" or \
                        ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Option Equity" or \
                        ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Option Index":
                    if ProductType_2ndCol == "Future Index":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100 * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100 * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100 * bbgOptContSize)
                    else:
                        Quantity = 0

                elif ProductType_2ndCol == "Bond":
                    if ProductType_2ndCol == "Future Index" or ProductType_2ndCol == "Option Equity" or \
                            ProductType_2ndCol == "Option Index":
                        PotentialError = "This calculation need to be checked, the contract size has not been considered"
                    Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100)

                else:
                    if ProductType_2ndCol == "Future Index" or ProductType_2ndCol == "Option Equity" \
                            or ProductType_2ndCol == "Option Index":
                        PotentialError = "This calculation need to be checked, the contract size has not been considered"
                    Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100)

            else:
                if ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Future Index" or \
                        ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Option Equity" or \
                        ExposureCalculationMethod == "Premium" and ProductType_2ndCol == "Option Index":
                    if ProductType_2ndCol == "Future Index":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        Quantity = CounterValueLocalCurrency / (UnderlyingPrice * bbgOptContSize)
                    else:
                        Quantity = 0
                elif ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Future Index" or \
                        ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Option Equity" or \
                        ExposureCalculationMethod == "Underlying" and ProductType_2ndCol == "Option Index":
                    if ProductType_2ndCol == "Future Index":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last * bbgFutContSize)
                    elif ProductType_2ndCol == "Option Equity":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last * bbgOptContSize)
                    elif ProductType_2ndCol == "Option Index":
                        Quantity = CounterValueLocalCurrency / (Bbg_Px_Last * bbgOptContSize)
                    else:
                        Quantity = 0

                elif ProductType_2ndCol == "Bond":
                    if ProductType_2ndCol == "Future Index" or ProductType_2ndCol == "Option Equity" or \
                            ProductType_2ndCol == "Option Index":
                        PotentialError = "This calculation need to be checked, the contract size has not been considered"
                    Quantity = CounterValueLocalCurrency / (Bbg_Px_Last / 100)

                else:
                    if ProductType_2ndCol == "Future Index" or ProductType_2ndCol == "Option Equity" or \
                            ProductType_2ndCol == "Option Index":
                        PotentialError = "This calculation need to be checked, the contract size has not been considered"
                    Quantity = CounterValueLocalCurrency / (Bbg_Px_Last)

        query1 = ("SELECT [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2,"
                  " [ENBROKER].[NAME] o3, [ENBROKER].[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, "
                  "[ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, [ENBROKER].[INFO] o9, "
                  "[ENBROKER].[MAILINGLIST] o10,[ENEXCHANGEBBG].[EXCHANGENAME] o49, [ENEXCHANGEBBG].[COUNTRY] o50,"
                  " [ENEXCHANGEBBG].[REGION] o51, [ENEXCHANGEBBG].[EXCHANGECODE] o52, [ENEXCHANGEBBG].[COMPOSITECODE] o53, "
                  "[ENEXCHANGEBBG].[MICCODE] o54, [ENTRADINGCOMBPS].[ID] o55, [ENTRADINGCOMBPS].[INSTRUMENT] o56, "
                  "[ENTRADINGCOMBPS].[BROKER] o57, [ENTRADINGCOMBPS].[BPS] o58, [ENTRADINGCOMBPS].[MIMIMUM] o59, "
                  "[ENTRADINGCOMBPS].[CENTPERSHARE] o60, [ENTRADINGCOMBPS].[NOTE] o61, [ENTRADINGCOMBPS].[COUNTRYID] o62, "
                  "[ENTRADINGCOMBPS].[EXCHANGEID] o63 FROM ((([outsys_prod].DBO.[OSUSR_BOL_TRADINGCOMBPS] "
                  "[ENTRADINGCOMBPS] Inner JOIN [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] ON"
                  " ([ENTRADINGCOMBPS].[BROKER] = [ENBROKER].[ID]))  Inner JOIN [outsys_prod].DBO.[OSUSR_BOL_COUNTRIESBBG]"
                  " [ENCOUNTRIESBBG] ON ([ENTRADINGCOMBPS].[COUNTRYID] = [ENCOUNTRIESBBG].[ID]))  Left JOIN "
                  "[outsys_prod].DBO.[OSUSR_BOL_EXCHANGEBBG] [ENEXCHANGEBBG] ON "
                  "([ENTRADINGCOMBPS].[EXCHANGEID] = [ENEXCHANGEBBG].[ID])) WHERE"
                  " ([ENTRADINGCOMBPS].[BROKER] = '" + str(BrokeriD) + "') AND"
                                                                       " (([ENEXCHANGEBBG].[EXCHANGECODE] = '" + str(
            Exchange) + "') OR"
                        " ([ENCOUNTRIESBBG].[COUNTRYISO] = '" + str(
            bbgCountryISO) + "')) ORDER BY [ENTRADINGCOMBPS].[INSTRUMENT] ASC ")
        cursor.execute(query1)
        resultGet1 = cursor.fetchall()
        for j, _ in enumerate(resultGet1):
            RG = resultGet1[j]
            BrokerBps = RG[20]
            BrokerCentPerShare = RG[22]
            QuantityRounded = Quantity
        if MEA_Multiplier != 1 or MEA_Multiplier != 0:
            Quantity = Quantity * MEA_Multiplier
            QuantityRounded = QuantityRounded * MEA_Multiplier

        CounterValueLocalCurrency = Hex_To_Decimal(CounterValueLocalCurrency)
        BrokerCommisionsBPS = CounterValueLocalCurrency * (BrokerBps / 10000)
        BrokerCommisionsCENT = QuantityRounded * Decimal(BrokerCentPerShare / 100)

        return CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded, \
               BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX
    except Exception as e:
        import sys
        import os
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(e))
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error on line no. : {exc_tb.tb_lineno} - {exc_type} - {fname}")
        raise 400


def TraderNotes(Ticker_ISIN, FundID):
    """

    This function used to get trader notes

    :param Ticker_ISIN:  TickerISIN
    :param FundID:  FundsID
    :return:  TraderNote

    """
    print("TraderNotes")
    conn = database_dev()
    cursor = conn.cursor()
    if not FundID or FundID == '':
        FundID = "NULL"
    TickerISIN = Ticker_ISIN.replace(" Equity", "").replace(" Corp", "").replace(" Index", "") \
        .replace(" Govt", "").replace(" Cmdty", "")
    query1 = ("SELECT [ENTRADERNOTES].[ID] o0, [ENTRADERNOTES].[TICKER_ISIN] o1, [ENTRADERNOTES].[NOTE] o2,"
              " [ENTRADERNOTES].[FUNDID] o3, [ENTRADERNOTES].[USERID] o4, [ENTRADERNOTES].[DATE] o5 "
              "FROM [outsys_prod].DBO.[OSUSR_BOL_TRADERNOTES] [ENTRADERNOTES] WHERE "
              "(([ENTRADERNOTES].[TICKER_ISIN] = '" + str(
        TickerISIN) + "') OR ([ENTRADERNOTES].[FUNDID] = " + str(FundID) + ")) ORDER BY"
                                                                           " [ENTRADERNOTES].[TICKER_ISIN] ASC")
    cursor.execute(query1)
    GetResult1 = cursor.fetchall()
    TraderNote = []
    for j, _ in enumerate(GetResult1):
        TraderNote_val = GetResult1[j][2]
        TraderNote.append(TraderNote_val)
    return TraderNote


def Order_Changes(OrderID, UserID, TimeCreation, TickerISIN, LimitPriceChanges, ExecutedQuantity, ChangeBroker,
                  NewBrokerShortCode):
    """

    In this function data inserting into OSUSR_SKP_ORDERCHANGES table.

    :param OrderID: order id
    :param UserID: GetUserId
    :param TimeCreation: CurrTime()
    :param TickerISIN: TickerISIN
    :param LimitPriceChanges: Limit
    :param ExecutedQuantity: 0
    :param ChangeBroker: False (Boolean type)
    :param NewBrokerShortCode: BrokerShortCode

    :return: return last object id as process id.

    """
    print("Order_Changes ---- last process")
    conn = database_dev()
    cursor = conn.cursor()
    CurrTime = get_current_date()
    ExecutedQuantity = Hex_To_Decimal(ExecutedQuantity)
    insert_q = (
            "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_ORDERCHANGES]([OrdersID],[TimeCreation],[TickerISIN],"
            "[LimitPriceChanges],[TimeOfChange],[ExecutedQuantity],[UserID],[BrokerHasBeenChanged],[BrokerShortCode])"
            "VALUES ('" + str(OrderID) + "','" + str(TimeCreation) + "','" + str(TickerISIN) + "','" + str(
        LimitPriceChanges) + "','" + str(CurrTime) + "','" + str(ExecutedQuantity) + "','" + str(UserID) + "','" + str(
        ChangeBroker) + "','" + str(NewBrokerShortCode) + "')")
    cursor.execute(insert_q)
    conn.commit()
    OSUSR_BOL_CONTROLTICKER_ISIN_ID = cursor.lastrowid
    return OSUSR_BOL_CONTROLTICKER_ISIN_ID


def Fund_Send(sStockName, TickerISIN, MultiplierQuantity, lastPrice, TradeQuantityPreciseIndicated, fund_id, side,
              userId, cashOrSwap, securityType, weight_Target, instructions, settleDate, suggestedBroker, limit,
              userName, urgency, expiry, userComment, fund, FundNumber, FundCurrency, ShowQuantityBox, isRepoLevEuro,
              StretegyID, repoExpiryDate):
    """

    This Process get called after api get triggered and return id once the order get inserted.

    :param sStockName:  stock name ( This is the stock name)
    :param TickerISIN:  TickerISIN (This is the ticker input para from form)
    :param MultiplierQuantity:  multiplier (This will define no. stocks want to purchase)
    :param lastPrice:  last price (Privious closing price)
    :param TradeQuantityPreciseIndicated:  quantity (Qauntity take from user)
    :param fund_id:  fund id initially must be None
    :param side:  BUY Or SELL
    :param userId:  user id (user who gonna purchase stock)
    :param cashOrSwap:  cash Or Swap (The transaction type for a new position will be CASH for long and SWAP for short.)
    :param securityType:  securityType
    :param weight_Target:  weight_Target (Max target price)
    :param instructions:  instructions (User given instructions)
    :param settleDate:   settleDate
    :param suggestedBroker:  suggestedBroker (Consultancy broker)
    :param limit: limit (set limit for stop loss)
    :param userName: userName
    :param urgency:  urgency Normal,High, Urgent
    :param expiry:  expiry
    :param userComment: user comment
    :param fund:  Fund Name
    :param FundNumber:  Fund Number
    :param FundCurrency:  Currency
    :param ShowQuantityBox:  ShowQuantityBox Boolean type

    :return: return id once the order get inserted.
    """
    print("Fund_Send")
    conn = database_dev()
    cursor = conn.cursor()
    Pair = False
    IsRepo = isRepoLevEuro
    RepoExpiryDate = repoExpiryDate if repoExpiryDate else '1900-01-01'
    StretegyID = StretegyID
    bol_wrongTicker,BolUndefinedMethodDeriv,Loading,processid= False,False,False,None
    Comment = userComment
    Multiplier = MultiplierQuantity
    ProductType_2ndCol, InstrumentType_1stCol = "", ""
    ForcingTtype = False  # Not sure about this variable
    gcashORswap = cashOrSwap
    IDcontactSuggestedBroker, PortfolioBrokerCode, NAV = suggestedBroker, None, 0  # need to find reference
    CurDate = get_current_date()
    Name = sStockName
    gUrgency = urgency
    Limit = limit
    WeightTarget = int(weight_Target)
    WeightActual,BrokerID = 0,0
    LineID = fund_id
    settleDate = settleDate if settleDate else '1900-01-01'
    # settleDate = settleDate if settleDate else '1900-01-01'

    user_given_quantity = TradeQuantityPreciseIndicated
    OptionExposureBox, ExposureCalculationMethod, BBGhttpError = False, None, False  # Those method have doubt
    TradeQuantityPreciseIndicated = round(MultiplierQuantity * TradeQuantityPreciseIndicated)
    TickerISIN = TickerISIN.replace("  Equity", " Equity").replace("  Corp", " Corp").replace("  Index",
                                                                                              " Index").replace(
        "  Govt", " Govt")
    GetSr3 = (
        "SELECT [ENSR].[ID] o0, [ENSR].[SUB_OR_RED] o1, [ENSR].[BUY_OR_SELL] o2, [ENSR].[TICKERCHECK] o3, [ENSR].[BBGHTTPERROR] o4, [ENSR].[ENHANCEDCASHREPORTDATE] o5, [ENSR].[FROMEMAILSERVICES] o6, [ENSR].[TIMERSERVICES] o7, [ENSR].[TESTMODE] o8, [ENSR].[COUNTERPARTY] o9, [ENSR].[DAYBACKENCASH] o10, [ENSR].[EMAILENHANCEDCASH] o11, [ENSR].[FILEPRICEMASTERNUMBER] o12, [ENSR].[SICAVNAME] o13, [ENSR].[MAILLISTID] o14, [ENSR].[DEFAULTBBGWST] o15, [ENSR].[RISKMANAGEMENTAPI] o16, [ENSR].[MODELPORTFOLIOSTRATEGY] o17, [ENSR].[MODELREQUESTSTATUS] o18, [ENSR].[CALCULATIONSTATUS] o19, [ENSR].[MODELBASKETNUMBER] o20, [ENSR].[STRATEGYDATE] o21, [ENSR].[STRATEGYTIME] o22, [ENSR].[PORTFOLIOID] o23, [ENSR].[ONGOINGCALCULATION] o24"
        " FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR]"
        " WHERE ([ENSR].[ID] = (1))"
        " ORDER BY [ENSR].[SUB_OR_RED] ASC ")
    cursor.execute(GetSr3)
    resultGetSr3 = cursor.fetchall()
    for i, _ in enumerate(resultGetSr3):
        resGetSr3 = resultGetSr3[i]
        if resGetSr3[8]:
            ToWithMax = "james.george@banorcapital.com; simone.cavallarin@banorcapital.com; david.howells@banorcapital.com; Oliver.Silvey@banorcapital.com; james.grant@banorcapital.com; wst01@banorcapital.com; wst02@banorcapital.com"
            ToTest = "simone.cavallarin@banorcapital.com; Oliver.Silvey@banorcapital.com; david.howells@banorcapital.com; james.grant@banorcapital.com; wenyan.hao@banorcapital.com"
            ToBanorCapital = "james.george@banorcapital.com; simone.cavallarin@banorcapital.com; Oliver.Silvey@banorcapital.com; david.howells@banorcapital.com; james.grant@banorcapital.com; wst01@banorcapital.com; wst02@banorcapital.com; wenyan.hao@banorcapital.com"
            ToTomNick = ""
            ToJamesMorton = ""
            ToKallisto = ""
        else:
            ToWithMax = "james.george@banorcapital.com; lorenzo.bombarda@banorcapital.com;  simone.cavallarin@banorcapital.com; giacomo.mergoni@banorcapital.com; massimiliano.cagliero@banor.it; david.howells@banorcapital.com; andrew.sandler@banorcapital.com; banorsicav@banorcapital.com; james.george349@gmail.com; wenyan.hao@banorcapital.com"
            ToTest = "james.george@banorcapital.com; simone.cavallarin@banorcapital.com; david.howells@banorcapital.com; james.grant@banorcapital.com"
            ToBanorCapital = "james.george@banorcapital.com; lorenzo.bombarda@banorcapital.com;  simone.cavallarin@banorcapital.com; giacomo.mergoni@banorcapital.com; david.howells@banorcapital.com; banorsicav@banorcapital.com; wenyan.hao@banorcapital.com "
            ToTomNick = "tomaso.mariotti@banor.it; nicolo.digiacomo@banor.it"
            ToJamesMorton = "jamesm@slam.com.sg"
            ToKallisto = "luca.clementoni@kallistopartners.com; Andrea.Federici@banorcapital.com"
            BEB_emailList = "davide.verardi@banor.it; Tomaso.Mariotti@banor.it; nicolo.digiacomo@banor.it"

    if ShowQuantityBox and TradeQuantityPreciseIndicated == 0:
        msg = 'Please double check the quantity'
        return msg
    else:
        logger_sqlalchemy = logg_Handler(fund, logger_name='ShowQuantityBox and TradeQuantityPreciseIndicated check ')
        logger_sqlalchemy.info("ShowQuantityBox and TradeQuantityPreciseIndicated is false")
        if ShowQuantityBox:
            OrderQtyType = "Quantity"
        else:
            OrderQtyType = "% Exp of Nav"
            if OptionExposureBox:
                if ExposureCalculationMethod == "None" or ExposureCalculationMethod == "":
                    BolUndefinedMethodDeriv = True

            if BBGhttpError:
                GetSrs = ("SELECT  top 1 * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] order by id desc")
                cursor.execute(GetSrs)
                resultGetSrs = cursor.fetchall()
                for i, _ in enumerate(resultGetSrs):
                    resGetSrs = resultGetSrs[i]
                    BBGhttpError = resGetSrs[4]
                    if isRepoLevEuro:
                        ProductType_2ndCol = 'REPO'
                        InstrumentType_1stCol = 'C'
            else:
                GetSrs2 = ("SELECT top 1  * FROM [outsys_prod].DBO.[OSUSR_38P_SR] [ENSR] order by id desc")
                cursor.execute(GetSrs2)
                resultGetSrs2 = cursor.fetchall()
                for gs2, _ in enumerate(resultGetSrs2):
                    resGetSrs2 = resultGetSrs2[gs2]
                    BBGhttpError = resGetSrs2[4]

                Http_Server_wst01_repose = Http_Server_wst01(TickerISIN, fund)
                if isinstance(Http_Server_wst01_repose, dict):
                    sStockName = Http_Server_wst01_repose['Name']
                    bbgCrncy = Http_Server_wst01_repose['Crncy']
                    bbgCountry = Http_Server_wst01_repose['Country']
                    bbg_Id_Isin = Http_Server_wst01_repose['Id_Isin']
                    bbg_Security_Typ = Http_Server_wst01_repose['Security_typ']
                    bbg_Ticker_And_Exch_Code = Http_Server_wst01_repose['Ticker_And_Exch_Code']
                    bbg_Country_Iso = Http_Server_wst01_repose['Country_Iso']
                    bbg_Country_Full_Name = Http_Server_wst01_repose['Country_Full_Name']
                    bbg_Issuer_Industry = Http_Server_wst01_repose['Issuer_Industry']
                    bbg_Parent_Ticker_Exchange = Http_Server_wst01_repose['Parent_Ticker_Exchange']
                    bbg_Px_Last = Http_Server_wst01_repose['Px_Last']
                    ShowPriceBox = True
                    TickerISIN = Http_Server_wst01_repose['TickerIsinUpperCase']
                    bbg_SecurityName = Http_Server_wst01_repose['SecurityName']
                    bbg_Exchange = Http_Server_wst01_repose['Eqy_Prim_Exch_Shrt']
                    bbg_SettleDate = Http_Server_wst01_repose['Settle_Date']
                    bbg_OptContSize = Http_Server_wst01_repose['Opt_Cont_Size']
                    bbg_FutContSize = Http_Server_wst01_repose['Fut_Cont_Size']
                    bbg_IDUltimateParentCo = Http_Server_wst01_repose['Id_bb_Ultimate_Parent_Co']
                    bbg_lotSize = Http_Server_wst01_repose['lot_size']
                    bbg_MarketStartTime = Http_Server_wst01_repose['StartTime']
                    bbg_MarketEndTime = Http_Server_wst01_repose['EndTime']
                    bbg_UnderlyingPrice = Http_Server_wst01_repose['UnderlayPrcie']
                    bbg_MaturityDate = Http_Server_wst01_repose['MaturityDate']
                    bbg_IDUltimateParentCoName = Http_Server_wst01_repose['Id_bb_Ultimate_Parent_Co_Name']
                    bbg_Name = Http_Server_wst01_repose['Name']
                    bbg_Short_Name = Http_Server_wst01_repose['Short_Name']
                    bbg_Market_Sector_Des = Http_Server_wst01_repose['Market_sector_des']
                    bbg_Chg_Pct_Ytd = Http_Server_wst01_repose['Chg_Pct_Ytd']
                    bbg_TickerIsinUpperCase = Http_Server_wst01_repose['TickerIsinUpperCase']
                    bbg_Eqy_Sh_Out = Http_Server_wst01_repose['Eqy_sh_out']
                    bbg_Amt_Outstanding = Http_Server_wst01_repose['Amt_Outstanding']
                    bbg_Volume_Avg_20D = Http_Server_wst01_repose['Volume_avg_20d']
                    bbg_Cntry_Of_Risk = Http_Server_wst01_repose['Cntry_of_Risk']
                    bbg_Payment_Rank = Http_Server_wst01_repose['Payment_Rank']
                    bbg_Capital_Contingent_Security = Http_Server_wst01_repose['Capital_Contingent_Security']
                    bbg_Registered_Country_Location = Http_Server_wst01_repose['Registered_Country_Location']
                    bbg_Country_Of_Largest_Revenue = Http_Server_wst01_repose['Country_of_largest_Revenue']
                    bbg_Underlying_Isin = Http_Server_wst01_repose['Underlying_Isin']
                    bbg_Opt_Strike_Px = Http_Server_wst01_repose['Opt_strike_Px']
                    bbg_Next_Call_Dt = Http_Server_wst01_repose['Next_Call_Dt']

                    GetControlTickerISINsByTickerRequested = (
                            "SELECT [ENCONTROLTICKER_ISIN].[ID] o0, [ENCONTROLTICKER_ISIN].[BOLEANAPIREQUESTSENT] o1, [ENCONTROLTICKER_ISIN].[BOLEANAPIREQUESTSENTAGAIN] o2, [ENCONTROLTICKER_ISIN].[APIDATETIMEREQUEST] o3, [ENCONTROLTICKER_ISIN].[TICKERREQUESTED] o4, [ENCONTROLTICKER_ISIN].[DATETIME] o5, [ENCONTROLTICKER_ISIN].[ERROR] o6, [ENCONTROLTICKER_ISIN].[SECURITYNAME] o7, [ENCONTROLTICKER_ISIN].[NAME] o8, [ENCONTROLTICKER_ISIN].[SHORT_NAME] o9, [ENCONTROLTICKER_ISIN].[CRNCY] o10,"
                            " [ENCONTROLTICKER_ISIN].[COUNTRY] o11, [ENCONTROLTICKER_ISIN].[ID_ISIN] o12, [ENCONTROLTICKER_ISIN].[MARKET_SECTOR_DES] o13, [ENCONTROLTICKER_ISIN].[SECURITY_TYP] o14, [ENCONTROLTICKER_ISIN].[TICKER_AND_EXCH_CODE] o15, [ENCONTROLTICKER_ISIN].[COUNTRY_ISO] o16, [ENCONTROLTICKER_ISIN].[COUNTRY_FULL_NAME] o17, [ENCONTROLTICKER_ISIN].[ISSUER_INDUSTRY] o18, [ENCONTROLTICKER_ISIN].[PARENT_TICKER_EXCHANGE] o19, [ENCONTROLTICKER_ISIN].[CHG_PCT_YTD] o20, [ENCONTROLTICKER_ISIN].[PX_LAST] o21, [ENCONTROLTICKER_ISIN].[TICKERISINUPPERCASE] o22, [ENCONTROLTICKER_ISIN].[EQY_PRIM_EXCH_SHRT] o23, [ENCONTROLTICKER_ISIN].[SETTLE_DATE] o24, [ENCONTROLTICKER_ISIN].[OPT_CONT_SIZE] o25, [ENCONTROLTICKER_ISIN].[FUT_CONT_SIZE] o26, [ENCONTROLTICKER_ISIN].[ID_BB_ULTIMATE_PARENT_CO] o27, [ENCONTROLTICKER_ISIN].[MATURITYDATE] o28, [ENCONTROLTICKER_ISIN].[STARTTIME] o29, [ENCONTROLTICKER_ISIN].[ENDTIME] o30, [ENCONTROLTICKER_ISIN].[UNDERLAYPRCIE] o31, [ENCONTROLTICKER_ISIN].[LOT_SIZE] o32, [ENCONTROLTICKER_ISIN].[ID_BB_ULTIMATE_PARENT_CO_NAM] o33, [ENCONTROLTICKER_ISIN].[CURRENCY1] o34, [ENCONTROLTICKER_ISIN].[CURRENCY2] o35, [ENCONTROLTICKER_ISIN].[HIST_PX_ENDMONTH] o36, [ENCONTROLTICKER_ISIN].[HIST_ERROR] o37, [ENCONTROLTICKER_ISIN].[HIST_DATE] o38, [ENCONTROLTICKER_ISIN].[HIST_BOLAPIREQUESTSENT] o39, [ENCONTROLTICKER_ISIN].[HIST_BOLAPIREQUESTSENTAG] o40, [ENCONTROLTICKER_ISIN].[EQY_SH_OUT] o41, [ENCONTROLTICKER_ISIN].[AMT_OUTSTANDING] o42, [ENCONTROLTICKER_ISIN].[VOLUME_AVG_20D] o43, [ENCONTROLTICKER_ISIN].[CNTRY_OF_RISK] o44, [ENCONTROLTICKER_ISIN].[PAYMENT_RANK] o45, [ENCONTROLTICKER_ISIN].[CAPITAL_CONTINGENT_SECURITY] o46, [ENCONTROLTICKER_ISIN].[REGISTERED_COUNTRY_LOCATION] o47, [ENCONTROLTICKER_ISIN].[COUNTRY_OF_LARGEST_REVENUE] o48, [ENCONTROLTICKER_ISIN].[UNDERLYING_ISIN] o49, [ENCONTROLTICKER_ISIN].[OPT_STRIKE_PX] o50, [ENCONTROLTICKER_ISIN].[MATURITY_DATE_ESTIMATED] o51, [ENCONTROLTICKER_ISIN].[NEXT_CALL_DT] o52, [ENCONTROLTICKER_ISIN].[DELTA] o53, [ENCONTROLTICKER_ISIN].[OPT_PUT_CALL] o54, [ENCONTROLTICKER_ISIN].[ERRORTEXT] o55, [ENCONTROLTICKER_ISIN].[INDUSTRY_GROUP] o56, [ENCONTROLTICKER_ISIN].[INDUSTRY_SECTOR] o57, [ENCONTROLTICKER_ISIN].[VOLUME_AVG_30D] o58, [ENCONTROLTICKER_ISIN].[CUR_MKT_CAP] o59, [ENCONTROLTICKER_ISIN].[VOLATILITY_30D] o60, [ENCONTROLTICKER_ISIN].[EQY_BETA] o61, [ENCONTROLTICKER_ISIN].[EQY_ALPHA] o62, [ENCONTROLTICKER_ISIN].[COUPON] o63, [ENCONTROLTICKER_ISIN].[ACCRUEDINTEREST] o64, [ENCONTROLTICKER_ISIN].[YELD] o65 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_CONTROLTICKER_ISIN] [ENCONTROLTICKER_ISIN] "
                            "WHERE ([ENCONTROLTICKER_ISIN].[TICKERREQUESTED] = '" + str(
                        TickerISIN) + "')")
                    current_date = get_current_date()
                    cursor.execute(GetControlTickerISINsByTickerRequested)
                    rGetControlTickerISINsByTickerRequested = cursor.fetchall()
                    if rGetControlTickerISINsByTickerRequested:
                        updateContrTick = (
                                "UPDATE [outsys_prod].DBO.[OSUSR_BOL_CONTROLTICKER_ISIN] set [BOLEANAPIREQUESTSENT] = 0, [BOLEANAPIREQUESTSENTAGAIN] = 0, [APIDATETIMEREQUEST] ='" + str(
                            CurDate) + "', [TICKERREQUESTED] = '" + str(
                            TickerISIN) + "', [DATETIME] = '" + str(
                            current_date) + "', [ERROR] = 0, [SECURITYNAME] = '" + str(
                            bbg_SecurityName) + "', [NAME] = '" + str(bbg_Name) + "', [SHORT_NAME] = '" + str(
                            bbg_Short_Name) + "', [CRNCY] = '" + str(bbgCrncy) + "', [COUNTRY] = '" + str(
                            bbgCountry) + "', [ID_ISIN] = '" + str(
                            bbg_Id_Isin) + "', [MARKET_SECTOR_DES] = '" + str(
                            bbg_Market_Sector_Des) + "', [SECURITY_TYP] = '" + str(
                            bbg_Security_Typ) + "', [TICKER_AND_EXCH_CODE] = '" + str(
                            bbg_Ticker_And_Exch_Code) + "', [COUNTRY_ISO] = '" + str(
                            bbg_Country_Iso) + "', [COUNTRY_FULL_NAME]= '" + str(
                            bbg_Country_Full_Name) + "', [ISSUER_INDUSTRY] = '" + str(
                            bbg_Issuer_Industry) + "', [PARENT_TICKER_EXCHANGE] = '" + str(
                            bbg_Parent_Ticker_Exchange) + "', [CHG_PCT_YTD] = '" + str(
                            bbg_Chg_Pct_Ytd) + "', [PX_LAST] = '" + str(
                            bbg_Px_Last) + "', [TICKERISINUPPERCASE] = '" + str(
                            bbg_TickerIsinUpperCase) + "', [EQY_PRIM_EXCH_SHRT] = '" + str(
                            bbg_Exchange) + "', [SETTLE_DATE] = '" + str(
                            bbg_SettleDate) + "', [OPT_CONT_SIZE] = '" + str(
                            bbg_OptContSize) + "', [FUT_CONT_SIZE] = '" + str(
                            bbg_FutContSize) + "', [ID_BB_ULTIMATE_PARENT_CO]= '" + str(
                            bbg_IDUltimateParentCo) + "', [MATURITYDATE] = '" + str(
                            bbg_MaturityDate) + "', [STARTTIME] = '" + str(
                            bbg_MarketStartTime) + "', [ENDTIME] = '" + str(
                            bbg_MarketEndTime) + "', [UNDERLAYPRCIE] ='" + str(
                            bbg_UnderlyingPrice) + "', [LOT_SIZE] ='" + str(
                            bbg_lotSize) + "', [ID_BB_ULTIMATE_PARENT_CO_NAM] = '" + str(
                            bbg_IDUltimateParentCoName) + "',[HIST_PX_ENDMONTH] = 0, [HIST_ERROR] = 0, [HIST_BOLAPIREQUESTSENT] = 0, [HIST_BOLAPIREQUESTSENTAG] = 0, [EQY_SH_OUT] = '" + str(
                            bbg_Eqy_Sh_Out) + "', [AMT_OUTSTANDING] = '" + str(
                            bbg_Amt_Outstanding) + "', [VOLUME_AVG_20D] = '" + str(
                            bbg_Volume_Avg_20D) + "', [CNTRY_OF_RISK] = '" + str(
                            bbg_Cntry_Of_Risk) + "', [PAYMENT_RANK] = '" + str(
                            bbg_Payment_Rank) + "', [CAPITAL_CONTINGENT_SECURITY] = '" + str(
                            bbg_Capital_Contingent_Security) + "', [REGISTERED_COUNTRY_LOCATION] = '" + str(
                            bbg_Registered_Country_Location) + "', [COUNTRY_OF_LARGEST_REVENUE] = '" + str(
                            bbg_Country_Of_Largest_Revenue) + "', [UNDERLYING_ISIN] = '" + str(
                            bbg_Underlying_Isin) + "', [OPT_STRIKE_PX] = '" + str(
                            bbg_Opt_Strike_Px) + "', [MATURITY_DATE_ESTIMATED] = '" + str(
                            bbg_MaturityDate) + "', [NEXT_CALL_DT] = '" + str(
                            bbg_Next_Call_Dt) + "' Where ([TICKERREQUESTED] = '" + str(TickerISIN) + "')")
                        print(updateContrTick)
                        cursor.execute(updateContrTick)
                        conn.commit()
                    else:
                        iGetControlTickerISINsByTickerRequested = (
                                "INSERT INTO [outsys_prod].DBO.[OSUSR_BOL_CONTROLTICKER_ISIN]([BOLEANAPIREQUESTSENT],"
                                "[BOLEANAPIREQUESTSENTAGAIN],[APIDATETIMEREQUEST],[TICKERREQUESTED],[DATETIME],[ERROR],"
                                "[SECURITYNAME],[NAME],[SHORT_NAME],[CRNCY],[COUNTRY],[ID_ISIN],[MARKET_SECTOR_DES],"
                                "[SECURITY_TYP],[TICKER_AND_EXCH_CODE],[COUNTRY_ISO],[COUNTRY_FULL_NAME],[ISSUER_INDUSTRY]"
                                ",[PARENT_TICKER_EXCHANGE],[CHG_PCT_YTD],[PX_LAST],[TICKERISINUPPERCASE],[EQY_PRIM_EXCH_SHRT],"
                                "[SETTLE_DATE],[OPT_CONT_SIZE],[FUT_CONT_SIZE],[ID_BB_ULTIMATE_PARENT_CO],[MATURITYDATE]"
                                ",[STARTTIME],[ENDTIME],[UNDERLAYPRCIE],[LOT_SIZE],[ID_BB_ULTIMATE_PARENT_CO_NAM],[HIST_PX_ENDMONTH],"
                                "[HIST_ERROR],[HIST_BOLAPIREQUESTSENT],[HIST_BOLAPIREQUESTSENTAG],[EQY_SH_OUT],[AMT_OUTSTANDING],"
                                "[VOLUME_AVG_20D],[CNTRY_OF_RISK],[PAYMENT_RANK],[CAPITAL_CONTINGENT_SECURITY],[REGISTERED_COUNTRY_LOCATION],"
                                "[COUNTRY_OF_LARGEST_REVENUE],[UNDERLYING_ISIN],[OPT_STRIKE_PX],[MATURITY_DATE_ESTIMATED],[NEXT_CALL_DT])"
                                "values(0,0,'" + str(CurDate) + "','" + str(TickerISIN) + "','" + str(
                            current_date) + "',0,'" + str(bbg_SecurityName) + "','" + str(bbg_Name) + "','" + str(
                            bbg_Short_Name) + "','" + str(bbgCrncy) + "','" + str(bbgCountry) + "','" + str(
                            bbg_Id_Isin) + "','" + str(bbg_Market_Sector_Des) + "','" + str(
                            bbg_Security_Typ) + "','" + str(bbg_Ticker_And_Exch_Code) + "','" + str(
                            bbg_Country_Iso) + "','" + str(bbg_Country_Full_Name) + "','" + str(
                            bbg_Issuer_Industry) + "','" + str(bbg_Parent_Ticker_Exchange) + "','" + str(
                            bbg_Chg_Pct_Ytd) + "','" + str(bbg_Px_Last) + "','" + str(
                            bbg_TickerIsinUpperCase) + "','" + str(bbg_Exchange) + "','" + str(
                            bbg_SettleDate) + "','" + str(bbg_OptContSize) + "','" + str(bbg_FutContSize) + "','" + str(
                            bbg_IDUltimateParentCo) + "','" + str(bbg_MaturityDate) + "','" + str(
                            bbg_MarketStartTime) + "','" + str(bbg_MarketEndTime) + "','" + str(
                            bbg_UnderlyingPrice) + "','" + str(bbg_lotSize) + "','" + str(
                            bbg_IDUltimateParentCoName) + "',0,0,0,0,'" + str(bbg_Eqy_Sh_Out) + "','" + str(
                            bbg_Amt_Outstanding) + "','" + str(bbg_Volume_Avg_20D) + "','" + str(
                            bbg_Cntry_Of_Risk) + "','" + str(bbg_Payment_Rank) + "','" + str(
                            bbg_Capital_Contingent_Security) + "','" + str(
                            bbg_Registered_Country_Location) + "','" + str(
                            bbg_Country_Of_Largest_Revenue) + "','" + str(bbg_Underlying_Isin) + "','" + str(
                            bbg_Opt_Strike_Px) + "','" + str(bbg_MaturityDate) + "','" + str(bbg_Next_Call_Dt) + "') "
                        )
                        print(iGetControlTickerISINsByTickerRequested)
                        cursor.execute(iGetControlTickerISINsByTickerRequested)
                        conn.commit()
                        # BBG Refresh here
                    if LineID is None or ProductType_2ndCol == "":
                        resCol1_col2_with_auto_correction = col1_col2_with_auto_correction(Ticker=TickerISIN,
                                                                                           FundNumber=FundNumber,
                                                                                           Col_1_Ready=InstrumentType_1stCol,
                                                                                           CurrencyIN=bbgCrncy,
                                                                                           ForcingInstrumentTypecol1=ForcingTtype,
                                                                                           FundName=fund)
                        InstrumentType_1stCol = resCol1_col2_with_auto_correction[0]
                        ProductType_2ndCol = resCol1_col2_with_auto_correction[1]
                        if InstrumentType_1stCol != "" and ProductType_2ndCol != "":
                            gDateTime = CurDate
                            OrderStage = "Pending Suggestion"
                        else:
                            resCol_1_and_2_Using_Ticker_2 = Col_1_and_2_Using_Ticker_2(Ticker=TickerISIN,
                                                                                       Col_1_Ready=InstrumentType_1stCol)
                            InstrumentType_1stCol = resCol_1_and_2_Using_Ticker_2[0]
                            ProductType_2ndCol = resCol_1_and_2_Using_Ticker_2[1]
                            if InstrumentType_1stCol != "" and ProductType_2ndCol != "":
                                gDateTime = CurDate
                                OrderStage = "Pending Suggestion"
                            else:
                                Col_1_and_2_Using_Weight_3_rec = Col_1_and_2_Using_Weight_3(Ticker=TickerISIN,
                                                                                            Weight=WeightTarget,
                                                                                            CashORswap=gcashORswap,
                                                                                            StockName=sStockName,
                                                                                            BbgSecurityTyp=bbg_Security_Typ,
                                                                                            FundName=fund)
                                InstrumentType_1stCol = Col_1_and_2_Using_Weight_3_rec[0]
                                ProductType_2ndCol = Col_1_and_2_Using_Weight_3_rec[1]
                    gDateTime = CurDate
                    OrderStage = "Pending Suggestion"
                    if InstrumentType_1stCol == "C" and ProductType_2ndCol == "Equity" or InstrumentType_1stCol == "C" \
                            and ProductType_2ndCol == "Equity - REIT" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Equity - Savings Share" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Equity - ADR" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "ETF" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Warrant" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Equity - Right" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Fund" or InstrumentType_1stCol == "C" and ProductType_2ndCol == "Equity - MLP" \
                            or InstrumentType_1stCol == "C" and ProductType_2ndCol == "Equity - Right" or \
                            InstrumentType_1stCol == "C" and ProductType_2ndCol == "Equity Preference" or InstrumentType_1stCol == "C" \
                            and ProductType_2ndCol == "Equity A-Shares" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Equity B-Shares" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Dutch Cert" or InstrumentType_1stCol == "C" and \
                            ProductType_2ndCol == "Equity - GDR" or InstrumentType_1stCol == "C" \
                            and ProductType_2ndCol == "Equity - Receipt":

                        BrokerName, BrokerShortCode, Custodian, Account, C4FBrokerName, BrokerID, BrokerType_A_B, \
                        NeedComment, BrokerSelReason, ExecutorFactor_Cost, JP_MorganAccount = \
                            Trading_Equity_Fund_ETF_Cash(FundNumber=FundNumber, Ticker=TickerISIN,
                                                         IDcontactSuggestedBroker=IDcontactSuggestedBroker,
                                                         Col_2=ProductType_2ndCol,
                                                         PortfolioBrokerCode=PortfolioBrokerCode)

                    else:
                        if InstrumentType_1stCol == "C" and ProductType_2ndCol == "Bond":
                            BrokerName, BrokerShortCode, Custodian, C4FBrokerName, Account, BrokerID, \
                            BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost = Trading_Bond_Cash(
                                FundNumber=FundNumber, Ticker=TickerISIN,
                                IDcontactSuggestedBroker=IDcontactSuggestedBroker,
                                PortfolioBrokerCode=PortfolioBrokerCode)
                        else:
                            if InstrumentType_1stCol == "SW":
                                BrokerName, BrokerShortCode, Custodian, Account, C4FBroker, \
                                BrokerID, \
                                BrokerType_A_B, NeedComment, BrokerSelReason, ExecutorFactor_Cost, JP_MorganAccount \
                                    = Trading_Swap(FundNumber=FundNumber,
                                                   IDcontactSuggestedBroker=IDcontactSuggestedBroker,
                                                   PortfolioBrokerCode=PortfolioBrokerCode if PortfolioBrokerCode else "")
                            else:
                                if InstrumentType_1stCol == "C" and ProductType_2ndCol == "Option Equity" or \
                                        "InstrumentType_1stCol" == "C" and ProductType_2ndCol == "Option Index" or \
                                        InstrumentType_1stCol == "C" and ProductType_2ndCol == "Future Index" or \
                                        InstrumentType_1stCol == "C" and ProductType_2ndCol == "Future Bond" or \
                                        InstrumentType_1stCol == "C" and ProductType_2ndCol == "Single stock future":
                                    BrokerName, BrokerShortCode, C4FBrokerName, BrokerID, BrokerType_A_B, NeedComment, BrokerSelReason, \
                                    ExecutorFactor_Cost, Custodian, Account, C4FBroker = Trading_Derivative_Cash(
                                        FundNumber=FundNumber,
                                        PortfolioBrokerCode=PortfolioBrokerCode)
                                else:
                                    BrokerName = "Broker has not been identified"
                                    Custodian = ""
                                    Account = ""

                    # Ajax refresh here

                    ArborProductType_ = CodesGeneration_Arbor(Col_1=InstrumentType_1stCol, Col_2=ProductType_2ndCol)

                    out_BrokerShortCode, out_BrokerID, out_BrokerName, out_C4F_BrokerName, out_JP_MorganAccount, BrokerType_A_B, \
                    NeedComment, BrokerSelReason, ExecutorFactor_Cost, ExposureTradeID, out_UBS_Account, out_LeiReportingCode, \
                    out_BrokerCodeLevEuro, out_masterAgreement, out_masterAgreementVersionDate = \
                        Broker_Selection(FundCode=FundNumber, InstrType=InstrumentType_1stCol,
                                         SecurityType=ProductType_2ndCol,
                                         Ticker=TickerISIN, BrokerID_fromPortfolio=None,
                                         BrokerCode_fromPortfolio=PortfolioBrokerCode,
                                         BrokerSuggestedByUser=IDcontactSuggestedBroker, FX=False, Bol_BNP=False,
                                         Bol_BDL=False)
                    BrokerName = out_BrokerName
                    BrokerShortCode = out_BrokerShortCode
                    ArborProductType = ArborProductType_
                    Casa4FundsCode = CodesGeneration_Casa_4_Funds(Col_1=InstrumentType_1stCol,
                                                                  Col_2=ProductType_2ndCol)
                    C4FSecurityType = Casa4FundsCode
                    t_plus = Value_Date_Generator(Col_1=InstrumentType_1stCol, Col_2=ProductType_2ndCol,
                                                  Ticker=TickerISIN)
                    ValueDate = datetime.now()
                    ValueDate = ValueDate + timedelta(days=t_plus)
                    ValueDate = ValueDate.strftime("%Y-%m-%d")
                    API_Command = Owl_Api(BBG_BrokerCode=BrokerShortCode, Col_1=InstrumentType_1stCol,
                                          Col_2=ProductType_2ndCol)

                    # Line Id is nothing but id for NALineID ans so on
                    # LineID = None
                    FundCode = FundNumber
                    Ticker_ISIN = TickerISIN
                    Weight_Target, xPairTrade = 0, 0  # Need to find out its reference
                    database_name = None

                    if FundNumber == 2:
                        database_name = 'OSUSR_38P_NORTHAMERICALS'
                        col_name = 'NorthAmericaLS_ID'
                        To = ToWithMax
                        FundName = "BANOR SICAV NORTH AMERICA LONG SHORT EQUITY"
                        Fund = "BANOR SICAV NORTH AMERICA LONG SHORT EQUITY"
                        FundNameShort = "North America l/s"
                    elif FundNumber == 3:
                        database_name = 'OSUSR_38P_GREATERCHINALS'
                        col_name = 'GREATERCHINALS_ID'
                        To = ToBanorCapital
                        FundName = "BANOR SICAV GREATER CHINA LONG SHORT EQUITY"
                        Fund = "BANOR SICAV GREATER CHINA LONG SHORT EQUITY"
                        FundNameShort = "Greater China l/s"
                    elif FundNumber == 1:
                        database_name = 'OSUSR_38P_ITALYLS'
                        col_name = 'ItalyLS_ID'
                        To = ToBanorCapital
                        FundName = "BANOR SICAV ITALY LONG SHORT EQUITY"
                        Fund = "BANOR SICAV ITALY LONG SHORT EQUITY"
                        FundNameShort = "Italy l/s"
                    elif FundNumber == 5:
                        database_name = 'OSUSR_38P_EUROPEANVALUE'
                        col_name = 'EUROPENVALUE_ID'
                        FundName = "BANOR SICAV EUROPEAN VALUE"
                        Fund = "BANOR SICAV EUROPEAN VALUE"
                        FundNameShort = "European Value"
                    elif FundNumber == 8:
                        database_name = 'OSUSR_BOL_CHIRON'
                        col_name = 'CHIRON_ID'
                        To = ToTomNick
                        FundName = "ARISTEA SICAV CHIRON"
                        Fund = "ARISTEA SICAV CHIRON"
                        FundNameShort = "ARISTEA_CHIRON"
                    elif FundNumber == 4:
                        database_name = 'OSUSR_BOL_EUROBOND'
                        To = BEB_emailList
                        col_name = 'EUROBOND_ID'
                        FundName = "BANOR SICAV EURO BOND"
                        Fund = "BANOR SICAV EURO BOND"
                        FundNameShort = "Euro Bond"
                    elif FundNumber == 10:
                        database_name = 'OSUSR_BOL_NEWFRONTIERS'
                        To = ToBanorCapital
                        col_name = 'NEWFRONTIERSID'
                        FundName = "ARISTEA SICAV NEW FRONTIERS"
                        Fund = "ARISTEA SICAV NEW FRONTIERS"
                        FundNameShort = "New Frontiers"
                    elif FundNumber == 6:
                        database_name = 'OSUSR_BOL_ROSEMARY'
                        To = ToKallisto
                        col_name = 'ROSEMARYID'
                        FundName = "BANOR SICAV ROSEMARY"
                        Fund = "BANOR SICAV ROSEMARY"
                        FundNameShort = "Rosemary"
                    elif FundNumber == 11:
                        database_name = 'OSUSR_BOL_ASIANALPHA'
                        To = ToJamesMorton
                        col_name = 'ASIANALPHAID'
                        FundName = "ARISTEA SICAV ASIAN ALPHA"
                        Fund = "ASIAN ALPHA"
                        FundNameShort = "AsianAlpha"
                    elif FundNumber == 12:
                        database_name = 'OSUSR_SKP_HIGHFOCUS'
                        To = ToBanorCapital
                        col_name = 'HIGHFOCUSID'
                        FundName = "BANOR A. A. S. HIGH FOCUS"
                        Fund = "BANOR A. A. S. HIGH FOCUS"
                        FundNameShort = "HighFocus"
                    elif FundNumber == 13:
                        database_name = 'OSUSR_SKP_MEAOPPORTUNITIES'
                        To = ToBanorCapital
                        col_name = 'MEAOPPORTUNITIESID'
                        FundName = "ARISTEA SICAV M&A OPPORTUNITIES"
                        Fund = "M&A Opportunities"
                        FundNameShort = "M&A Opportunities"
                    elif FundNumber == 14:
                        database_name = 'OSUSR_SKP_LEVEURO'
                        To = ToBanorCapital
                        col_name = 'LEVEUROID'
                        FundName = "LevEuro"
                        Fund = "LevEuro"
                        FundNameShort = "LevEuro"
                    elif FundNumber == 16:
                        database_name = 'OSUSR_SKP_ASIMOCCO'
                        To = ToBanorCapital
                        col_name = 'ASIMOCCOID'
                        FundName = "Assimoco"
                        Fund = "Assimoco"
                        FundNameShort = "Assimoco"
                    elif FundNumber == 18:
                        database_name = 'OSUSR_SKP_RAFFAELLO'
                        To = ToBanorCapital
                        col_name = 'RAFFAELLO'
                        FundName = "Raffaello"
                        Fund = "Raffaello"
                        FundNameShort = "Raffaello"
                    elif FundNumber == 7:
                        database_name = 'OSUSR_SKP_Imola'
                        To = ToBanorCapital
                        col_name = 'IMOLAID'
                        FundName = "Imola"
                        Fund = "Imola"
                        FundNameShort = "Imola"
                    elif FundNumber == 9:
                        database_name = 'OSUSR_SKP_Montecuccoli '
                        To = ToBanorCapital
                        col_name = 'MONTECUCCOLIID'
                        FundName = "Montecuccoli"
                        Fund = "Montecuccoli"
                        FundNameShort = "Montecuccoli"

                    # perform operation for region
                    qepCurrdate = get_current_date()
                    if ForcingTtype:
                        xSystemMessageAutoGenerated = "New Instr Type for a name that we already hold."
                    else:
                        if LineID is None:
                            query1 = ("SELECT * FROM [outsys_prod].DBO." + database_name + " "
                                                                                              "WHERE ([DATE] = (convert(datetime, substring(" + "'" + str(
                                qepCurrdate) + "'" + ", 1, 10), 120))) AND"
                                                     " ([TICKER_ISIN] = " + "'" + str(
                                Ticker_ISIN) + "'" + ") ORDER BY [NAME] ASC ")
                            cursor.execute(query1)
                            GetAllRecords = cursor.fetchall()
                            if len(GetAllRecords) > 1:
                                OrderStage = "Pending with error!"
                                subject = "The last trade is using a name that is contained two times in the portfolio!"
                                Title = None
                                # Email_Send_ErrorAlarm(subject, Title)
                            if not GetAllRecords:
                                for i, _ in enumerate(GetAllRecords):
                                    LineID = GetAllRecords[i][0]
                            if not LineID:
                                xSystemMessageAutoGenerated = "New position"
                            if Pair:
                                xSystemMessageAutoGenerated = "PairTrade has been changed"
                            else:
                                xSystemMessageAutoGenerated = "Changed weight in a existing position"
                            if LineID is None:
                                LineID = "NULL"

                        QuantityToAvoidShortSelling = MultiplierQuantity

                        WeightTarget_para = WeightTarget / 100
                        if WeightTarget / 100 > WeightActual and InstrumentType_1stCol == "SW" and WeightTarget / 100 <= 0:
                            BuySellCoverShort = "BCOV"
                            SuggestionPreciseInstruction = "limit: 1% above latest availalbe price"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 20% of Vol, Limit 1% ABOVE latest available price"
                                Limit = bbg_Px_Last * 1.01
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = " 30% of Vol, Limit 5% ABOVE latest available price"
                                Limit = bbg_Px_Last * 1.05
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 < WeightActual and InstrumentType_1stCol == "SW" and WeightTarget / 100 <= 0:
                            BuySellCoverShort = "SSELL"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 20% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.99
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = " 30% of Vol, Limit 5% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.95
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 > WeightActual and InstrumentType_1stCol == "C":
                            BuySellCoverShort = "SELL"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 20% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.99
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = " 30% of Vol, Limit 5% BELOW  latest available price"
                                Limit = bbg_Px_Last * 0.95
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 < WeightActual and InstrumentType_1stCol == "C":
                            BuySellCoverShort = "SELL"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = "20% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.99
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = "30% of Vol, Limit 5% BELOW  latest available price"
                                Limit = bbg_Px_Last * 0.95
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 < 0 and WeightActual == 0 and InstrumentType_1stCol == "SW":

                            BuySellCoverShort = "SSELL"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 20% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.99
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = " 30% of Vol, Limit 5% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.95
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 > 0 and WeightActual == 0:
                            BuySellCoverShort = "BUY"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 20% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 1.01
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = " 30% of Vol, Limit 5% ABOVE latest available price"
                                Limit = bbg_Px_Last * 1.05
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 > WeightActual and InstrumentType_1stCol == "SW" and WeightTarget / 100 > 0:
                            BuySellCoverShort = "BUY"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = "25% of Vol, Limit 1% ABOVE latest available price"
                                Limit = bbg_Px_Last * 1.05
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = "30% of Vol, Limit 5% ABOVE  latest available price"
                                Limit = bbg_Px_Last * 1.05
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual

                        elif WeightTarget / 100 < WeightActual and InstrumentType_1stCol == "SW" and WeightTarget / 100 >= 0:
                            BuySellCoverShort = "SELL"
                            if gUrgency == "Urgent":
                                SuggestionPreciseInstruction = " 25% of Vol, Limit 1% BELOW latest available price"
                                Limit = bbg_Px_Last * 0.99
                            elif gUrgency == "High":
                                SuggestionPreciseInstruction = "30% of Vol, Limit 5% ABOVE  latest available price"
                                Limit = bbg_Px_Last * 1.05
                            elif gUrgency == "Normal":
                                SuggestionPreciseInstruction = "start with 50% of Vol, NO limit, get it done today!"
                            else:
                                WeightActual = WeightActual
                        else:
                            WeightActual = WeightActual
                            SuggestionPreciseInstruction = ""
                            BuySellCoverShort = side

                        # checking for record present or not
                        cursor.execute(
                            "SELECT  id FROM [outsys_prod].DBO." + database_name + "  WHERE ([TICKER_ISIN] = '" + str(
                                TickerISIN) + "' And [DATE]= '" + str(qepCurrdate) + "' ) ORDER BY [NAME] ASC ")

                        ChkRecords = cursor.fetchall()

                        #######################  If is repo true and fund is LevEuro ############################
                        # ProductType_2ndCol = securityType
                        if IsRepo or FundNumber == 14:
                            StretegyID = str(StretegyID) if StretegyID else 'NULL'
                            if ChkRecords:
                                LineID = ChkRecords[0][0]
                                Update_query = (
                                        "UPDATE [outsys_prod].DBO." + database_name + " SET  [DATE] = '" + str(
                                    qepCurrdate) + "', [INSTRTYPE] = '" + str(
                                    InstrumentType_1stCol) + "', [TICKER_ISIN] = '" + str(
                                    TickerISIN) + "',	[NAME] = '" + str(
                                    sStockName) + "',	[WEIGHT_ACTUAL] = '" + str(
                                    WeightActual) + "',	[WEIGHT_TARGET]='" + str(
                                    WeightTarget_para) + "',	[PAIRTRADE]= '" + str(
                                    xPairTrade) + "', [SECURITYTYPE]='" + str(
                                    ProductType_2ndCol) + "', [Quantity]=" + str(
                                    TradeQuantityPreciseIndicated) + ",[DB_LASTPRICE]='" + str(
                                    lastPrice) + "',[Currency]='" + str(bbgCrncy) + "',[IsRepo]='" + str(
                                    IsRepo) + "',[StrategyID] = " + StretegyID + ",[RepoExpiryDate] = '" + str(
                                    RepoExpiryDate) + "',[ORDERSTAGE] = '"+str(
                                    OrderStage)+"'	 WHERE [ID] = '" + str(
                                    LineID) + "'")
                                cursor.execute(Update_query)
                                conn.commit()
                                logger_sqlalchemy = logg_Handler(fund,
                                                                 logger_name="Update_query for different funds")
                                logger_sqlalchemy.info("Database" + database_name + " entry updated")

                            else:
                                Insert_query = (
                                        "INSERT INTO [outsys_prod].DBO." + database_name + " ([DATE],[INSTRTYPE],[TICKER_ISIN],[NAME],[WEIGHT_ACTUAL],[WEIGHT_TARGET],[PAIRTRADE],[SECURITYTYPE],[Quantity],[ORDERSTAGE],[DB_LASTPRICE],[Currency],[IsRepo],[StrategyID],[RepoExpiryDate]) VALUES "
                                                                                              "('" + str(
                                    qepCurrdate) + "','" + str(InstrumentType_1stCol) + "',"
                                                                                        "'" + str(
                                    TickerISIN) + "','" + str(sStockName) + "','" + str(WeightActual) + "','" + str(
                                    WeightTarget_para) + "','" + str(xPairTrade) + "','" + str(
                                    ProductType_2ndCol) + "','" + str(TradeQuantityPreciseIndicated) + "','" + str(
                                    OrderStage) + "','" + str(lastPrice) + "','" + str(bbgCrncy) + "','" + str(
                                    IsRepo) + "'," + (StretegyID) + ",'" + str(RepoExpiryDate) + "') ")
                                print(Insert_query)
                                cursor.execute(Insert_query)
                                conn.commit()
                                new_id = cursor.lastrowid
                                logger_sqlalchemy = logg_Handler(fund, logger_name="Insert query for different funds")
                                logger_sqlalchemy.info(
                                    "Database" + str(database_name) + " new entry added with " + str(
                                        new_id) + " as id.")
                        else:
                            if ChkRecords:
                                LineID = ChkRecords[0][0]
                                Update_query = (
                                        "UPDATE [outsys_prod].DBO." + database_name + " SET  [DATE] = '" + str(
                                    qepCurrdate) + "', [INSTRTYPE] = '" + str(
                                    InstrumentType_1stCol) + "', [TICKER_ISIN] = '" + str(
                                    TickerISIN) + "',	[NAME] = '" + str(
                                    sStockName) + "',	[WEIGHT_ACTUAL] = '" + str(
                                    WeightActual) + "',	[WEIGHT_TARGET]='" + str(
                                    WeightTarget_para) + "',	[PAIRTRADE]= '" + str(
                                    xPairTrade) + "', [SECURITYTYPE]='" + str(
                                    ProductType_2ndCol) + "', [Quantity]=" + str(
                                    TradeQuantityPreciseIndicated) + ",[DB_LASTPRICE]='" + str(
                                    lastPrice) + "',[Currency]='" + str(bbgCrncy) + "',[ORDERSTAGE] = '"+str(
                                    OrderStage)+"' WHERE [ID] = '" + str(
                                    LineID) + "'")
                                cursor.execute(Update_query)
                                conn.commit()
                                logger_sqlalchemy = logg_Handler(fund,
                                                                 logger_name="Update_query for different funds")
                                logger_sqlalchemy.info("Database" + database_name + " entry updated")

                            else:
                                Insert_query = (
                                        "INSERT INTO [outsys_prod].DBO." + database_name + " ([DATE],[INSTRTYPE],[TICKER_ISIN],[NAME],[WEIGHT_ACTUAL],[WEIGHT_TARGET],[PAIRTRADE],[SECURITYTYPE],[Quantity],[ORDERSTAGE],[DB_LASTPRICE],[Currency]) VALUES "
                                                                                              "('" + str(
                                    qepCurrdate) + "','" + str(InstrumentType_1stCol) + "',"
                                                                                        "'" + str(
                                    TickerISIN) + "','" + str(sStockName) + "','" + str(WeightActual) + "','" + str(
                                    WeightTarget_para) + "','" + str(xPairTrade) + "','" + str(
                                    ProductType_2ndCol) + "','" + str(TradeQuantityPreciseIndicated) + "','" + str(
                                    OrderStage) + "','" + str(lastPrice) + "','" + str(bbgCrncy) + "') ")
                                print(Insert_query)
                                cursor.execute(Insert_query)
                                conn.commit()
                                new_id = cursor.lastrowid
                                logger_sqlalchemy = logg_Handler(fund, logger_name="Insert query for different funds")
                                logger_sqlalchemy.info(
                                    "Database" + str(database_name) + " new entry added with " + str(
                                        new_id) + " as id.")
                            # new_id = 428800

                        if LineID is None or LineID == "NULL" or LineID == '':
                            LineID = new_id

                        query = (
                                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[CASA4FUND_FUNDNAME] o76, [ENORDERS].[CURRENCY] o77, [ENORDERS].[CASA4FUNDSECURITYTYPE] o78, [ENORDERS].[EXECUTEDQUANTITY] o79, [ENORDERS].[PENDINGQUANTITY] o80, [ENORDERS].[ORDERFROMDAYBEFORE] o81, [ENORDERS].[REBALANCE] o82, [ENORDERS].[ISFROMYESTERDAY] o83, [ENORDERS].[LAST_PRICE] o84, [ENORDERS].[C4F_BROKERCODE] o85, [ENORDERS].[FUNDNAV] o86, [ENORDERS].[FUNDCURRENCY] o87, [ENORDERS].[STOCKCURRENCY] o88, [ENORDERS].[SETTLEMENTCURRENCY] o89, [ENORDERS].[URGENCY] o90, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o91, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o92, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o93, [ENORDERS].[TRADEQUANTITYCALCULATED] o94, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o95, [ENORDERS].[BBGSECURITYNAME] o96, [ENORDERS].[BBGEXCHANGE] o97, [ENORDERS].[ORDERCLOSE] o98, [ENORDERS].[PRECISEINSTRUCTIONS] o99, [ENORDERS].[ISIN] o100, [ENORDERS].[COUNTRY] o101, [ENORDERS].[ORDERSTAGEOWL] o102, [ENORDERS].[APICORRELATIONID] o103, [ENORDERS].[APIORDERREFID] o104, [ENORDERS].[SETTLEMENTDATE] o105, [ENORDERS].[BBGMESS1] o106, [ENORDERS].[BBGSETTLEDATE] o107, [ENORDERS].[BBGEMSXSTATUS] o108, [ENORDERS].[BBGEMSXSEQUENCE] o109, [ENORDERS].[BBGEMSXROUTEID] o110, [ENORDERS].[BBGCOUNTRYISO] o111, [ENORDERS].[BROKERBPS] o112, [ENORDERS].[BROKERCENTPERSHARE] o113, [ENORDERS].[TRADINGCOMMISSIONSBPS] o114, [ENORDERS].[TRADINGCOMMISSIONSCENT] o115, [ENORDERS].[BBG_OPTCONTSIZE] o116, [ENORDERS].[BBG_FUTCONTSIZE] o117, [ENORDERS].[BBG_IDPARENTCO] o118, [ENORDERS].[BBG_LOTSIZE] o119, [ENORDERS].[BBG_MARKETOPENINGTIME] o120, [ENORDERS].[BBG_MARKETCLOSINGTIME] o121, [ENORDERS].[BBG_PRICEINVALID] o122, [ENORDERS].[BBG_OPT_UNDL_PX] o123, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o124, [ENORDERS].[POTENTIALERROR] o125, [ENORDERS].[RMSTATUS] o126, [ENORDERS].[ACTUALQUANTITY] o127, [ENORDERS].[RM1] o128, [ENORDERS].[RM2] o129, [ENORDERS].[RM3] o130, [ENORDERS].[RM4] o131, [ENORDERS].[RM5] o132, [ENORDERS].[RM6] o133, [ENORDERS].[RM7] o134, [ENORDERS].[RM8] o135, [ENORDERS].[RM9] o136, [ENORDERS].[RM10] o137, [ENORDERS].[BBG_PARENTCOID] o138, [ENORDERS].[BBG_PARENTCONAME] o139, [ENORDERS].[TRADERNOTES] o140, [ENORDERS].[PORTFOLIOUPDATED] o141, [ENORDERS].[UPDALREADYADDED] o142, [ENORDERS].[UPDALREADYSUBTRACTED] o143, [ENORDERS].[BROKERSELMETHOD] o144, [ENORDERS].[BROKERSELREASON] o145, [ENORDERS].[EXECUTORFACTOR_COST] o146, [ENORDERS].[EXECUTORFACTOR_SPEED] o147, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o148, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o149, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o150, [ENORDERS].[EXECUTORFACTOR_NATURE] o151, [ENORDERS].[EXECUTORFACTOR_VENUE] o152, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o153, [ENORDERS].[NEEDCOMMENT] o154, [ENORDERS].[FX_BASKETRUNID] o155, [ENORDERS].[FIX_CONF] o156, [ENORDERS].[FIX_CIORDID] o157, [ENORDERS].[FIX_EXECUTIONID] o158, [ENORDERS].[FIX_AVGPX] o159, [ENORDERS].[FIX_FAR_AVGPX] o160, [ENORDERS].[FIX_LASTQTY] o161, [ENORDERS].[FIX_FAR_LASTQTY] o162, [ENORDERS].[FIX_LEAVESQTY] o163, [ENORDERS].[OUTRIGHTORSWAP] o164, [ENORDERS].[CURRENCY_2] o165, [ENORDERS].[JPMORGANACCOUNT] o166, [ENORDERS].[BROKERCODEAUTO] o167, [ENORDERS].[EXPOSURETRADEID] o168, [ENORDERS].[BROKERHASBEENCHANGED] o169, [ENORDERS].[UBSACCOUNT] o170, [ENORDERS].[RISKMANAGEMENTRESULT] o171, [ENORDERS].[ARRIVALPRICE] o172, [ENORDERS].[ADV20D] o173, [ENORDERS].[MEAMULTIPLIER] o174, [ENORDERS].[LEVEUROSTATEGYID] o175, [ENORDERS].[ISREPO] o176, [ENORDERS].[REPOEPIRYDATE] o177, [ENORDERS].[REPO_CODEDOSSIER] o178, [ENORDERS].[REPO_VALEURTAUX] o179, [ENORDERS].[REPO_BICSENDER] o180, [ENORDERS].[REPO_CODECONTREPARTIE] o181, [ENORDERS].[REPO_COMPARTIMENT] o182, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o183, [ENORDERS].[REPO_NOMTAUX] o184, [ENORDERS].[REPO_REFERENCEEXTERNE] o185, [ENORDERS].[REPO_BASECALCULINTERET] o186, [ENORDERS].[REPO_TERMDATE] o187, [ENORDERS].[REPO_HAIRCUT] o188, [ENORDERS].[REPO_INTEREST_RATE] o189, [ENORDERS].[LEVEUROSETTLEDATE] o190, [ENORDERS].[REPO_2RDLEG_PRICE] o191, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o192, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o193, [ENORDERS].[LEIREPORTINGCODE] o194, [ENORDERS].[BROKERCODE] o195, [ENORDERS].[MASTERAGREEMENT] o196, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o197, [ENORDERS].[REPO_SFTR] o198, [ENORDERS].[REPO_REAL] o199, [ENORDERS].[APPROVALDATETIMEMILLI] o200, [ENORDERS].[REPO_UTI] o201 "
                                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[DATE] = '" + str(
                            qepCurrdate) + "') AND ([ENORDERS].[FUNDCODE] = convert(varchar(11), '" + str(
                            FundNumber) + "')) AND ([ENORDERS].[TICKERISIN] = '" + str(
                            TickerISIN) + "') ORDER BY [ENORDERS].[SIDE] ASC ")
                        cursor.execute(query)
                        FundsIDs = 0
                        getRecords = cursor.fetchall()
                        for i, _ in enumerate(getRecords):
                            FundsIDs = getRecords[i][70]

                        CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded, \
                        BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX \
                            = Calculations(WeightTarget=WeightTarget, WeightActual=WeightActual,
                                           FundCurrency=FundCurrency,
                                           StockCurrency=bbgCrncy, ShowQuantityBox=ShowQuantityBox,
                                           PreciseQuantityIndicated=Decimal(TradeQuantityPreciseIndicated),
                                           Bbg_Px_Last=Decimal(bbg_Px_Last),
                                           ProductType_2ndCol=ProductType_2ndCol,
                                           NAV_or_PCvalue=Decimal(NAV),
                                           QuantityToAvoidShortSelling=QuantityToAvoidShortSelling,
                                           bbgCountryISO=bbg_Country_Iso, BrokeriD=BrokerID,
                                           Exchange=bbg_Exchange,
                                           bbgFutContSize=bbg_FutContSize,
                                           bbgOptContSize=bbg_OptContSize,
                                           ExposureCalculationMethod=ExposureCalculationMethod,
                                           UnderlyingPrice=Decimal(bbg_UnderlyingPrice),
                                           MEA_Multiplier=Decimal(Multiplier))
                        TradeQuantityCalculated = Quantity
                        CounterValueInLocal = CounterValueLocalCurrency
                        TradeQuantityCalculatedRounded = QuantityRounded
                        CounterValueFundCurrency = CounterValueFundCurrency

                        TraderNote_s = TraderNotes(Ticker_ISIN=TickerISIN, FundID=FundsIDs)
                        calFx = FX
                        TraderNote = TraderNote_s[0] if TraderNote_s else ""

                        ctime = get_current_time()

                        aCurrentDate = get_current_date()
                        NewTargetWeight = WeightTarget / 100
                        OrderType = "Limit" if Limit != 0 else "Market"
                        Username = userName
                        OrderQtyValue = TradeQuantityCalculatedRounded if TradeQuantityPreciseIndicated else TradeQuantityPreciseIndicated
                        out_BrokerID = str('NULL') if out_BrokerID is None else out_BrokerID

                        # ProductType_2ndCol = securityType
                        # ------------------------------------------ Create order query add here --------------------------
                        o_IsRepo = 1 if IsRepo else 0
                        print("LineID fund id",LineID)

                        CreateOrders = (
                                "INSERT INTO [outsys_prod].DBO.[OSUSR_38P_ORDERS]([CreationTime],[TickerISIN],[FundCode],[ActualWeight],"
                                "[NewTargetWeight],[Side],[Approved],[ProductType],[FundName],[Broker],[TradingDeskConfirmation],[bnrProductType],"
                                "[ProductID],[Limit],[OrderType],[bnrBroker],[Expiry],[Routing],[Fund],[Custodian],[OrderQtyType],[UserComment],[Account],"
                                "[Date],[FundNameShort],[StockName],[IntrumentType],[BrokerID_contactTAB],[SettleDate],"
                                "[TransactionType],""[" + col_name + "]"",[OrderStage],[BnrOrderPreciseQuantity],"
                                                                     "[Last_Price],[Currency],[FundCurrency],[Urgency],"
                                                                     "[Fx_FundCrncyVSfundCrncy],[PreciseInstructions],"
                                                                     "[ORDERQTYVALUE],[OPERATOR],[INSTRUCTIONS],"
                                                                     "[isrepo],[BROKERSELMETHOD],[LevEuroSettleDate],"
                                                                     "[LeiReportingCode],[REPO_CodeContrepartie],"
                                                                     "[BrokerCode],[MasterAgreement],[MasterAgreementVersion_date]) VALUES ('" + str(
                            ctime) + "','" + str(TickerISIN) + "','" + str(FundCode) + "','" + str(
                            WeightActual) + "','" + str(NewTargetWeight) + "','" + str(
                            BuySellCoverShort) + "','Pending','" + str(ArborProductType) + "','" + str(
                            FundName) + "','" + str(out_BrokerShortCode) + "'," + str(0) + ",'" + str(
                            ProductType_2ndCol) + "','" + str(TickerISIN) + "','" + str(
                            Limit) + "','" + str(OrderType) + "','" + str(out_BrokerName) + "','" + str(
                            expiry) + "','Automated','" + str(Fund) + "','" + str(Custodian) + "','" + str(
                            OrderQtyType) + "','" + str(Comment).replace("'","''") + "','" + str(Account) + "','" + str(
                            aCurrentDate) + "','" + str(FundNameShort) + "','" + str(Name) + "','" + str(
                            InstrumentType_1stCol) + "'," + str(out_BrokerID) + ",'" + str(
                            settleDate) + "','" + str(InstrumentType_1stCol) + "','" + str(
                            LineID) + "','Pending','" + str(TradeQuantityPreciseIndicated) + "','" + str(
                            bbg_Px_Last) + "','" + str(bbgCrncy) + "','" + str(FundCurrency) + "','" + str(
                            gUrgency) + "','" + str(calFx) + "','" + str(SuggestionPreciseInstruction) + "','" + str(
                            user_given_quantity) + "','" + str(Username) + "','" + str(instructions).replace("'","''") + "',"+str(
                            o_IsRepo)+",'"+str(BrokerType_A_B)+"','"+str(settleDate)+"','"+str(
                            out_LeiReportingCode)+"','"+str(out_BrokerCodeLevEuro)+"','"+str(
                            out_BrokerCodeLevEuro)+"','"+str(out_masterAgreement)+"','"+str(out_masterAgreementVersionDate)+"')")
                        print(CreateOrders)
                        cursor.execute(CreateOrders)
                        conn.commit()
                        OrderID = cursor.lastrowid
                        logger_sqlalchemy = logg_Handler(fund, logger_name="Insert data into orders table")
                        logger_sqlalchemy.info(
                            "In order table new entry added with " + str(OrderID) + " as id.")
                        # OrderID = 11110
                        print("Order id",OrderID)
                        UserID = userId
                        OSUSR_BOL_CONTROLTICKER_ISIN_ID = Order_Changes(OrderID, UserID, TimeCreation=ctime,
                                                                        TickerISIN=TickerISIN,
                                                                        LimitPriceChanges=Limit,
                                                                        ExecutedQuantity=0, ChangeBroker=False,
                                                                        NewBrokerShortCode=BrokerShortCode)
                        processid = OSUSR_BOL_CONTROLTICKER_ISIN_ID
                        print("processid-->", processid)
                        conn.close()

                        # Email_Send_Advisory(IsLogContent=False, FundName=sFundName,
                        #                     BuySell=BuySellCoverShort, Ticker=TickerISIN,
                        #                     StockName=sStockName, To=To,
                        #                     PreviousWeight=WeightActual, NewWeight=WeightTarget,
                        #                     IntrumentType=InstrumentType_1stCol,
                        #                     Message=xSystemMessageAutoGenerated, Urgency=gUrgency,
                        #                     SuggestionGenerated=gDateTime, PairTrade=xPairTrade,
                        #                     QuantityIndicated=TradeQuantityPreciseIndicated,
                        #                     )
                        return str(processid)
                else:
                    return Http_Server_wst01_repose

