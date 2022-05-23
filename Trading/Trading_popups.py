from Funds.configuration import database_dev, get_current_time, get_current_date,get_current_date_time
from datetime import datetime, timedelta
from Funds.email_send_service import Emails_Send,Email_Send_Advisory_Medata
import calendar
from flask import jsonify


def col_mapping(fund_number):
    """
    This fuction called to map col_name (i.e NorthAmericaLS_ID,GREATERCHINALS_ID etc.) dynamically with fund number

    :param fund_number:

    :return: col_name
    """
    FundNumber  = fund_number
    if FundNumber == 2:
        col_name = 'NorthAmericaLS_ID'
    elif FundNumber == 3:
        col_name = 'GREATERCHINALS_ID'
    elif FundNumber == 1:
        col_name = 'ItalyLS_ID'
    elif FundNumber == 5:
        col_name = 'EUROPENVALUE_ID'
    elif FundNumber == 8:
        col_name = 'CHIRON_ID'
    elif FundNumber == 4:
        col_name = 'EUROBOND_ID'
    elif FundNumber == 10:
        col_name = 'NEWFRONTIERSID'
    elif FundNumber == 6:
        col_name = 'ROSEMARYID'
    elif FundNumber == 11:
        col_name = 'ASIANALPHAID'
    elif FundNumber == 12:
        col_name = 'HIGHFOCUSID'
    elif FundNumber == 13:
        col_name = 'MEAOPPORTUNITIESID'
    elif FundNumber == 14:
        col_name = 'LEVEUROID'
    elif FundNumber == 16:
        col_name = 'ASIMOCCOID'
    elif FundNumber == 18:
        col_name = 'RAFFAELLO'
    return col_name


def day_of_week(date_obj):
    """
    Calculate days of week based on date

    :param date_obj: Date

    :return: return day of week

    """
    CurDate = date_obj.strftime('%Y-%m-%d')
    date = ' '.join(CurDate.split("-"))
    year, month, day = (int(i) for i in date.split(' '))
    dayNumber = calendar.weekday(year, month, day)
    # days = ["Monday", "Tuesday", "Wednesday", "Thursday","Friday", "Saturday", "Sunday"]
    days = ["1", "2", "3", "4", "5", "6", "0"]
    return days[dayNumber]


def AddDays(order_date, day):
    """
    This method used to ddd day (i.e 1/3/2022 while add day 1 then date will be 2/3/2022) into order date

    :param order_date:
    :param day:

    :return: return updated date

    """
    CurDate = order_date.strftime('%Y-%m-%d')
    current_date_temp = datetime.strptime(CurDate, '%Y-%m-%d')
    newdate = current_date_temp + timedelta(days=int(day))
    return newdate


def Hex_To_Decimal(string):
    """
    This method used to convert hexadecimal to decimal number

    :param string: hexadecimal Number

    :return:  convert Hex to decimal

    """
    IntakeValue = string
    # IntakeValue = Index(string,"E",startIndex:0,searchFromEnd:False,ignoreCase:True)
    if IntakeValue < 0:
        Decimal = float(IntakeValue)
    else:
        Decimal = 0
    return Decimal


def get_current_datetime():
    """
    This function used to get current datetime
    :return: current datetime object

    """
    now = datetime.now()
    curr_date_time = now.strftime('%Y-%m-%d-%H:%M:%S')
    return curr_date_time


def Order_Stage(NorthAmericaLS_ID, GreaterChinaLS_ID, ItalyLS_ID, EuropeanValue_ID, OrderStage, EuroBond_ID, Chiron_ID,
                NewFrontiersID, Rosemary_ID, AsianAlpha_ID, HighFocus_ID, MEAorder_ID, LevEuroID, RaffaelloID,Username):
    """
    This method update order_stage for different funds based on it's ID.
    """
    print("Order_Stage process called...!!!")
    conn = database_dev()
    cursor = conn.cursor()
    Username = Username

    if NorthAmericaLS_ID is not None and GreaterChinaLS_ID is not None or NorthAmericaLS_ID is not None and \
            ItalyLS_ID is not None or NorthAmericaLS_ID is not None and EuropeanValue_ID is not None or \
            GreaterChinaLS_ID is not None and ItalyLS_ID is not None or GreaterChinaLS_ID is not None and \
            EuropeanValue_ID is not None or ItalyLS_ID is not None and EuropeanValue_ID is not None or \
            NewFrontiersID is not None and EuropeanValue_ID is not None or NewFrontiersID is not None and \
            ItalyLS_ID is not None or AsianAlpha_ID is not None and Rosemary_ID is not None:
        mail = "Screen: List suggestions, Action: Rejecting, -BanoCS logic: OrderStage. Error:" \
               "The last suggestion received by the trading desk has no fund id! User: " + str(Username)
        Emails_Send(mail)
        return None
    else:
        if ItalyLS_ID is not None:
            select_query = (
                    "SELECT [ENITALYLS].[ID] o0, [ENITALYLS].[DATE] o1, [ENITALYLS].[INSTRTYPE] o2, [ENITALYLS].[SECURITYTYPE] o3, [ENITALYLS].[QUANTITY] o4, [ENITALYLS].[TICKER_ISIN] o5, [ENITALYLS].[NAME] o6, [ENITALYLS].[COST] o7, [ENITALYLS].[WEIGHT_ACTUAL] o8, [ENITALYLS].[WEIGHT_TARGET] o9, [ENITALYLS].[PAIRTRADE] o10, [ENITALYLS].[ORDERSTAGE] o11, [ENITALYLS].[ORDERUPDATE] o12, [ENITALYLS].[CURRENCY] o13, [ENITALYLS].[DB_LASTPRICE] o14, [ENITALYLS].[FUNDCURRENCY] o15, [ENITALYLS].[FX] o16, [ENITALYLS].[FXDATETIME] o17, [ENITALYLS].[FXTICKER] o18, [ENITALYLS].[COUNTERVALUELOCAL] o19, [ENITALYLS].[COUNTERVALUEFUNDCRNCY] o20, [ENITALYLS].[HIST_PX_LASTMONTH] o21, [ENITALYLS].[HIST_DATE] o22, [ENITALYLS].[BROKERCODE] o23, [ENITALYLS].[BBG_PARENTCOID] o24 "
                    "FROM [outsys_prod].DBO.[OSUSR_38P_ITALYLS] [ENITALYLS] WHERE ([ENITALYLS].[ID] = '" + str(
                ItalyLS_ID) + "') ORDER BY [ENITALYLS].[NAME] ASC ")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ITALYLS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(ItalyLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()

        elif EuropeanValue_ID is not None:
            select_query = ("SELECT [ENEUROPEANVALUE].[ID] o0, [ENEUROPEANVALUE].[DATE] o1, [ENEUROPEANVALUE].[INSTRTYPE] o2, [ENEUROPEANVALUE].[SECURITYTYPE] o3, [ENEUROPEANVALUE].[QUANTITY] o4, [ENEUROPEANVALUE].[TICKER_ISIN] o5, [ENEUROPEANVALUE].[NAME] o6, [ENEUROPEANVALUE].[COST] o7, [ENEUROPEANVALUE].[WEIGHT_ACTUAL] o8, [ENEUROPEANVALUE].[WEIGHT_TARGET] o9, [ENEUROPEANVALUE].[PAIRTRADE] o10, [ENEUROPEANVALUE].[ORDERSTAGE] o11, [ENEUROPEANVALUE].[ORDERUPDATE] o12, [ENEUROPEANVALUE].[CURRENCY] o13, [ENEUROPEANVALUE].[DB_LASTPRICE] o14, [ENEUROPEANVALUE].[FX] o15, [ENEUROPEANVALUE].[FXDATETIME] o16, [ENEUROPEANVALUE].[FXTICKER] o17, [ENEUROPEANVALUE].[FUNDCURRENCY] o18, [ENEUROPEANVALUE].[COUNTERVALUELOCAL] o19, [ENEUROPEANVALUE].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROPEANVALUE].[HIST_PX_LASTMONTH] o21, [ENEUROPEANVALUE].[HIST_DATE] o22, [ENEUROPEANVALUE].[BBG_PARENTCOID] o23, [ENEUROPEANVALUE].[BROKERCODE] o24"
                            "FROM [outsys_prod].DBO.[OSUSR_38P_EUROPEANVALUE] [ENEUROPEANVALUE] WHERE ([ENEUROPEANVALUE].[ID] = '"+str(EuropeanValue_ID)+"') ORDER BY [ENEUROPEANVALUE].[NAME] ASC")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_EUROPEANVALUE] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(ItalyLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif GreaterChinaLS_ID is not None:
            select_query = ("SELECT [ENGREATERCHINALS].[ID] o0, [ENGREATERCHINALS].[DATE] o1, [ENGREATERCHINALS].[INSTRTYPE] o2, [ENGREATERCHINALS].[SECURITYTYPE] o3, [ENGREATERCHINALS].[QUANTITY] o4, [ENGREATERCHINALS].[TICKER_ISIN] o5, [ENGREATERCHINALS].[NAME] o6, [ENGREATERCHINALS].[COST] o7, [ENGREATERCHINALS].[WEIGHT_ACTUAL] o8, [ENGREATERCHINALS].[WEIGHT_TARGET] o9, [ENGREATERCHINALS].[PAIRTRADE] o10, [ENGREATERCHINALS].[ORDERSTAGE] o11, [ENGREATERCHINALS].[ORDERUPDATE] o12, [ENGREATERCHINALS].[CURRENCY] o13, [ENGREATERCHINALS].[DB_LASTPRICE] o14, [ENGREATERCHINALS].[FX] o15, [ENGREATERCHINALS].[FXDATETIME] o16, [ENGREATERCHINALS].[FXTICKER] o17, [ENGREATERCHINALS].[FUNDCURRENCY] o18, [ENGREATERCHINALS].[COUNTERVALUELOCAL] o19, [ENGREATERCHINALS].[COUNTERVALUEFUNDCRNCY] o20, [ENGREATERCHINALS].[HIST_PX_LASTMONTH] o21, [ENGREATERCHINALS].[HIST_DATE] o22, [ENGREATERCHINALS].[BROKERCODE] o23, [ENGREATERCHINALS].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] [ENGREATERCHINALS] WHERE ([ENGREATERCHINALS].[ID] = '" + str(GreaterChinaLS_ID) + "') ORDER BY [ENGREATERCHINALS].[NAME] ASC ")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] set OrderStage = '" + str(order_stage) + "' where ID = '" + str(GreaterChinaLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif NorthAmericaLS_ID is not None:
            select_query = ("SELECT [ENNORTHAMERICALS].[ID] o0, [ENNORTHAMERICALS].[DATE] o1, [ENNORTHAMERICALS].[INSTRTYPE] o2, [ENNORTHAMERICALS].[SECURITYTYPE] o3, [ENNORTHAMERICALS].[QUANTITY] o4, [ENNORTHAMERICALS].[TICKER_ISIN] o5, [ENNORTHAMERICALS].[NAME] o6, [ENNORTHAMERICALS].[COST] o7, [ENNORTHAMERICALS].[WEIGHT_ACTUAL] o8, [ENNORTHAMERICALS].[WEIGHT_TARGET] o9, [ENNORTHAMERICALS].[PAIRTRADE] o10, [ENNORTHAMERICALS].[TRADINGINDEX] o11, [ENNORTHAMERICALS].[ORDERSTAGE] o12, [ENNORTHAMERICALS].[ORDERUPDATE] o13, [ENNORTHAMERICALS].[CURRENCY] o14, [ENNORTHAMERICALS].[DB_LASTPRICE] o15, [ENNORTHAMERICALS].[FX] o16, [ENNORTHAMERICALS].[FXDATETIME] o17, [ENNORTHAMERICALS].[FXTICKER] o18, [ENNORTHAMERICALS].[FUNDCURRENCY] o19, [ENNORTHAMERICALS].[COUNTERVALUELOCAL] o20, [ENNORTHAMERICALS].[COUNTERVALUEFUNDCRNCY] o21, [ENNORTHAMERICALS].[HIST_PX_LASTMONTH] o22, [ENNORTHAMERICALS].[HIST_DATE] o23, [ENNORTHAMERICALS].[BROKERCODE] o24, [ENNORTHAMERICALS].[BBG_PARENTCOID] o25 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] [ENNORTHAMERICALS] WHERE ([ENNORTHAMERICALS].[ID] = '" + str(NorthAmericaLS_ID) + "') ORDER BY [ENNORTHAMERICALS].[NAME] ASC")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(NorthAmericaLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif EuroBond_ID is not None:
            select_query = ("SELECT [ENEUROBOND].[ID] o0, [ENEUROBOND].[DATE] o1, [ENEUROBOND].[INSTRTYPE] o2, [ENEUROBOND].[SECURITYTYPE] o3, [ENEUROBOND].[QUANTITY] o4, [ENEUROBOND].[TICKER_ISIN] o5, [ENEUROBOND].[NAME] o6, [ENEUROBOND].[COST] o7, [ENEUROBOND].[WEIGHT_ACTUAL] o8, [ENEUROBOND].[WEIGHT_TARGET] o9, [ENEUROBOND].[PAIRTRADE] o10, [ENEUROBOND].[ORDERSTAGE] o11, [ENEUROBOND].[ORDERUPDATE] o12, [ENEUROBOND].[CURRENCY] o13, [ENEUROBOND].[DB_LASTPRICE] o14, [ENEUROBOND].[FX] o15, [ENEUROBOND].[FXDATETIME] o16, [ENEUROBOND].[FXTICKER] o17, [ENEUROBOND].[FUNDCURRENCY] o18, [ENEUROBOND].[COUNTERVALUELOCAL] o19, [ENEUROBOND].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROBOND].[ENDMONTH_PX] o21, [ENEUROBOND].[HIST_PX_LASTMONTH] o22, [ENEUROBOND].[HIST_DATE] o23, [ENEUROBOND].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_EUROBOND] [ENEUROBOND] WHERE ([ENEUROBOND].[ID] = '" + str(EuroBond_ID) + "') ORDER BY [ENEUROBOND].[DATE] ASC ")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_EUROBOND] set OrderStage = '" + str(order_stage) + "' where ID = '" + str(EuroBond_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif Chiron_ID is not None:
            select = ("SELECT [ENCHIRON].[ID] o0, [ENCHIRON].[DATE] o1, [ENCHIRON].[INSTRTYPE] o2, [ENCHIRON].[SECURITYTYPE] o3, [ENCHIRON].[QUANTITY] o4, [ENCHIRON].[TICKER_ISIN] o5, [ENCHIRON].[NAME] o6, [ENCHIRON].[COST] o7, [ENCHIRON].[WEIGHT_ACTUAL] o8, [ENCHIRON].[WEIGHT_TARGET] o9, [ENCHIRON].[PAIRTRADE] o10, [ENCHIRON].[ORDERSTAGE] o11, [ENCHIRON].[ORDERUPDATE] o12, [ENCHIRON].[CURRENCY] o13, [ENCHIRON].[DB_LASTPRICE] o14, [ENCHIRON].[FX] o15, [ENCHIRON].[FXDATETIME] o16, [ENCHIRON].[FXTICKER] o17, [ENCHIRON].[FUNDCURRENCY] o18, [ENCHIRON].[COUNTERVALUELOCAL] o19, [ENCHIRON].[COUNTERVALUEFUNDCRNCY] o20, [ENCHIRON].[HIST_PX_LASTMONTH] o21, [ENCHIRON].[HIST_DATE] o22, [ENCHIRON].[BBG_PARENTCOID] o23 "
                      "FROM [outsys_prod].DBO.[OSUSR_BOL_CHIRON] [ENCHIRON] WHERE ([ENCHIRON].[ID] = '"+str(Chiron_ID)+"') ORDER BY [ENCHIRON].[NAME] ASC ")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_CHIRON] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(Chiron_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif NewFrontiersID is not None:
            select = ("SELECT [ENNEWFRONTIERS].[ID] o0, [ENNEWFRONTIERS].[DATE] o1, [ENNEWFRONTIERS].[INSTRTYPE] o2, [ENNEWFRONTIERS].[SECURITYTYPE] o3, [ENNEWFRONTIERS].[QUANTITY] o4, [ENNEWFRONTIERS].[TICKER_ISIN] o5, [ENNEWFRONTIERS].[NAME] o6, [ENNEWFRONTIERS].[COST] o7, [ENNEWFRONTIERS].[WEIGHT_ACTUAL] o8, [ENNEWFRONTIERS].[WEIGHT_TARGET] o9, [ENNEWFRONTIERS].[PAIRTRADE] o10, [ENNEWFRONTIERS].[ORDERSTAGE] o11, [ENNEWFRONTIERS].[ORDERUPDATE] o12, [ENNEWFRONTIERS].[CURRENCY] o13, [ENNEWFRONTIERS].[DB_LASTPRICE] o14, [ENNEWFRONTIERS].[FX] o15, [ENNEWFRONTIERS].[FXDATETIME] o16, [ENNEWFRONTIERS].[FXTICKER] o17, [ENNEWFRONTIERS].[FUNDCURRENCY] o18, [ENNEWFRONTIERS].[COUNTERVALUELOCAL] o19, [ENNEWFRONTIERS].[COUNTERVALUEFUNDCRNCY] o20, [ENNEWFRONTIERS].[HIST_PX_LASTMONTH] o21, [ENNEWFRONTIERS].[HIST_DATE] o22, [ENNEWFRONTIERS].[BROKERCODE] o23, [ENNEWFRONTIERS].[BBG_PARENTCOID] o24 "
                      "FROM [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS] [ENNEWFRONTIERS] WHERE ([ENNEWFRONTIERS].[ID] = '"+str(NewFrontiersID)+"')")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS] set OrderStage = '" + str(order_stage) + "' where ID = '" + str(NewFrontiersID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif Rosemary_ID is not None:
            select = ("SELECT [ENROSEMARY].[ID] o0, [ENROSEMARY].[DATE] o1, [ENROSEMARY].[INSTRTYPE] o2, [ENROSEMARY].[SECURITYTYPE] o3, [ENROSEMARY].[QUANTITY] o4, [ENROSEMARY].[TICKER_ISIN] o5, [ENROSEMARY].[NAME] o6, [ENROSEMARY].[COST] o7, [ENROSEMARY].[WEIGHT_ACTUAL] o8, [ENROSEMARY].[WEIGHT_TARGET] o9, [ENROSEMARY].[PAIRTRADE] o10, [ENROSEMARY].[ORDERSTAGE] o11, [ENROSEMARY].[ORDERUPDATE] o12, [ENROSEMARY].[CURRENCY] o13, [ENROSEMARY].[DB_LASTPRICE] o14, [ENROSEMARY].[FX] o15, [ENROSEMARY].[FXDATETIME] o16, [ENROSEMARY].[FXTICKER] o17, [ENROSEMARY].[FUNDCURRENCY] o18, [ENROSEMARY].[COUNTERVALUELOCAL] o19, [ENROSEMARY].[COUNTERVALUEFUNDCRNCY] o20, [ENROSEMARY].[HIST_PX_LASTMONTH] o21, [ENROSEMARY].[HIST_DATE] o22, [ENROSEMARY].[BROKERCODE] o23, [ENROSEMARY].[BBG_PARENTCOID] o24 "
                      "FROM [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY] [ENROSEMARY] WHERE ([ENROSEMARY].[ID] = '"+str(Rosemary_ID)+"')")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(Rosemary_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif AsianAlpha_ID is not None:
            select = ("SELECT [ENASIANALPHA].[ID] o0, [ENASIANALPHA].[DATE] o1, [ENASIANALPHA].[INSTRTYPE] o2, [ENASIANALPHA].[SECURITYTYPE] o3, [ENASIANALPHA].[QUANTITY] o4, [ENASIANALPHA].[TICKER_ISIN] o5, [ENASIANALPHA].[ISIN] o6, [ENASIANALPHA].[NAME] o7, [ENASIANALPHA].[COST] o8, [ENASIANALPHA].[WEIGHT_ACTUAL] o9, [ENASIANALPHA].[WEIGHT_TARGET] o10, [ENASIANALPHA].[PAIRTRADE] o11, [ENASIANALPHA].[ORDERSTAGE] o12, [ENASIANALPHA].[ORDERUPDATE] o13, [ENASIANALPHA].[CURRENCY] o14, [ENASIANALPHA].[DB_LASTPRICE] o15, [ENASIANALPHA].[FUNDCURRENCY] o16, [ENASIANALPHA].[FX] o17, [ENASIANALPHA].[FXDATETIME] o18, [ENASIANALPHA].[FXTICKER] o19, [ENASIANALPHA].[COUNTERVALUELOCAL] o20, [ENASIANALPHA].[COUNTERVALUEFUNDCRNCY] o21, [ENASIANALPHA].[HIST_PX_LASTMONTH] o22, [ENASIANALPHA].[HIST_DATE] o23, [ENASIANALPHA].[COUNTRY_ISO] o24, [ENASIANALPHA].[COUNTRY_FULL_NAME] o25, [ENASIANALPHA].[BBG_PARENTCOID] o26, [ENASIANALPHA].[BROKERCODE] o27 "
                      "FROM [outsys_prod].DBO.[OSUSR_BOL_ASIANALPHA] [ENASIANALPHA] WHERE ([ENASIANALPHA].[ID] = '"+str(AsianAlpha_ID)+"') ORDER BY [ENASIANALPHA].[NAME] ASC ")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_ASIANALPHA] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(AsianAlpha_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif HighFocus_ID is not None:
            select = ("SELECT [ENHIGHFOCUS].[ID] o0, [ENHIGHFOCUS].[DATE] o1, [ENHIGHFOCUS].[INSTRTYPE] o2, [ENHIGHFOCUS].[SECURITYTYPE] o3, [ENHIGHFOCUS].[QUANTITY] o4, [ENHIGHFOCUS].[TICKER_ISIN] o5, [ENHIGHFOCUS].[NAME] o6, [ENHIGHFOCUS].[COST] o7, [ENHIGHFOCUS].[WEIGHT_ACTUAL] o8, [ENHIGHFOCUS].[WEIGHT_TARGET] o9, [ENHIGHFOCUS].[PAIRTRADE] o10, [ENHIGHFOCUS].[TRADINGINDEX] o11, [ENHIGHFOCUS].[ORDERSTAGE] o12, [ENHIGHFOCUS].[ORDERUPDATE] o13, [ENHIGHFOCUS].[CURRENCY] o14, [ENHIGHFOCUS].[DB_LASTPRICE] o15, [ENHIGHFOCUS].[FX] o16, [ENHIGHFOCUS].[FXDATETIME] o17, [ENHIGHFOCUS].[FXTICKER] o18, [ENHIGHFOCUS].[FUNDCURRENCY] o19, [ENHIGHFOCUS].[COUNTERVALUELOCAL] o20, [ENHIGHFOCUS].[COUNTERVALUEFUNDCRNCY] o21, [ENHIGHFOCUS].[HIST_PX_LASTMONTH] o22, [ENHIGHFOCUS].[HIST_DATE] o23, [ENHIGHFOCUS].[BROKERCODE] o24, [ENHIGHFOCUS].[BBG_PARENTCOID] o25, [ENHIGHFOCUS].[COUNTRY_ISO] o26, [ENHIGHFOCUS].[COUNTRY_FULL_NAME] o27 "
                      "FROM [outsys_prod].DBO.[OSUSR_SKP_HIGHFOCUS] [ENHIGHFOCUS] WHERE ([ENHIGHFOCUS].[ID] = '"+str(HighFocus_ID)+"') ORDER BY [ENHIGHFOCUS].[NAME] ASC")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_HIGHFOCUS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(HighFocus_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif MEAorder_ID is not None:
            select = ("SELECT [ENMEAOPPORTUNITIES].[ID] o0, [ENMEAOPPORTUNITIES].[DATE] o1, [ENMEAOPPORTUNITIES].[INSTRTYPE] o2, [ENMEAOPPORTUNITIES].[SECURITYTYPE] o3, [ENMEAOPPORTUNITIES].[QUANTITY] o4, [ENMEAOPPORTUNITIES].[TICKER_ISIN] o5, [ENMEAOPPORTUNITIES].[NAME] o6, [ENMEAOPPORTUNITIES].[COST] o7, [ENMEAOPPORTUNITIES].[WEIGHT_ACTUAL] o8, [ENMEAOPPORTUNITIES].[WEIGHT_TARGET] o9, [ENMEAOPPORTUNITIES].[PAIRTRADE] o10, [ENMEAOPPORTUNITIES].[ORDERSTAGE] o11, [ENMEAOPPORTUNITIES].[ORDERUPDATE] o12, [ENMEAOPPORTUNITIES].[CURRENCY] o13, [ENMEAOPPORTUNITIES].[DB_LASTPRICE] o14, [ENMEAOPPORTUNITIES].[FX] o15, [ENMEAOPPORTUNITIES].[FXDATETIME] o16, [ENMEAOPPORTUNITIES].[FXTICKER] o17, [ENMEAOPPORTUNITIES].[FUNDCURRENCY] o18, [ENMEAOPPORTUNITIES].[COUNTERVALUELOCAL] o19, [ENMEAOPPORTUNITIES].[COUNTERVALUEFUNDCRNCY] o20, [ENMEAOPPORTUNITIES].[HIST_PX_LASTMONTH] o21, [ENMEAOPPORTUNITIES].[HIST_DATE] o22, [ENMEAOPPORTUNITIES].[BROKERCODE] o23, [ENMEAOPPORTUNITIES].[BBG_PARENTCOID] o24, [ENMEAOPPORTUNITIES].[COUNTRY_ISO] o25, [ENMEAOPPORTUNITIES].[COUNTRY_FULL_NAME] o26 "
                      "FROM [outsys_prod].DBO.[OSUSR_SKP_MEAOPPORTUNITIES] [ENMEAOPPORTUNITIES] WHERE ([ENMEAOPPORTUNITIES].[ID] = '"+str(MEAorder_ID)+"')")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_MEAOPPORTUNITIES] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(MEAorder_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif LevEuroID is not None:
            select = ("SELECT [ENLEVEURO].[ID] o0, [ENLEVEURO].[LEVEUROSTRID] o1, [ENLEVEURO].[DATE] o2, [ENLEVEURO].[INSTRTYPE] o3, [ENLEVEURO].[SECURITYTYPE] o4, [ENLEVEURO].[QUANTITY] o5, [ENLEVEURO].[TICKER_ISIN] o6, [ENLEVEURO].[NAME] o7, [ENLEVEURO].[COST] o8, [ENLEVEURO].[WEIGHT_ACTUAL] o9, [ENLEVEURO].[WEIGHT_TARGET] o10, [ENLEVEURO].[PAIRTRADE] o11, [ENLEVEURO].[ORDERSTAGE] o12, [ENLEVEURO].[ORDERUPDATE] o13, [ENLEVEURO].[CURRENCY] o14, [ENLEVEURO].[DB_LASTPRICE] o15, [ENLEVEURO].[FX] o16, [ENLEVEURO].[FXDATETIME] o17, [ENLEVEURO].[FXTICKER] o18, [ENLEVEURO].[FUNDCURRENCY] o19, [ENLEVEURO].[COUNTERVALUELOCAL] o20, [ENLEVEURO].[COUNTERVALUEFUNDCRNCY] o21, [ENLEVEURO].[HIST_PX_LASTMONTH] o22, [ENLEVEURO].[HIST_DATE] o23, [ENLEVEURO].[BBG_PARENTCOID] o24, [ENLEVEURO].[DAILYPL] o25, [ENLEVEURO].[SININCEPTREPOPL] o26, [ENLEVEURO].[SININCEPACCRUAL] o27, [ENLEVEURO].[SININCEPMTK] o28, [ENLEVEURO].[REALISEDPL] o29, [ENLEVEURO].[TOTALPL] o30, [ENLEVEURO].[CARRYYEARLY] o31, [ENLEVEURO].[CARRYLEFT] o32, [ENLEVEURO].[YAS_BOND_YLD] o33, [ENLEVEURO].[YAS_ASW_SPREAD] o34, [ENLEVEURO].[YAS_MOD_DUR] o35, [ENLEVEURO].[YAS_RISK] o36, [ENLEVEURO].[RISK_MID] o37, [ENLEVEURO].[DELTA_MID] o38, [ENLEVEURO].[PX_BID] o39, [ENLEVEURO].[PX_ASK] o40, [ENLEVEURO].[INT_ACC] o41, [ENLEVEURO].[SETTLE_DATE] o42, [ENLEVEURO].[RTG_SP_LT_LC_ISSUER_CREDIT] o43, [ENLEVEURO].[NXT_CPN_DT] o44, [ENLEVEURO].[NEXT_CASH_FLOW] o45, [ENLEVEURO].[AMT_OUTSTANDING] o46, [ENLEVEURO].[OPT_UNDL_TICKER] o47, [ENLEVEURO].[ISREPO] o48, [ENLEVEURO].[LEEUROREPOID] o49, [ENLEVEURO].[STRATEGYID] o50, [ENLEVEURO].[REPOEXPIRYDATE] o51 "
                      "FROM [outsys_prod].DBO.[OSUSR_SKP_LEVEURO] [ENLEVEURO] WHERE ([ENLEVEURO].[ID] = '"+str(LevEuroID)+"')")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_LEVEURO] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(LevEuroID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif RaffaelloID is not None:
            select = ("SELECT [ENRAFFAELLO].[ID] o0, [ENRAFFAELLO].[DATE] o1, [ENRAFFAELLO].[INSTRTYPE] o2, [ENRAFFAELLO].[SECURITYTYPE] o3, [ENRAFFAELLO].[QUANTITY] o4, [ENRAFFAELLO].[TICKER_ISIN] o5, [ENRAFFAELLO].[NAME] o6, [ENRAFFAELLO].[COST] o7, [ENRAFFAELLO].[WEIGHT_ACTUAL] o8, [ENRAFFAELLO].[WEIGHT_TARGET] o9, [ENRAFFAELLO].[PAIRTRADE] o10, [ENRAFFAELLO].[TRADINGINDEX] o11, [ENRAFFAELLO].[ORDERSTAGE] o12, [ENRAFFAELLO].[ORDERUPDATE] o13, [ENRAFFAELLO].[CURRENCY] o14, [ENRAFFAELLO].[DB_LASTPRICE] o15, [ENRAFFAELLO].[FX] o16, [ENRAFFAELLO].[FXDATETIME] o17, [ENRAFFAELLO].[FXTICKER] o18, [ENRAFFAELLO].[FUNDCURRENCY] o19, [ENRAFFAELLO].[COUNTERVALUELOCAL] o20, [ENRAFFAELLO].[COUNTERVALUEFUNDCRNCY] o21, [ENRAFFAELLO].[HIST_PX_LASTMONTH] o22, [ENRAFFAELLO].[HIST_DATE] o23, [ENRAFFAELLO].[BROKERCODE] o24, [ENRAFFAELLO].[BBG_PARENTCOID] o25, [ENRAFFAELLO].[COUNTRY_ISO] o26, [ENRAFFAELLO].[COUNTRY_FULL_NAME] o27 "
                      "FROM [outsys_prod].DBO.[OSUSR_SKP_RAFFAELLO] [ENRAFFAELLO] WHERE ([ENRAFFAELLO].[ID] = '"+str(RaffaelloID)+"')")
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_RAFFAELLO] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(RaffaelloID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        return None


def PortfolioUpdatedMethod(query_set, PortfolioUpdated):
    """
    This method used to reduce redundancy  of code. Calculate  PreviousQuantity, PreviousPrice

    :param query_set: query set
    :param PortfolioUpdated: PortfolioUpdated boolean field

    :return: PreviousQuantity, PreviousPrice

    """
    PreviousQuantity, PreviousPrice = 0, 0
    if not PortfolioUpdated:
        PreviousQuantity = query_set[0][4]
        PreviousPrice = query_set[0][7]
    return PreviousQuantity, PreviousPrice


def BuySellCalculator(PreviousQuantity, PreviousPrice, Side, ExecutedQuantity, ExecutionPrice):
    """
    This method called to calculate Buy sell values

    :param PreviousQuantity:  PreviousQuantity
    :param PreviousPrice:  PreviousPrice
    :param Side:  Side
    :param ExecutedQuantity:  ExecutedQuantity
    :param ExecutionPrice:  ExecutionPrice

    :return:  NewQuantity, NewPrice, Executed

    """
    NewQuantity, NewPrice, Executed = 0, 0, 0
    if Side == "BUY":
        if PreviousQuantity < 0:
            Executed = True
            NewQuantity = (PreviousQuantity + ExecutedQuantity)
            NewPrice = PreviousPrice
        else:
            Executed = True
            NewQuantity = (PreviousQuantity + ExecutedQuantity)
            NewPrice = ((PreviousPrice * PreviousQuantity) + (ExecutedQuantity * ExecutionPrice)) / NewQuantity
    elif Side == "SELL":
        if PreviousQuantity < 0:
            Executed = True
            NewQuantity = (PreviousQuantity - ExecutedQuantity)
            NewPrice = ((PreviousPrice * PreviousQuantity) + (ExecutedQuantity * ExecutionPrice)) / NewQuantity
        else:
            Executed = True
            NewQuantity = (PreviousQuantity - ExecutedQuantity)
            NewPrice = PreviousPrice
    elif Side == "BCOV":
        Executed = True
        NewQuantity = (PreviousQuantity + ExecutedQuantity)
        PreviousPrice = PreviousPrice
    elif Side == "SSELL":
        Executed = True
        NewQuantity = (PreviousQuantity - ExecutedQuantity)
        NewPrice = ((PreviousPrice * PreviousQuantity) + (ExecutedQuantity * ExecutionPrice)) / NewQuantity

    NewPrice = abs(NewPrice)
    return NewQuantity, NewPrice, Executed


def UpdatePortfolio(OrderID, GiveMeDate):
    """
    This is method starting point of UpdatePortfolio script. Using this method we can updated portfolios.

    :param OrderID: order id
    :param GiveMeDate: Date

    :return:  Update Portfolio based on different funds.

    """

    print("UpdatePortfolio process called....!!!")
    conn = database_dev()
    cursor = conn.cursor()
    CurrTime = get_current_date()
    CurrDateTime = get_current_datetime()
    PreviousQuantity, PreviousPrice = 0, 0
    GetRecords, region_id, db_name = [], "NULL", None
    if GiveMeDate is None:
        GiveMeDate = get_current_date_time()
    else:
        GiveMeDate = GiveMeDate.strftime("%Y-%m-%d %H:%M:%S")

    LoopOrders = (
            "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[CASA4FUND_FUNDNAME] o76, [ENORDERS].[CURRENCY] o77, [ENORDERS].[CASA4FUNDSECURITYTYPE] o78, [ENORDERS].[EXECUTEDQUANTITY] o79, [ENORDERS].[PENDINGQUANTITY] o80, [ENORDERS].[ORDERFROMDAYBEFORE] o81, [ENORDERS].[REBALANCE] o82, [ENORDERS].[ISFROMYESTERDAY] o83, [ENORDERS].[LAST_PRICE] o84, [ENORDERS].[C4F_BROKERCODE] o85, [ENORDERS].[FUNDNAV] o86, [ENORDERS].[FUNDCURRENCY] o87, [ENORDERS].[STOCKCURRENCY] o88, [ENORDERS].[SETTLEMENTCURRENCY] o89, [ENORDERS].[URGENCY] o90, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o91, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o92, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o93, [ENORDERS].[TRADEQUANTITYCALCULATED] o94, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o95, [ENORDERS].[BBGSECURITYNAME] o96, [ENORDERS].[BBGEXCHANGE] o97, [ENORDERS].[ORDERCLOSE] o98, [ENORDERS].[PRECISEINSTRUCTIONS] o99, [ENORDERS].[ISIN] o100, [ENORDERS].[COUNTRY] o101, [ENORDERS].[ORDERSTAGEOWL] o102, [ENORDERS].[APICORRELATIONID] o103, [ENORDERS].[APIORDERREFID] o104, [ENORDERS].[SETTLEMENTDATE] o105, [ENORDERS].[BBGMESS1] o106, [ENORDERS].[BBGSETTLEDATE] o107, [ENORDERS].[BBGEMSXSTATUS] o108, [ENORDERS].[BBGEMSXSEQUENCE] o109, [ENORDERS].[BBGEMSXROUTEID] o110, [ENORDERS].[BBGCOUNTRYISO] o111, [ENORDERS].[BROKERBPS] o112, [ENORDERS].[BROKERCENTPERSHARE] o113, [ENORDERS].[TRADINGCOMMISSIONSBPS] o114, [ENORDERS].[TRADINGCOMMISSIONSCENT] o115, [ENORDERS].[BBG_OPTCONTSIZE] o116, [ENORDERS].[BBG_FUTCONTSIZE] o117, [ENORDERS].[BBG_IDPARENTCO] o118, [ENORDERS].[BBG_LOTSIZE] o119, [ENORDERS].[BBG_MARKETOPENINGTIME] o120, [ENORDERS].[BBG_MARKETCLOSINGTIME] o121, [ENORDERS].[BBG_PRICEINVALID] o122, [ENORDERS].[BBG_OPT_UNDL_PX] o123, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o124, [ENORDERS].[POTENTIALERROR] o125, [ENORDERS].[RMSTATUS] o126, [ENORDERS].[ACTUALQUANTITY] o127, [ENORDERS].[RM1] o128, [ENORDERS].[RM2] o129, [ENORDERS].[RM3] o130, [ENORDERS].[RM4] o131, [ENORDERS].[RM5] o132, [ENORDERS].[RM6] o133, [ENORDERS].[RM7] o134, [ENORDERS].[RM8] o135, [ENORDERS].[RM9] o136, [ENORDERS].[RM10] o137, [ENORDERS].[BBG_PARENTCOID] o138, [ENORDERS].[BBG_PARENTCONAME] o139, [ENORDERS].[TRADERNOTES] o140, [ENORDERS].[PORTFOLIOUPDATED] o141, [ENORDERS].[UPDALREADYADDED] o142, [ENORDERS].[UPDALREADYSUBTRACTED] o143, [ENORDERS].[BROKERSELMETHOD] o144, [ENORDERS].[BROKERSELREASON] o145, [ENORDERS].[EXECUTORFACTOR_COST] o146, [ENORDERS].[EXECUTORFACTOR_SPEED] o147, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o148, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o149, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o150, [ENORDERS].[EXECUTORFACTOR_NATURE] o151, [ENORDERS].[EXECUTORFACTOR_VENUE] o152, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o153, [ENORDERS].[NEEDCOMMENT] o154, [ENORDERS].[FX_BASKETRUNID] o155, [ENORDERS].[FIX_CONF] o156, [ENORDERS].[FIX_CIORDID] o157, [ENORDERS].[FIX_EXECUTIONID] o158, [ENORDERS].[FIX_AVGPX] o159, [ENORDERS].[FIX_FAR_AVGPX] o160, [ENORDERS].[FIX_LASTQTY] o161, [ENORDERS].[FIX_FAR_LASTQTY] o162, [ENORDERS].[FIX_LEAVESQTY] o163, [ENORDERS].[OUTRIGHTORSWAP] o164, [ENORDERS].[CURRENCY_2] o165, [ENORDERS].[JPMORGANACCOUNT] o166, [ENORDERS].[BROKERCODEAUTO] o167, [ENORDERS].[EXPOSURETRADEID] o168, [ENORDERS].[BROKERHASBEENCHANGED] o169, [ENORDERS].[UBSACCOUNT] o170, [ENORDERS].[RISKMANAGEMENTRESULT] o171, [ENORDERS].[ARRIVALPRICE] o172, [ENORDERS].[ADV20D] o173, [ENORDERS].[MEAMULTIPLIER] o174, [ENORDERS].[LEVEUROSTATEGYID] o175, [ENORDERS].[ISREPO] o176, [ENORDERS].[REPOEPIRYDATE] o177, [ENORDERS].[REPO_CODEDOSSIER] o178, [ENORDERS].[REPO_VALEURTAUX] o179, [ENORDERS].[REPO_BICSENDER] o180, [ENORDERS].[REPO_CODECONTREPARTIE] o181, [ENORDERS].[REPO_COMPARTIMENT] o182, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o183, [ENORDERS].[REPO_NOMTAUX] o184, [ENORDERS].[REPO_REFERENCEEXTERNE] o185, [ENORDERS].[REPO_BASECALCULINTERET] o186, [ENORDERS].[REPO_TERMDATE] o187, [ENORDERS].[REPO_HAIRCUT] o188, [ENORDERS].[REPO_INTEREST_RATE] o189, [ENORDERS].[LEVEUROSETTLEDATE] o190, [ENORDERS].[REPO_2RDLEG_PRICE] o191, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o192, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o193, [ENORDERS].[LEIREPORTINGCODE] o194, [ENORDERS].[BROKERCODE] o195, [ENORDERS].[MASTERAGREEMENT] o196, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o197, [ENORDERS].[REPO_SFTR] o198, [ENORDERS].[REPO_REAL] o199, [ENORDERS].[APPROVALDATETIMEMILLI] o200, [ENORDERS].[REPO_UTI] o201 "
            "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[DATE] = '" + str(GiveMeDate) + "') AND ([ENORDERS].[EXECUTEDQUANTITY] > convert(decimal(37,8), 0))  AND ([ENORDERS].[ID] = '" + str(OrderID) + "') ORDER BY [ENORDERS].[CREATIONTIME] ASC ")
    cursor.execute(LoopOrders)
    LoopOrders_rec = cursor.fetchall()

    for i, _ in enumerate(LoopOrders_rec):
        LP = LoopOrders_rec[i]
        NewFrontiers_ID = LP[64]
        GreaterChinaLS_ID = LP[56]
        NorthAmericaLS_ID = LP[57]
        EuroBond_ID = LP[53]
        EuropeanValue_ID = LP[8]
        Chiron_ID = LP[63]
        ItalyLS_ID = LP[55]
        OrderUpdate = "Position updated at" + str(CurrTime)
        Rosemary_ID = LP[65]
        Side = LP[2]
        ExecutedQuantity = LP[79]
        ExecutionPrice = LP[50]
        OrderID = LP[0]
        updAlreadySubtracted = LP[143]
        updAlreadyAdded = LP[142]
        OrderUpdatingID = LP[71]
        PortfolioUpdated = LP[141]
        TickerISIN = LP[34]
        AsianAlpha_ID = LP[67]
        HighFocusID = LP[69]
        Meaopportunities_ID = LP[68]
        LevEuroID = LP[73]
        Raffaelloid = LP[75]

        if ExecutedQuantity == abs(int(updAlreadySubtracted)) or ExecutedQuantity == abs(int(updAlreadyAdded)):
            continue
        else:
            if PortfolioUpdated:
                pf_q = (
                        "SELECT [ENORDERSUPDATING].[ID] o0, [ENORDERSUPDATING].[NORTHAMERICAID] o1, [ENORDERSUPDATING].[GREATERCHINAID] o2, [ENORDERSUPDATING].[ITALYID] o3, [ENORDERSUPDATING].[NEWFRONTIERSID] o4, [ENORDERSUPDATING].[EUROBONDID] o5, [ENORDERSUPDATING].[ROSEMARYID] o6, [ENORDERSUPDATING].[CHIRONID] o7, [ENORDERSUPDATING].[EUROPEANVALUE] o8, [ENORDERSUPDATING].[ORDERID] o9, [ENORDERSUPDATING].[ASIANALPHAID] o10, [ENORDERSUPDATING].[HIGHFOCUSID] o11, [ENORDERSUPDATING].[MEAOPPORTUNITIESID] o12, [ENORDERSUPDATING].[LEVEUROID] o13, [ENORDERSUPDATING].[DATETIME] o14, [ENORDERSUPDATING].[QUANTITYBEFOREUPDATE] o15, [ENORDERSUPDATING].[QUANTITYAFTERUPDATE] o16, [ENORDERSUPDATING].[EXECUTEDQUANTITY] o17, [ENORDERSUPDATING].[EXECUTIONPRICE] o18, [ENORDERSUPDATING].[AVERAGEPRICEBEFOREUPDATE] o19, [ENORDERSUPDATING].[AVERAGEPRICEAFTERUPDATE] o20, [ENORDERSUPDATING].[SIDE] o21, [ENORDERSUPDATING].[TICKERISIN] o22, [ENORDERSUPDATING].[RAFFAELLOID] o23, [ENORDERSUPDATING].[IMOLAID] o24, [ENORDERSUPDATING].[MONTECUCCOLIID] o25 "
                        "FROM [outsys_prod].DBO.[OSUSR_BOL_ORDERSUPDATING] [ENORDERSUPDATING] WHERE ([ENORDERSUPDATING].[ID] = '" + str(
                    OrderUpdatingID) + "')")
                cursor.execute(pf_q)
                PreviousQuAndPrice = cursor.fetchall()
                for j, _ in enumerate(PreviousQuAndPrice):
                    previous_qu_price = PreviousQuAndPrice[i]
                    PreviousQuantity = previous_qu_price[15]
                    PreviousPrice = previous_qu_price[19]

                if ItalyLS_ID is not None:
                    itely_query = (
                             "SELECT  [ENITALYLS].[ID] o0, [ENITALYLS].[DATE] o1, [ENITALYLS].[INSTRTYPE] o2, [ENITALYLS].[SECURITYTYPE] o3, [ENITALYLS].[QUANTITY] o4, [ENITALYLS].[TICKER_ISIN] o5, [ENITALYLS].[NAME] o6, [ENITALYLS].[COST] o7, [ENITALYLS].[WEIGHT_ACTUAL] o8, [ENITALYLS].[WEIGHT_TARGET] o9, [ENITALYLS].[PAIRTRADE] o10, [ENITALYLS].[ORDERSTAGE] o11, [ENITALYLS].[ORDERUPDATE] o12, [ENITALYLS].[CURRENCY] o13, [ENITALYLS].[DB_LASTPRICE] o14, [ENITALYLS].[FUNDCURRENCY] o15, [ENITALYLS].[FX] o16, [ENITALYLS].[FXDATETIME] o17, [ENITALYLS].[FXTICKER] o18, [ENITALYLS].[COUNTERVALUELOCAL] o19, [ENITALYLS].[COUNTERVALUEFUNDCRNCY] o20, [ENITALYLS].[HIST_PX_LASTMONTH] o21, [ENITALYLS].[HIST_DATE] o22, [ENITALYLS].[BROKERCODE] o23, [ENITALYLS].[BBG_PARENTCOID] o24"
                            "FROM [outsys_prod].DBO.[OSUSR_38P_ITALYLS] [ENITALYLS] WHERE ([ENITALYLS].[ID] = '" + str(
                        ItalyLS_ID) + "') ORDER BY [ENITALYLS].[NAME] ASC ")
                    cursor.execute(itely_query)
                    GetRecords = cursor.fetchall()
                    region_id = ItalyLS_ID
                    db_name = 'OSUSR_38P_ITALYLS'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(itely_query, PortfolioUpdated)
                elif EuropeanValue_ID is not None:
                    select_query = (
                            "SELECT [ENEUROPEANVALUE].[ID] o0, [ENEUROPEANVALUE].[DATE] o1, [ENEUROPEANVALUE].[INSTRTYPE] o2, [ENEUROPEANVALUE].[SECURITYTYPE] o3, [ENEUROPEANVALUE].[QUANTITY] o4, [ENEUROPEANVALUE].[TICKER_ISIN] o5, [ENEUROPEANVALUE].[NAME] o6, [ENEUROPEANVALUE].[COST] o7, [ENEUROPEANVALUE].[WEIGHT_ACTUAL] o8, [ENEUROPEANVALUE].[WEIGHT_TARGET] o9, [ENEUROPEANVALUE].[PAIRTRADE] o10, [ENEUROPEANVALUE].[ORDERSTAGE] o11, [ENEUROPEANVALUE].[ORDERUPDATE] o12, [ENEUROPEANVALUE].[CURRENCY] o13, [ENEUROPEANVALUE].[DB_LASTPRICE] o14, [ENEUROPEANVALUE].[FX] o15, [ENEUROPEANVALUE].[FXDATETIME] o16, [ENEUROPEANVALUE].[FXTICKER] o17, [ENEUROPEANVALUE].[FUNDCURRENCY] o18, [ENEUROPEANVALUE].[COUNTERVALUELOCAL] o19, [ENEUROPEANVALUE].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROPEANVALUE].[HIST_PX_LASTMONTH] o21, [ENEUROPEANVALUE].[HIST_DATE] o22, [ENEUROPEANVALUE].[BBG_PARENTCOID] o23, [ENEUROPEANVALUE].[BROKERCODE] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_EUROPEANVALUE] [ENEUROPEANVALUE] WHERE ([ENEUROPEANVALUE].[ID] = '" + str(
                        EuropeanValue_ID) + "') ORDER BY [ENEUROPEANVALUE].[NAME] ASC ")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = EuropeanValue_ID
                    db_name = 'OSUSR_38P_EUROPEANVALUE'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif GreaterChinaLS_ID is not None:
                    select_query = (
                            "SELECT [ENGREATERCHINALS].[ID] o0, [ENGREATERCHINALS].[DATE] o1, [ENGREATERCHINALS].[INSTRTYPE] o2, [ENGREATERCHINALS].[SECURITYTYPE] o3, [ENGREATERCHINALS].[QUANTITY] o4, [ENGREATERCHINALS].[TICKER_ISIN] o5, [ENGREATERCHINALS].[NAME] o6, [ENGREATERCHINALS].[COST] o7, [ENGREATERCHINALS].[WEIGHT_ACTUAL] o8, [ENGREATERCHINALS].[WEIGHT_TARGET] o9, [ENGREATERCHINALS].[PAIRTRADE] o10, [ENGREATERCHINALS].[ORDERSTAGE] o11, [ENGREATERCHINALS].[ORDERUPDATE] o12, [ENGREATERCHINALS].[CURRENCY] o13, [ENGREATERCHINALS].[DB_LASTPRICE] o14, [ENGREATERCHINALS].[FX] o15, [ENGREATERCHINALS].[FXDATETIME] o16, [ENGREATERCHINALS].[FXTICKER] o17, [ENGREATERCHINALS].[FUNDCURRENCY] o18, [ENGREATERCHINALS].[COUNTERVALUELOCAL] o19, [ENGREATERCHINALS].[COUNTERVALUEFUNDCRNCY] o20, [ENGREATERCHINALS].[HIST_PX_LASTMONTH] o21, [ENGREATERCHINALS].[HIST_DATE] o22, [ENGREATERCHINALS].[BROKERCODE] o23, [ENGREATERCHINALS].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] [ENGREATERCHINALS] WHERE ([ENGREATERCHINALS].[ID] = '" + str(
                        GreaterChinaLS_ID) + "') ORDER BY [ENGREATERCHINALS].[NAME] ASC ")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = GreaterChinaLS_ID
                    db_name = 'OSUSR_38P_GREATERCHINALS'
                    PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif NorthAmericaLS_ID is not None:
                    select_query = (
                            "SELECT [ENNORTHAMERICALS].[ID] o0, [ENNORTHAMERICALS].[DATE] o1, [ENNORTHAMERICALS].[INSTRTYPE] o2, [ENNORTHAMERICALS].[SECURITYTYPE] o3, [ENNORTHAMERICALS].[QUANTITY] o4, [ENNORTHAMERICALS].[TICKER_ISIN] o5, [ENNORTHAMERICALS].[NAME] o6, [ENNORTHAMERICALS].[COST] o7, [ENNORTHAMERICALS].[WEIGHT_ACTUAL] o8, [ENNORTHAMERICALS].[WEIGHT_TARGET] o9, [ENNORTHAMERICALS].[PAIRTRADE] o10, [ENNORTHAMERICALS].[TRADINGINDEX] o11, [ENNORTHAMERICALS].[ORDERSTAGE] o12, [ENNORTHAMERICALS].[ORDERUPDATE] o13, [ENNORTHAMERICALS].[CURRENCY] o14, [ENNORTHAMERICALS].[DB_LASTPRICE] o15, [ENNORTHAMERICALS].[FX] o16, [ENNORTHAMERICALS].[FXDATETIME] o17, [ENNORTHAMERICALS].[FXTICKER] o18, [ENNORTHAMERICALS].[FUNDCURRENCY] o19, [ENNORTHAMERICALS].[COUNTERVALUELOCAL] o20, [ENNORTHAMERICALS].[COUNTERVALUEFUNDCRNCY] o21, [ENNORTHAMERICALS].[HIST_PX_LASTMONTH] o22, [ENNORTHAMERICALS].[HIST_DATE] o23, [ENNORTHAMERICALS].[BROKERCODE] o24, [ENNORTHAMERICALS].[BBG_PARENTCOID] o25 "
                            "FROM [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] [ENNORTHAMERICALS] WHERE ([ENNORTHAMERICALS].[ID] ='" + str(
                        NorthAmericaLS_ID) + "') ORDER BY [ENNORTHAMERICALS].[NAME] ASC ")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = NorthAmericaLS_ID
                    db_name = 'OSUSR_38P_NORTHAMERICALS'
                    PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif EuroBond_ID is not None:
                    select_query = (
                            "SELECT [ENEUROBOND].[ID] o0, [ENEUROBOND].[DATE] o1, [ENEUROBOND].[INSTRTYPE] o2, [ENEUROBOND].[SECURITYTYPE] o3, [ENEUROBOND].[QUANTITY] o4, [ENEUROBOND].[TICKER_ISIN] o5, [ENEUROBOND].[NAME] o6, [ENEUROBOND].[COST] o7, [ENEUROBOND].[WEIGHT_ACTUAL] o8, [ENEUROBOND].[WEIGHT_TARGET] o9, [ENEUROBOND].[PAIRTRADE] o10, [ENEUROBOND].[ORDERSTAGE] o11, [ENEUROBOND].[ORDERUPDATE] o12, [ENEUROBOND].[CURRENCY] o13, [ENEUROBOND].[DB_LASTPRICE] o14, [ENEUROBOND].[FX] o15, [ENEUROBOND].[FXDATETIME] o16, [ENEUROBOND].[FXTICKER] o17, [ENEUROBOND].[FUNDCURRENCY] o18, [ENEUROBOND].[COUNTERVALUELOCAL] o19, [ENEUROBOND].[COUNTERVALUEFUNDCRNCY] o20, [ENEUROBOND].[ENDMONTH_PX] o21, [ENEUROBOND].[HIST_PX_LASTMONTH] o22, [ENEUROBOND].[HIST_DATE] o23, [ENEUROBOND].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_EUROBOND] [ENEUROBOND] WHERE ([ENEUROBOND].[ID] = '" + str(
                        EuroBond_ID) + "') ORDER BY [ENEUROBOND].[DATE] ASC ")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = EuroBond_ID
                    db_name = 'OSUSR_BOL_EUROBOND'
                    PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif Chiron_ID is not None:
                    select_query = (
                            "SELECT [ENCHIRON].[ID] o0, [ENCHIRON].[DATE] o1, [ENCHIRON].[INSTRTYPE] o2, [ENCHIRON].[SECURITYTYPE] o3, [ENCHIRON].[QUANTITY] o4, [ENCHIRON].[TICKER_ISIN] o5, [ENCHIRON].[NAME] o6, [ENCHIRON].[COST] o7, [ENCHIRON].[WEIGHT_ACTUAL] o8, [ENCHIRON].[WEIGHT_TARGET] o9, [ENCHIRON].[PAIRTRADE] o10, [ENCHIRON].[ORDERSTAGE] o11, [ENCHIRON].[ORDERUPDATE] o12, [ENCHIRON].[CURRENCY] o13, [ENCHIRON].[DB_LASTPRICE] o14, [ENCHIRON].[FX] o15, [ENCHIRON].[FXDATETIME] o16, [ENCHIRON].[FXTICKER] o17, [ENCHIRON].[FUNDCURRENCY] o18, [ENCHIRON].[COUNTERVALUELOCAL] o19, [ENCHIRON].[COUNTERVALUEFUNDCRNCY] o20, [ENCHIRON].[HIST_PX_LASTMONTH] o21, [ENCHIRON].[HIST_DATE] o22, [ENCHIRON].[BBG_PARENTCOID] o23 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_CHIRON] [ENCHIRON] WHERE ([ENCHIRON].[ID] = '" + str(
                        Chiron_ID) + "') ORDER BY [ENCHIRON].[NAME] ASC ")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = Chiron_ID
                    db_name = 'OSUSR_BOL_CHIRON'
                    PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif NewFrontiers_ID is not None:
                    select_query = (
                            "SELECT [ENNEWFRONTIERS].[ID] o0, [ENNEWFRONTIERS].[DATE] o1, [ENNEWFRONTIERS].[INSTRTYPE] o2, [ENNEWFRONTIERS].[SECURITYTYPE] o3, [ENNEWFRONTIERS].[QUANTITY] o4, [ENNEWFRONTIERS].[TICKER_ISIN] o5, [ENNEWFRONTIERS].[NAME] o6, [ENNEWFRONTIERS].[COST] o7, [ENNEWFRONTIERS].[WEIGHT_ACTUAL] o8, [ENNEWFRONTIERS].[WEIGHT_TARGET] o9, [ENNEWFRONTIERS].[PAIRTRADE] o10, [ENNEWFRONTIERS].[ORDERSTAGE] o11, [ENNEWFRONTIERS].[ORDERUPDATE] o12, [ENNEWFRONTIERS].[CURRENCY] o13, [ENNEWFRONTIERS].[DB_LASTPRICE] o14, [ENNEWFRONTIERS].[FX] o15, [ENNEWFRONTIERS].[FXDATETIME] o16, [ENNEWFRONTIERS].[FXTICKER] o17, [ENNEWFRONTIERS].[FUNDCURRENCY] o18, [ENNEWFRONTIERS].[COUNTERVALUELOCAL] o19, [ENNEWFRONTIERS].[COUNTERVALUEFUNDCRNCY] o20, [ENNEWFRONTIERS].[HIST_PX_LASTMONTH] o21, [ENNEWFRONTIERS].[HIST_DATE] o22, [ENNEWFRONTIERS].[BROKERCODE] o23, [ENNEWFRONTIERS].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS] [ENNEWFRONTIERS] WHERE ([ENNEWFRONTIERS].[ID] = '" + str(
                        NewFrontiers_ID) + "') ORDER BY [ENNEWFRONTIERS].[NAME] ASC")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = NewFrontiers_ID
                    db_name = 'OSUSR_BOL_NEWFRONTIERS'
                    PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif Rosemary_ID is not None:
                    select_query = (
                            "SELECT [ENROSEMARY].[ID] o0, [ENROSEMARY].[DATE] o1, [ENROSEMARY].[INSTRTYPE] o2, [ENROSEMARY].[SECURITYTYPE] o3, [ENROSEMARY].[QUANTITY] o4, [ENROSEMARY].[TICKER_ISIN] o5, [ENROSEMARY].[NAME] o6, [ENROSEMARY].[COST] o7, [ENROSEMARY].[WEIGHT_ACTUAL] o8, [ENROSEMARY].[WEIGHT_TARGET] o9, [ENROSEMARY].[PAIRTRADE] o10, [ENROSEMARY].[ORDERSTAGE] o11, [ENROSEMARY].[ORDERUPDATE] o12, [ENROSEMARY].[CURRENCY] o13, [ENROSEMARY].[DB_LASTPRICE] o14, [ENROSEMARY].[FX] o15, [ENROSEMARY].[FXDATETIME] o16, [ENROSEMARY].[FXTICKER] o17, [ENROSEMARY].[FUNDCURRENCY] o18, [ENROSEMARY].[COUNTERVALUELOCAL] o19, [ENROSEMARY].[COUNTERVALUEFUNDCRNCY] o20, [ENROSEMARY].[HIST_PX_LASTMONTH] o21, [ENROSEMARY].[HIST_DATE] o22, [ENROSEMARY].[BROKERCODE] o23, [ENROSEMARY].[BBG_PARENTCOID] o24 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY] [ENROSEMARY] WHERE ([ENROSEMARY].[ID] = '" + str(
                        Rosemary_ID) + "') ORDER BY [ENROSEMARY].[NAME] ASC")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = Rosemary_ID
                    db_name = 'OSUSR_BOL_ROSEMARY'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif AsianAlpha_ID is not None:
                    select_query = (
                            "SELECT [ENASIANALPHA].[ID] o0, [ENASIANALPHA].[DATE] o1, [ENASIANALPHA].[INSTRTYPE] o2, [ENASIANALPHA].[SECURITYTYPE] o3, [ENASIANALPHA].[QUANTITY] o4, [ENASIANALPHA].[TICKER_ISIN] o5, [ENASIANALPHA].[ISIN] o6, [ENASIANALPHA].[NAME] o7, [ENASIANALPHA].[COST] o8, [ENASIANALPHA].[WEIGHT_ACTUAL] o9, [ENASIANALPHA].[WEIGHT_TARGET] o10, [ENASIANALPHA].[PAIRTRADE] o11, [ENASIANALPHA].[ORDERSTAGE] o12, [ENASIANALPHA].[ORDERUPDATE] o13, [ENASIANALPHA].[CURRENCY] o14, [ENASIANALPHA].[DB_LASTPRICE] o15, [ENASIANALPHA].[FUNDCURRENCY] o16, [ENASIANALPHA].[FX] o17, [ENASIANALPHA].[FXDATETIME] o18, [ENASIANALPHA].[FXTICKER] o19, [ENASIANALPHA].[COUNTERVALUELOCAL] o20, [ENASIANALPHA].[COUNTERVALUEFUNDCRNCY] o21, [ENASIANALPHA].[HIST_PX_LASTMONTH] o22, [ENASIANALPHA].[HIST_DATE] o23, [ENASIANALPHA].[COUNTRY_ISO] o24, [ENASIANALPHA].[COUNTRY_FULL_NAME] o25, [ENASIANALPHA].[BBG_PARENTCOID] o26, [ENASIANALPHA].[BROKERCODE] o27 "
                            "FROM [outsys_prod].DBO.[OSUSR_BOL_ASIANALPHA] [ENASIANALPHA] WHERE ([ENASIANALPHA].[ID] = '" + str(
                        AsianAlpha_ID) + "') ORDER BY [ENASIANALPHA].[NAME] ASC")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = AsianAlpha_ID
                    db_name = 'OSUSR_BOL_ASIANALPHA'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif HighFocusID is not None:
                    select_query = (
                            "SELECT [ENHIGHFOCUS].[ID] o0, [ENHIGHFOCUS].[DATE] o1, [ENHIGHFOCUS].[INSTRTYPE] o2, [ENHIGHFOCUS].[SECURITYTYPE] o3, [ENHIGHFOCUS].[QUANTITY] o4, [ENHIGHFOCUS].[TICKER_ISIN] o5, [ENHIGHFOCUS].[NAME] o6, [ENHIGHFOCUS].[COST] o7, [ENHIGHFOCUS].[WEIGHT_ACTUAL] o8, [ENHIGHFOCUS].[WEIGHT_TARGET] o9, [ENHIGHFOCUS].[PAIRTRADE] o10, [ENHIGHFOCUS].[TRADINGINDEX] o11, [ENHIGHFOCUS].[ORDERSTAGE] o12, [ENHIGHFOCUS].[ORDERUPDATE] o13, [ENHIGHFOCUS].[CURRENCY] o14, [ENHIGHFOCUS].[DB_LASTPRICE] o15, [ENHIGHFOCUS].[FX] o16, [ENHIGHFOCUS].[FXDATETIME] o17, [ENHIGHFOCUS].[FXTICKER] o18, [ENHIGHFOCUS].[FUNDCURRENCY] o19, [ENHIGHFOCUS].[COUNTERVALUELOCAL] o20, [ENHIGHFOCUS].[COUNTERVALUEFUNDCRNCY] o21, [ENHIGHFOCUS].[HIST_PX_LASTMONTH] o22, [ENHIGHFOCUS].[HIST_DATE] o23, [ENHIGHFOCUS].[BROKERCODE] o24, [ENHIGHFOCUS].[BBG_PARENTCOID] o25, [ENHIGHFOCUS].[COUNTRY_ISO] o26, [ENHIGHFOCUS].[COUNTRY_FULL_NAME] o27 "
                            "FROM [outsys_prod].DBO.[OSUSR_SKP_HIGHFOCUS] [ENHIGHFOCUS] WHERE ([ENHIGHFOCUS].[ID] = '" + str(
                        HighFocusID) + "') ORDER BY [ENASIANALPHA].[NAME] ASC")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = HighFocusID
                    db_name = 'OSUSR_SKP_HIGHFOCUS'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif Meaopportunities_ID is not None:
                    select_query = (
                            "SELECT [ENMEAOPPORTUNITIES].[ID] o0, [ENMEAOPPORTUNITIES].[DATE] o1, [ENMEAOPPORTUNITIES].[INSTRTYPE] o2, [ENMEAOPPORTUNITIES].[SECURITYTYPE] o3, [ENMEAOPPORTUNITIES].[QUANTITY] o4, [ENMEAOPPORTUNITIES].[TICKER_ISIN] o5, [ENMEAOPPORTUNITIES].[NAME] o6, [ENMEAOPPORTUNITIES].[COST] o7, [ENMEAOPPORTUNITIES].[WEIGHT_ACTUAL] o8, [ENMEAOPPORTUNITIES].[WEIGHT_TARGET] o9, [ENMEAOPPORTUNITIES].[PAIRTRADE] o10, [ENMEAOPPORTUNITIES].[ORDERSTAGE] o11, [ENMEAOPPORTUNITIES].[ORDERUPDATE] o12, [ENMEAOPPORTUNITIES].[CURRENCY] o13, [ENMEAOPPORTUNITIES].[DB_LASTPRICE] o14, [ENMEAOPPORTUNITIES].[FX] o15, [ENMEAOPPORTUNITIES].[FXDATETIME] o16, [ENMEAOPPORTUNITIES].[FXTICKER] o17, [ENMEAOPPORTUNITIES].[FUNDCURRENCY] o18, [ENMEAOPPORTUNITIES].[COUNTERVALUELOCAL] o19, [ENMEAOPPORTUNITIES].[COUNTERVALUEFUNDCRNCY] o20, [ENMEAOPPORTUNITIES].[HIST_PX_LASTMONTH] o21, [ENMEAOPPORTUNITIES].[HIST_DATE] o22, [ENMEAOPPORTUNITIES].[BROKERCODE] o23, [ENMEAOPPORTUNITIES].[BBG_PARENTCOID] o24, [ENMEAOPPORTUNITIES].[COUNTRY_ISO] o25, [ENMEAOPPORTUNITIES].[COUNTRY_FULL_NAME] o26 "
                            "FROM [outsys_prod].DBO.[OSUSR_SKP_MEAOPPORTUNITIES] [ENMEAOPPORTUNITIES] WHERE ([ENMEAOPPORTUNITIES].[ID] = '" + str(
                        Meaopportunities_ID) + "') ORDER BY [ENASIANALPHA].[NAME] ASC")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = Meaopportunities_ID
                    db_name = 'OSUSR_SKP_MEAOPPORTUNITIES'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif LevEuroID is not None:
                    select_query = (
                            "SELECT [ENLEVEURO].[ID] o0, [ENLEVEURO].[LEVEUROSTRID] o1, [ENLEVEURO].[DATE] o2, [ENLEVEURO].[INSTRTYPE] o3, [ENLEVEURO].[SECURITYTYPE] o4, [ENLEVEURO].[QUANTITY] o5, [ENLEVEURO].[TICKER_ISIN] o6, [ENLEVEURO].[NAME] o7, [ENLEVEURO].[COST] o8, [ENLEVEURO].[WEIGHT_ACTUAL] o9, [ENLEVEURO].[WEIGHT_TARGET] o10, [ENLEVEURO].[PAIRTRADE] o11, [ENLEVEURO].[ORDERSTAGE] o12, [ENLEVEURO].[ORDERUPDATE] o13, [ENLEVEURO].[CURRENCY] o14, [ENLEVEURO].[DB_LASTPRICE] o15, [ENLEVEURO].[FX] o16, [ENLEVEURO].[FXDATETIME] o17, [ENLEVEURO].[FXTICKER] o18, [ENLEVEURO].[FUNDCURRENCY] o19, [ENLEVEURO].[COUNTERVALUELOCAL] o20, [ENLEVEURO].[COUNTERVALUEFUNDCRNCY] o21, [ENLEVEURO].[HIST_PX_LASTMONTH] o22, [ENLEVEURO].[HIST_DATE] o23, [ENLEVEURO].[BBG_PARENTCOID] o24, [ENLEVEURO].[DAILYPL] o25, [ENLEVEURO].[SININCEPTREPOPL] o26, [ENLEVEURO].[SININCEPACCRUAL] o27, [ENLEVEURO].[SININCEPMTK] o28, [ENLEVEURO].[REALISEDPL] o29, [ENLEVEURO].[TOTALPL] o30, [ENLEVEURO].[CARRYYEARLY] o31, [ENLEVEURO].[CARRYLEFT] o32, [ENLEVEURO].[YAS_BOND_YLD] o33, [ENLEVEURO].[YAS_ASW_SPREAD] o34, [ENLEVEURO].[YAS_MOD_DUR] o35, [ENLEVEURO].[YAS_RISK] o36, [ENLEVEURO].[RISK_MID] o37, [ENLEVEURO].[DELTA_MID] o38, [ENLEVEURO].[PX_BID] o39, [ENLEVEURO].[PX_ASK] o40, [ENLEVEURO].[INT_ACC] o41, [ENLEVEURO].[SETTLE_DATE] o42, [ENLEVEURO].[RTG_SP_LT_LC_ISSUER_CREDIT] o43, [ENLEVEURO].[NXT_CPN_DT] o44, [ENLEVEURO].[NEXT_CASH_FLOW] o45, [ENLEVEURO].[AMT_OUTSTANDING] o46, [ENLEVEURO].[OPT_UNDL_TICKER] o47, [ENLEVEURO].[ISREPO] o48, [ENLEVEURO].[LEEUROREPOID] o49, [ENLEVEURO].[STRATEGYID] o50, [ENLEVEURO].[REPOEXPIRYDATE] o51 "
                            "FROM [outsys_prod].DBO.[OSUSR_SKP_LEVEURO] [ENLEVEURO] WHERE ([ENLEVEURO].[ID] = '" + str(
                        LevEuroID) + "')")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = LevEuroID
                    db_name = 'OSUSR_SKP_LEVEURO'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)
                elif Raffaelloid is not None:
                    select_query = (
                            "SELECT [ENRAFFAELLO].[ID] o0, [ENRAFFAELLO].[DATE] o1, [ENRAFFAELLO].[INSTRTYPE] o2, [ENRAFFAELLO].[SECURITYTYPE] o3, [ENRAFFAELLO].[QUANTITY] o4, [ENRAFFAELLO].[TICKER_ISIN] o5, [ENRAFFAELLO].[NAME] o6, [ENRAFFAELLO].[COST] o7, [ENRAFFAELLO].[WEIGHT_ACTUAL] o8, [ENRAFFAELLO].[WEIGHT_TARGET] o9, [ENRAFFAELLO].[PAIRTRADE] o10, [ENRAFFAELLO].[TRADINGINDEX] o11, [ENRAFFAELLO].[ORDERSTAGE] o12, [ENRAFFAELLO].[ORDERUPDATE] o13, [ENRAFFAELLO].[CURRENCY] o14, [ENRAFFAELLO].[DB_LASTPRICE] o15, [ENRAFFAELLO].[FX] o16, [ENRAFFAELLO].[FXDATETIME] o17, [ENRAFFAELLO].[FXTICKER] o18, [ENRAFFAELLO].[FUNDCURRENCY] o19, [ENRAFFAELLO].[COUNTERVALUELOCAL] o20, [ENRAFFAELLO].[COUNTERVALUEFUNDCRNCY] o21, [ENRAFFAELLO].[HIST_PX_LASTMONTH] o22, [ENRAFFAELLO].[HIST_DATE] o23, [ENRAFFAELLO].[BROKERCODE] o24, [ENRAFFAELLO].[BBG_PARENTCOID] o25, [ENRAFFAELLO].[COUNTRY_ISO] o26, [ENRAFFAELLO].[COUNTRY_FULL_NAME] o27 "
                            "FROM [outsys_prod].DBO.[OSUSR_SKP_RAFFAELLO] [ENRAFFAELLO] WHERE ([ENRAFFAELLO].[ID] = '" + str(
                        Raffaelloid) + "')")
                    cursor.execute(select_query)
                    GetRecords = cursor.fetchall()
                    region_id = Raffaelloid
                    db_name = 'OSUSR_SKP_RAFFAELLO'
                    PreviousQuantity, PreviousPrice = PortfolioUpdatedMethod(select_query, PortfolioUpdated)

                NewQuantity, NewPrice, Executed = BuySellCalculator(PreviousQuantity=PreviousQuantity,
                                                                    PreviousPrice=PreviousPrice, Side=Side,
                                                                    ExecutedQuantity=ExecutedQuantity,
                                                                    ExecutionPrice=ExecutionPrice)
                QuantityAfterUpdate = NewQuantity
                PriceAfterUpdate = NewPrice
                ProcessExecuted = Executed

                # OrderUpdate = fetch_records(GetRecords)
                if region_id != "NULL" and db_name is not None:
                    UpdateSr = ("UPDATE [outsys_prod].DBO."+str(db_name)+" set Quantity = '" + str(
                        QuantityAfterUpdate) + "',COST = '"+str(PriceAfterUpdate)+"', ORDERUPDATE = '"+str(OrderUpdate)+"', where ID = '" + str(region_id) + "'")
                    cursor.execute(UpdateSr)
                    conn.commit()

                q = ("SELECT [ENORDERSUPDATING].[ID] o0, [ENORDERSUPDATING].[NORTHAMERICAID] o1, [ENORDERSUPDATING].[GREATERCHINAID] o2, [ENORDERSUPDATING].[ITALYID] o3, [ENORDERSUPDATING].[NEWFRONTIERSID] o4, [ENORDERSUPDATING].[EUROBONDID] o5, [ENORDERSUPDATING].[ROSEMARYID] o6, [ENORDERSUPDATING].[CHIRONID] o7, [ENORDERSUPDATING].[EUROPEANVALUE] o8, [ENORDERSUPDATING].[ORDERID] o9, [ENORDERSUPDATING].[ASIANALPHAID] o10, [ENORDERSUPDATING].[HIGHFOCUSID] o11, [ENORDERSUPDATING].[MEAOPPORTUNITIESID] o12, [ENORDERSUPDATING].[LEVEUROID] o13, [ENORDERSUPDATING].[DATETIME] o14, [ENORDERSUPDATING].[QUANTITYBEFOREUPDATE] o15, [ENORDERSUPDATING].[QUANTITYAFTERUPDATE] o16, [ENORDERSUPDATING].[EXECUTEDQUANTITY] o17, [ENORDERSUPDATING].[EXECUTIONPRICE] o18, [ENORDERSUPDATING].[AVERAGEPRICEBEFOREUPDATE] o19, [ENORDERSUPDATING].[AVERAGEPRICEAFTERUPDATE] o20, [ENORDERSUPDATING].[SIDE] o21, [ENORDERSUPDATING].[TICKERISIN] o22, [ENORDERSUPDATING].[RAFFAELLOID] o23, [ENORDERSUPDATING].[IMOLAID] o24, [ENORDERSUPDATING].[MONTECUCCOLIID] o25 "
                 "FROM [outsys_prod].DBO.[OSUSR_BOL_ORDERSUPDATING] [ENORDERSUPDATING] WHERE ([ENORDERSUPDATING].[ID] = '"+str(OrderUpdatingID)+"' )")
                cursor.execute(q)
                OrdersUpd = cursor.fetchall()
                if OrdersUpd:
                    UpdateOrd = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_ORDERSUPDATING] set NewFrontiersID = '" +str(NewFrontiers_ID)+"',GreaterChinaID='"+str(GreaterChinaLS_ID)+"',NorthAmericaID = '"+str(NorthAmericaLS_ID)+"',EuroBondID = '"+str(EuroBond_ID)+"',EuropeanValue = '"+str(EuropeanValue_ID)+"',ChironID= '"+str(Chiron_ID)+"',italyID = '"+str(ItalyLS_ID)+"',OrderUpdate= '"+str(OrderUpdate)+"',RosemaryID='"+str(Rosemary_ID)+"',OrderID='"+str(OrderID)+"',DateTime='"+str(CurrDateTime)+"',QuantityBeforeUpdate = '"+str(PreviousQuantity)+"',AveragePriceBeforeUpdate= '"+str(PreviousPrice)+"',ExecutedQuantity = '"+str(ExecutedQuantity)+"',ExecutionPrice='"+str(ExecutionPrice)+"',QuantityAfterUpdate='"+str(QuantityAfterUpdate)+"',AveragePriceAfterUpdate='"+str(PriceAfterUpdate)+"',Side = '"+str(Side)+"',TickerISIN='"+str(TickerISIN)+"',AsianAlphaID = '"+str(AsianAlpha_ID)+"',HighFocusID= '"+str(Meaopportunities_ID)+"',LevEuroID='"+str(LevEuroID)+"'  where ID = '" + str(OrderUpdatingID) + "'")
                    cursor.execute(UpdateOrd)
                    conn.commit()
                    OrdersUpdating_id = OrderUpdatingID
                else:
                    insert_q = ("INSERT INTO [outsys_prod].DBO.[OSUSR_BOL_ORDERSUPDATING] ([NewFrontiersID],[GreaterChinaID],[NorthAmericaID],[EuroBondID],[EuropeanValue],[ChironID],[italyID],[OrderUpdate],[RosemaryID],[OrderID],[DateTime],[QuantityBeforeUpdate],[AveragePriceBeforeUpdate],[ExecutedQuantity],[ExecutionPrice],[QuantityAfterUpdate],[AveragePriceAfterUpdate],[Side],[TickerISIN],[AsianAlphaID],[HighFocusID],[LevEuroID]) "
                         "VALUES('" +str(NewFrontiers_ID)+"','"+str(GreaterChinaLS_ID)+"','"+str(NorthAmericaLS_ID)+"','"+str(EuroBond_ID)+"','"+str(EuropeanValue_ID)+"','"+str(Chiron_ID)+"','"+str(ItalyLS_ID)+"','"+str(OrderUpdate)+"','"+str(Rosemary_ID)+"','"+str(OrderID)+"','"+str(CurrDateTime)+"','"+str(PreviousQuantity)+"','"+str(PreviousPrice)+"','"+str(ExecutedQuantity)+"','"+str(ExecutionPrice)+"','"+str(QuantityAfterUpdate)+"','"+str(PriceAfterUpdate)+"','"+str(Side)+"','"+str(TickerISIN)+"','"+str(AsianAlpha_ID)+"','"+str(Meaopportunities_ID)+"','"+str(LevEuroID)+"') ")
                    cursor.execute(insert_q)
                    conn.commit()
                    OrdersUpdating_id = cursor.lastrowid

                updt_q = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set PortfolioUpdated = '"+str(True)+"',OrdersUpdating = '"+str(OrdersUpdating_id)+"'")
                cursor.execute(updt_q)
                conn.commit()

                if Side == "BUY" or Side == "BCOV":
                    updt_val = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set UpdAlreadyAdded = '" + str(ExecutedQuantity) + "'")
                    cursor.execute(updt_val)
                    conn.commit()

                elif Side == "SELL" or Side == "SSELL":
                    updt_val2 = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set UpdAlreadySubtracted = '" + str(ExecutedQuantity) + "'")
                    cursor.execute(updt_val2)
                    conn.commit()
                else:
                    """ AbortTransaction - this issues a database rollback command this undoes all the changes committed
                     into database till the last commit"""
                    curs = conn.cursor()
                    curs.execute("ROLLBACK")
                    conn.commit()
                    # conn.rollback()  # alternative way to rollback
                conn.close()
    return None


def Order_Changes(OrderID, UserID, TimeCreation, TickerISIN, LimitPriceChanges, ExecutedQuantity, ChangeBroker,
                  NewBrokerShortCode):
    """
    This method called to dump order records into OSUSR_SKP_ORDERCHANGES table.

    :param OrderID:  order id
    :param UserID: user id
    :param TimeCreation:  creation time
    :param TickerISIN: TickerISIN
    :param LimitPriceChanges:  LimitPriceChanges
    :param ExecutedQuantity: ExecutedQuantity
    :param ChangeBroker: ChangeBroker
    :param NewBrokerShortCode: NewBrokerShortCode

    :return: New created id.

    """
    print("Order_Changes  process---!!!!")
    conn = database_dev()
    cursor = conn.cursor()
    # CurrTime = get_current_datetime()
    CurrTime = get_current_date_time()
    Executed_Quantity = Hex_To_Decimal(ExecutedQuantity)
    Limit_Price_Changes = Hex_To_Decimal(LimitPriceChanges)
    NewBrokerShortCode = NewBrokerShortCode if NewBrokerShortCode else "NULL"
    insert_record_ORDERCHANGES = (
            "INSERT INTO [outsys_prod].DBO.[OSUSR_SKP_ORDERCHANGES]([OrdersID],[TimeCreation],[TickerISIN],"
            "[LimitPriceChanges],[TimeOfChange],[ExecutedQuantity],[UserID],[BrokerHasBeenChanged],[BrokerShortCode])"
            "VALUES ('" + str(OrderID) + "','"+str(TimeCreation)+"','" + str(TickerISIN) + "','" + str(
        Limit_Price_Changes) + "','"+str(CurrTime)+"','" + str(Executed_Quantity) + "','" + str(UserID) + "','" + str(
        ChangeBroker) + "','" + str(NewBrokerShortCode) + "')")
    print(insert_record_ORDERCHANGES)
    # cursor.execute(insert_record_ORDERCHANGES)
    # conn.commit()
    # conn.close()
    # OSUSR_BOL_CONTROLTICKER_ISIN_ID = cursor.lastrowid
    OSUSR_BOL_CONTROLTICKER_ISIN_ID = None
    return OSUSR_BOL_CONTROLTICKER_ISIN_ID


def Calculations(WeightTarget, WeightActual, FundCurrency, StockCurrency, ShowQuantityBox, PreciseQuantityIndicated,
                 Bbg_Px_Last, ProductType_2ndCol, NAV_or_PCvalue, QuantityToAvoidShortSelling, bbgCountryISO,
                 MEA_Multiplier, BrokeriD, Exchange, bbgFutContSize, bbgOptContSize, ExposureCalculationMethod,
                 UnderlyingPrice):
    """
    perform calculations to get  CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded,
    BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX etc. values
    """
    print("Calculations")
    conn = database_dev()
    cursor = conn.cursor()
    bbgCountryISO = bbgCountryISO if bbgCountryISO else 0
    bbgOptContSize = bbgOptContSize if bbgOptContSize else 0
    CounterValueFundCurrency, CounterValueLocalCurrency, BrokerCommisionsBPS, BrokerBps, \
    BrokerCentPerShare, BrokerCommisionsCENT, PotentialError = 0, 0, 0, 0, 0, 0, None
    QuantityRounded = 0
    WeightTarget = WeightTarget / 100
    if FundCurrency == StockCurrency:
        Fx_FundCrncyVSfundCrncy = 1
        FX = 1
    else:
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
            CounterValueLocalCurrency = 0
            if ProductType_2ndCol == "Bond":
                CounterValueLocalCurrency = (PreciseQuantityIndicated * Bbg_Px_Last) / 100
            if StockCurrency == "GBp":
                if ProductType_2ndCol == "Future Index":
                    (PreciseQuantityIndicated * Bbg_Px_Last / 100 * bbgFutContSize)
                elif ProductType_2ndCol == "Option Equity":
                    (PreciseQuantityIndicated * Bbg_Px_Last / 100 * bbgOptContSize)
                elif ProductType_2ndCol == "Option Index":
                    CounterValueLocalCurrency = (PreciseQuantityIndicated * Bbg_Px_Last * bbgOptContSize)
                else:
                    CounterValueLocalCurrency = PreciseQuantityIndicated * Bbg_Px_Last / 100
            else:
                if ProductType_2ndCol == "Future Index":
                    CounterValueLocalCurrency = (PreciseQuantityIndicated * Bbg_Px_Last * bbgFutContSize)
                elif ProductType_2ndCol == "Option Equity":
                    CounterValueLocalCurrency = (PreciseQuantityIndicated * Bbg_Px_Last * bbgOptContSize)
                elif ProductType_2ndCol == "Option Index":
                    CounterValueLocalCurrency = PreciseQuantityIndicated * Bbg_Px_Last * bbgOptContSize
                else:
                    CounterValueLocalCurrency = PreciseQuantityIndicated * Bbg_Px_Last
        else:
            CounterValueLocalCurrency = 0

        CounterValueLocalCurrency = abs(CounterValueLocalCurrency)

        if CounterValueLocalCurrency == 0:
            CounterValueFundCurrency = 0
            if Fx_FundCrncyVSfundCrncy == 0:
                CounterValueFundCurrency = 0
            else:
                CounterValueFundCurrency = CounterValueLocalCurrency / Fx_FundCrncyVSfundCrncy

        CounterValueFundCurrency = abs(CounterValueFundCurrency)

    if WeightTarget == 0:
        if bbgFutContSize != 0:
            bbgFutContSize = 1
        CounterValueLocalCurrency = abs(QuantityToAvoidShortSelling * Bbg_Px_Last) * bbgFutContSize

        if Fx_FundCrncyVSfundCrncy == 0:
            CounterValueFundCurrency = 0
        else:
            CounterValueFundCurrency = abs(CounterValueLocalCurrency / Fx_FundCrncyVSfundCrncy)

        Quantity = abs(QuantityToAvoidShortSelling)
        try:
            if len(Quantity) >= 6:
                QuantityRounded1 = round(Quantity, 4)
            elif len(Quantity) >= 5:
                QuantityRounded1 = round(Quantity, 3)
            elif len(Quantity) >= 4:
                QuantityRounded1 = round(Quantity, 2)
            else:
                QuantityRounded1 = Quantity
        except:
            QuantityRounded1 = Quantity
        QuantityRounded = abs(QuantityRounded1)
    else:
        if Bbg_Px_Last == 0:
            CounterValueFundCurrency = 0
        else:
            CounterValueFundCurrency = NAV_or_PCvalue * (WeightTarget - WeightActual)
        CounterValueFundCurrency = abs(CounterValueFundCurrency)

        CounterValueLocalCurrency = abs((CounterValueFundCurrency * Fx_FundCrncyVSfundCrncy))

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
        BrokerBps = resultGet1[j][58]
        BrokerCentPerShare = resultGet1[j][60]
        QuantityRounded = Quantity
    if MEA_Multiplier != 1 or MEA_Multiplier != 0:
        Quantity = Quantity * MEA_Multiplier
        QuantityRounded = QuantityRounded * MEA_Multiplier

    BrokerCommisionsBPS = CounterValueLocalCurrency * (BrokerBps / 10000)
    BrokerCommisionsCENT = QuantityRounded * (BrokerCentPerShare / 100)

    return CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded, \
           BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX


def trading_popups(OrderID, ChangeBroker, BrokerID, InsertExecution, ExecutedQuantity, ExecutionPrice, ValueDate,
                   NothingDone, user_id, user_name, fund_id, fund_number, Multiplier, user_comment, instruction, limit):
    """
    This function calle. when used click on save button from trading_popups. Using this process orders record get updated.

    """
    print("trading_popups script called...!!")
    conn = database_dev()
    cursor = conn.cursor()
    limit = limit
    NewTargetWeight = 0
    OrderClose = 0
    ExecutedQuantity = ExecutedQuantity if ExecutedQuantity else 0
    TickerISIN = "NULL"
    Username = user_name
    fundId = fund_id
    ShortCode = None
    try:
        order_query = (
                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[CASA4FUND_FUNDNAME] o76, [ENORDERS].[CURRENCY] o77, [ENORDERS].[CASA4FUNDSECURITYTYPE] o78, [ENORDERS].[EXECUTEDQUANTITY] o79, [ENORDERS].[PENDINGQUANTITY] o80, [ENORDERS].[ORDERFROMDAYBEFORE] o81, [ENORDERS].[REBALANCE] o82, [ENORDERS].[ISFROMYESTERDAY] o83, [ENORDERS].[LAST_PRICE] o84, [ENORDERS].[C4F_BROKERCODE] o85, [ENORDERS].[FUNDNAV] o86, [ENORDERS].[FUNDCURRENCY] o87, [ENORDERS].[STOCKCURRENCY] o88, [ENORDERS].[SETTLEMENTCURRENCY] o89, [ENORDERS].[URGENCY] o90, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o91, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o92, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o93, [ENORDERS].[TRADEQUANTITYCALCULATED] o94, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o95, [ENORDERS].[BBGSECURITYNAME] o96, [ENORDERS].[BBGEXCHANGE] o97, [ENORDERS].[ORDERCLOSE] o98, [ENORDERS].[PRECISEINSTRUCTIONS] o99, [ENORDERS].[ISIN] o100, [ENORDERS].[COUNTRY] o101, [ENORDERS].[ORDERSTAGEOWL] o102, [ENORDERS].[APICORRELATIONID] o103, [ENORDERS].[APIORDERREFID] o104, [ENORDERS].[SETTLEMENTDATE] o105, [ENORDERS].[BBGMESS1] o106, [ENORDERS].[BBGSETTLEDATE] o107, [ENORDERS].[BBGEMSXSTATUS] o108, [ENORDERS].[BBGEMSXSEQUENCE] o109, [ENORDERS].[BBGEMSXROUTEID] o110, [ENORDERS].[BBGCOUNTRYISO] o111, [ENORDERS].[BROKERBPS] o112, [ENORDERS].[BROKERCENTPERSHARE] o113, [ENORDERS].[TRADINGCOMMISSIONSBPS] o114, [ENORDERS].[TRADINGCOMMISSIONSCENT] o115, [ENORDERS].[BBG_OPTCONTSIZE] o116, [ENORDERS].[BBG_FUTCONTSIZE] o117, [ENORDERS].[BBG_IDPARENTCO] o118, [ENORDERS].[BBG_LOTSIZE] o119, [ENORDERS].[BBG_MARKETOPENINGTIME] o120, [ENORDERS].[BBG_MARKETCLOSINGTIME] o121, [ENORDERS].[BBG_PRICEINVALID] o122, [ENORDERS].[BBG_OPT_UNDL_PX] o123, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o124, [ENORDERS].[POTENTIALERROR] o125, [ENORDERS].[RMSTATUS] o126, [ENORDERS].[ACTUALQUANTITY] o127, [ENORDERS].[RM1] o128, [ENORDERS].[RM2] o129, [ENORDERS].[RM3] o130, [ENORDERS].[RM4] o131, [ENORDERS].[RM5] o132, [ENORDERS].[RM6] o133, [ENORDERS].[RM7] o134, [ENORDERS].[RM8] o135, [ENORDERS].[RM9] o136, [ENORDERS].[RM10] o137, [ENORDERS].[BBG_PARENTCOID] o138, [ENORDERS].[BBG_PARENTCONAME] o139, [ENORDERS].[TRADERNOTES] o140, [ENORDERS].[PORTFOLIOUPDATED] o141, [ENORDERS].[UPDALREADYADDED] o142, [ENORDERS].[UPDALREADYSUBTRACTED] o143, [ENORDERS].[BROKERSELMETHOD] o144, [ENORDERS].[BROKERSELREASON] o145, [ENORDERS].[EXECUTORFACTOR_COST] o146, [ENORDERS].[EXECUTORFACTOR_SPEED] o147, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o148, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o149, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o150, [ENORDERS].[EXECUTORFACTOR_NATURE] o151, [ENORDERS].[EXECUTORFACTOR_VENUE] o152, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o153, [ENORDERS].[NEEDCOMMENT] o154, [ENORDERS].[FX_BASKETRUNID] o155, [ENORDERS].[FIX_CONF] o156, [ENORDERS].[FIX_CIORDID] o157, [ENORDERS].[FIX_EXECUTIONID] o158, [ENORDERS].[FIX_AVGPX] o159, [ENORDERS].[FIX_FAR_AVGPX] o160, [ENORDERS].[FIX_LASTQTY] o161, [ENORDERS].[FIX_FAR_LASTQTY] o162, [ENORDERS].[FIX_LEAVESQTY] o163, [ENORDERS].[OUTRIGHTORSWAP] o164, [ENORDERS].[CURRENCY_2] o165, [ENORDERS].[JPMORGANACCOUNT] o166, [ENORDERS].[BROKERCODEAUTO] o167, [ENORDERS].[EXPOSURETRADEID] o168, [ENORDERS].[BROKERHASBEENCHANGED] o169, [ENORDERS].[UBSACCOUNT] o170, [ENORDERS].[RISKMANAGEMENTRESULT] o171, [ENORDERS].[ARRIVALPRICE] o172, [ENORDERS].[ADV20D] o173, [ENORDERS].[MEAMULTIPLIER] o174, [ENORDERS].[LEVEUROSTATEGYID] o175, [ENORDERS].[ISREPO] o176, [ENORDERS].[REPOEPIRYDATE] o177, [ENORDERS].[REPO_CODEDOSSIER] o178, [ENORDERS].[REPO_VALEURTAUX] o179, [ENORDERS].[REPO_BICSENDER] o180, [ENORDERS].[REPO_CODECONTREPARTIE] o181, [ENORDERS].[REPO_COMPARTIMENT] o182, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o183, [ENORDERS].[REPO_NOMTAUX] o184, [ENORDERS].[REPO_REFERENCEEXTERNE] o185, [ENORDERS].[REPO_BASECALCULINTERET] o186, [ENORDERS].[REPO_TERMDATE] o187, [ENORDERS].[REPO_HAIRCUT] o188, [ENORDERS].[REPO_INTEREST_RATE] o189, [ENORDERS].[LEVEUROSETTLEDATE] o190, [ENORDERS].[REPO_2RDLEG_PRICE] o191, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o192, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o193, [ENORDERS].[LEIREPORTINGCODE] o194, [ENORDERS].[BROKERCODE] o195, [ENORDERS].[MASTERAGREEMENT] o196, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o197, [ENORDERS].[REPO_SFTR] o198, [ENORDERS].[REPO_REAL] o199, [ENORDERS].[APPROVALDATETIMEMILLI] o200, [ENORDERS].[REPO_UTI] o201 "
                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
            OrderID) + "') ORDER BY [ENORDERS].[SIDE] ASC ")
        cursor.execute(order_query)
        GetOrderById = cursor.fetchall()
        for i, _ in enumerate(GetOrderById):
            gobi = GetOrderById[i]
            NewTargetWeight = gobi[37]
            # limit = gobi[8]
            TickerISIN = gobi[34]

        query = (
                "SELECT  [ENBROKER].[ID] o0, [ENBROKER].[RELATIONTIPE] o1, [ENBROKER].[MACROFUNCTION] o2, [ENBROKER].[NAME] o3, [ENBROKER]."
                "[C4F_FUNDSBROKERCODE] o4, [ENBROKER].[SHORTCODE] o5, [ENBROKER].[ASSETCLASS] o6, [ENBROKER].[PHONE] o7, [ENBROKER].[ADDRESS] o8, "
                "[ENBROKER].[INFO] o9, [ENBROKER].[MAILINGLIST] o10, [ENBROKER].[MAILINGLIST_SIT_BANORCAP] o11, [ENBROKER].[CREATEDON] o12, "
                "[ENBROKER].[UPDATEDON] o13, [ENBROKER].[FAX] o14 FROM [outsys_prod].DBO.[OSUSR_38P_BROKER] [ENBROKER] "
                "WHERE ([ENBROKER].[ID] = " + str(BrokerID) + ") AND "
                                                              "([ENBROKER].[RELATIONTIPE] = N'Broker') ORDER BY [ENBROKER].[NAME] ASC ")
        cursor.execute(query)
        BrokerInfo_save = cursor.fetchall()

        BrokerID_contactTAB = BrokerID

        for i, _ in enumerate(BrokerInfo_save):
            bis = BrokerInfo_save[i]
            Name = bis[3]
            ShortCode = bis[5]

        if ChangeBroker:
            NeedComment = True
            BrokerSelReason = "CHANGED by the user, additional information needed."
            BrokerSelMethod = "B"
            BrokerBps = 0
            BrokerCentPerShare = 0
            TradingCommissionsBPS = 0
            TradingCommissionsCENT = 0
            BrokerHasBeenChanged = ChangeBroker
            BrokerID_contactTAB = BrokerID
            bnrBroker = Name
            ExecutorFactor_Cost = 0
            ExecutorFactor_Speed = 0
            ExecutorFactor_Likelihood = 0
            ExecutorFactor_Settlement = 0
            ExecutorFactor_OrderSize = 0
            ExecutorFactor_Nature = 0
            ExecutorFactor_Venue = 0
            ExecutorFactor_Consideration = 0
            Broker = ShortCode if ShortCode else "NULL"

            update_q = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set NeedComment = '" + str(NeedComment) + "',"
                                                                                                                  "BrokerSelReason = '" + str(
                BrokerSelReason) + "',BrokerSelMethod = '" + str(BrokerSelMethod) + "',"
                                                                                    "BrokerBps = '" + str(
                BrokerBps) + "',BrokerCentPerShare='" + str(BrokerCentPerShare) + "',TradingCommissionsBPS = '" + str(
                TradingCommissionsBPS) + "',TradingCommissionsCENT = '" + str(
                TradingCommissionsCENT) + "',BrokerHasBeenChanged='" + str(
                BrokerHasBeenChanged) + "',BrokerID_contactTAB = '" + str(
                BrokerID_contactTAB) + "',bnrBroker='" + str(bnrBroker) + "', ExecutorFactor_Cost = '" + str(
                ExecutorFactor_Cost) + "',ExecutorFactor_Speed='" + str(
                ExecutorFactor_Speed) + "',ExecutorFactor_Likelihood='" + str(
                ExecutorFactor_Likelihood) + "',ExecutorFactor_Settlement = '" + str(
                ExecutorFactor_Settlement) + "',ExecutorFactor_OrderSize = '" + str(
                ExecutorFactor_OrderSize) + "',ExecutorFactor_Nature = '" + str(
                ExecutorFactor_Nature) + "',ExecutorFactor_Venue = '" + str(
                ExecutorFactor_Venue) + "', ExecutorFactor_Consideration = '" + str(
                ExecutorFactor_Consideration) + "',Broker = '" + str(Broker) + "' where ID = '" + str(OrderID) + "'")
            cursor.execute(update_q)
            conn.commit()

        if InsertExecution:
            if ExecutedQuantity < 0:
                msg = "Please don't insert negative quantity. The executed quantity has to be positive."
                return msg
            else:
                NewTargetWeight = float(NewTargetWeight) / 100
                ExecutionPrice = ExecutionPrice
                OrderClose = NothingDone
                SettlementDate = ValueDate
                ExecutedQuantity = ExecutedQuantity
        else:
            if limit != 0:
                OrderType = "Limit"
            else:
                OrderType = "Market"

        NewTargetWeight = Hex_To_Decimal(NewTargetWeight / 100)
        ChangingModificationTime = get_current_datetime()
        ExecutionPrice = ExecutionPrice
        OrderStage = "Pending change"
        SettlementDate = ValueDate

        update_q = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set NewTargetWeight = '" + str(
            NewTargetWeight) + "', ChangingModificationTime = (convert(datetime, substring(" + "'" + str(
            ChangingModificationTime) + "'" + ", 1, 10), 120)),ExecutionPrice = '" + str(
            ExecutionPrice) + "',OrderStage='" + str(OrderStage) + "',OrderClose='" + str(
            OrderClose) + "',SettlementDate = (convert(datetime, substring(" + "'" + str(
            SettlementDate) + "'" + ", 1, 10), 120)), LIMIT ='" + str(limit) + "',USERCOMMENT='" + str(
            user_comment) + "',INSTRUCTIONS='" + str(instruction) + "' where ID = '" + str(OrderID) + "'")
        cursor.execute(update_q)
        conn.commit()

        UserID = user_id
        TimeCreation = get_current_date_time()
        Order_Changes(OrderID=OrderID, UserID=UserID, TimeCreation=TimeCreation, TickerISIN=TickerISIN,
                      LimitPriceChanges=limit, ExecutedQuantity=ExecutedQuantity, ChangeBroker=ChangeBroker,
                      NewBrokerShortCode=ShortCode)

        Orders_query = (
                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[CASA4FUND_FUNDNAME] o76, [ENORDERS].[CURRENCY] o77, [ENORDERS].[CASA4FUNDSECURITYTYPE] o78, [ENORDERS].[EXECUTEDQUANTITY] o79, [ENORDERS].[PENDINGQUANTITY] o80, [ENORDERS].[ORDERFROMDAYBEFORE] o81, [ENORDERS].[REBALANCE] o82, [ENORDERS].[ISFROMYESTERDAY] o83, [ENORDERS].[LAST_PRICE] o84, [ENORDERS].[C4F_BROKERCODE] o85, [ENORDERS].[FUNDNAV] o86, [ENORDERS].[FUNDCURRENCY] o87, [ENORDERS].[STOCKCURRENCY] o88, [ENORDERS].[SETTLEMENTCURRENCY] o89, [ENORDERS].[URGENCY] o90, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o91, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o92, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o93, [ENORDERS].[TRADEQUANTITYCALCULATED] o94, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o95, [ENORDERS].[BBGSECURITYNAME] o96, [ENORDERS].[BBGEXCHANGE] o97, [ENORDERS].[ORDERCLOSE] o98, [ENORDERS].[PRECISEINSTRUCTIONS] o99, [ENORDERS].[ISIN] o100, [ENORDERS].[COUNTRY] o101, [ENORDERS].[ORDERSTAGEOWL] o102, [ENORDERS].[APICORRELATIONID] o103, [ENORDERS].[APIORDERREFID] o104, [ENORDERS].[SETTLEMENTDATE] o105, [ENORDERS].[BBGMESS1] o106, [ENORDERS].[BBGSETTLEDATE] o107, [ENORDERS].[BBGEMSXSTATUS] o108, [ENORDERS].[BBGEMSXSEQUENCE] o109, [ENORDERS].[BBGEMSXROUTEID] o110, [ENORDERS].[BBGCOUNTRYISO] o111, [ENORDERS].[BROKERBPS] o112, [ENORDERS].[BROKERCENTPERSHARE] o113, [ENORDERS].[TRADINGCOMMISSIONSBPS] o114, [ENORDERS].[TRADINGCOMMISSIONSCENT] o115, [ENORDERS].[BBG_OPTCONTSIZE] o116, [ENORDERS].[BBG_FUTCONTSIZE] o117, [ENORDERS].[BBG_IDPARENTCO] o118, [ENORDERS].[BBG_LOTSIZE] o119, [ENORDERS].[BBG_MARKETOPENINGTIME] o120, [ENORDERS].[BBG_MARKETCLOSINGTIME] o121, [ENORDERS].[BBG_PRICEINVALID] o122, [ENORDERS].[BBG_OPT_UNDL_PX] o123, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o124, [ENORDERS].[POTENTIALERROR] o125, [ENORDERS].[RMSTATUS] o126, [ENORDERS].[ACTUALQUANTITY] o127, [ENORDERS].[RM1] o128, [ENORDERS].[RM2] o129, [ENORDERS].[RM3] o130, [ENORDERS].[RM4] o131, [ENORDERS].[RM5] o132, [ENORDERS].[RM6] o133, [ENORDERS].[RM7] o134, [ENORDERS].[RM8] o135, [ENORDERS].[RM9] o136, [ENORDERS].[RM10] o137, [ENORDERS].[BBG_PARENTCOID] o138, [ENORDERS].[BBG_PARENTCONAME] o139, [ENORDERS].[TRADERNOTES] o140, [ENORDERS].[PORTFOLIOUPDATED] o141, [ENORDERS].[UPDALREADYADDED] o142, [ENORDERS].[UPDALREADYSUBTRACTED] o143, [ENORDERS].[BROKERSELMETHOD] o144, [ENORDERS].[BROKERSELREASON] o145, [ENORDERS].[EXECUTORFACTOR_COST] o146, [ENORDERS].[EXECUTORFACTOR_SPEED] o147, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o148, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o149, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o150, [ENORDERS].[EXECUTORFACTOR_NATURE] o151, [ENORDERS].[EXECUTORFACTOR_VENUE] o152, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o153, [ENORDERS].[NEEDCOMMENT] o154, [ENORDERS].[FX_BASKETRUNID] o155, [ENORDERS].[FIX_CONF] o156, [ENORDERS].[FIX_CIORDID] o157, [ENORDERS].[FIX_EXECUTIONID] o158, [ENORDERS].[FIX_AVGPX] o159, [ENORDERS].[FIX_FAR_AVGPX] o160, [ENORDERS].[FIX_LASTQTY] o161, [ENORDERS].[FIX_FAR_LASTQTY] o162, [ENORDERS].[FIX_LEAVESQTY] o163, [ENORDERS].[OUTRIGHTORSWAP] o164, [ENORDERS].[CURRENCY_2] o165, [ENORDERS].[JPMORGANACCOUNT] o166, [ENORDERS].[BROKERCODEAUTO] o167, [ENORDERS].[EXPOSURETRADEID] o168, [ENORDERS].[BROKERHASBEENCHANGED] o169, [ENORDERS].[UBSACCOUNT] o170, [ENORDERS].[RISKMANAGEMENTRESULT] o171, [ENORDERS].[ARRIVALPRICE] o172, [ENORDERS].[ADV20D] o173, [ENORDERS].[MEAMULTIPLIER] o174, [ENORDERS].[LEVEUROSTATEGYID] o175, [ENORDERS].[ISREPO] o176, [ENORDERS].[REPOEPIRYDATE] o177, [ENORDERS].[REPO_CODEDOSSIER] o178, [ENORDERS].[REPO_VALEURTAUX] o179, [ENORDERS].[REPO_BICSENDER] o180, [ENORDERS].[REPO_CODECONTREPARTIE] o181, [ENORDERS].[REPO_COMPARTIMENT] o182, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o183, [ENORDERS].[REPO_NOMTAUX] o184, [ENORDERS].[REPO_REFERENCEEXTERNE] o185, [ENORDERS].[REPO_BASECALCULINTERET] o186, [ENORDERS].[REPO_TERMDATE] o187, [ENORDERS].[REPO_HAIRCUT] o188, [ENORDERS].[REPO_INTEREST_RATE] o189, [ENORDERS].[LEVEUROSETTLEDATE] o190, [ENORDERS].[REPO_2RDLEG_PRICE] o191, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o192, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o193, [ENORDERS].[LEIREPORTINGCODE] o194, [ENORDERS].[BROKERCODE] o195, [ENORDERS].[MASTERAGREEMENT] o196, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o197, [ENORDERS].[REPO_SFTR] o198, [ENORDERS].[REPO_REAL] o199, [ENORDERS].[APPROVALDATETIMEMILLI] o200, [ENORDERS].[REPO_UTI] o201 "
                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = " + str(
            OrderID) + ")")
        cursor.execute(Orders_query)
        orders_rec = cursor.fetchall()

        recs = orders_rec[0]
        NorthAmericaLS_ID = recs[57]
        GreaterChinaLS_ID = recs[56]
        ItalyLS_ID = recs[55]
        EuropeanValue_ID = recs[53]
        OrderStage = "Suggestion filled"
        OrderQuantity = Hex_To_Decimal(recs[21])
        OrderDate = recs[1]
        EuroBond_ID = recs[58]
        Chiron_ID = recs[63]
        NewFrontiersID = recs[64]
        RosemaryID = recs[65]
        AsianAlphaID = recs[67]
        HighFocus_ID = recs[69]
        MEAopportunities = recs[68]
        LevEuroID = recs[73]
        Raffaello = recs[75]

        if NorthAmericaLS_ID:
            fundId = NorthAmericaLS_ID
        elif GreaterChinaLS_ID:
            fundId = GreaterChinaLS_ID
        elif ItalyLS_ID:
            fundId = ItalyLS_ID
        elif EuropeanValue_ID:
            fundId = EuropeanValue_ID
        elif EuroBond_ID:
            fundId = EuroBond_ID
        elif Chiron_ID:
            fundId = Chiron_ID
        elif NewFrontiersID:
            fundId = NewFrontiersID
        elif RosemaryID:
            fundId = RosemaryID
        elif AsianAlphaID:
            fundId = AsianAlphaID
        elif HighFocus_ID:
            fundId = HighFocus_ID
        elif MEAopportunities:
            fundId = MEAopportunities
        elif LevEuroID:
            fundId = LevEuroID
        elif Raffaello:
            fundId = Raffaello

        if OrderQuantity == ExecutedQuantity:
            OrderStage = "Filled"
        elif ExecutedQuantity < OrderQuantity:
            OrderStage = "Partially filled"
        else:
            pass

        if InsertExecution:
            Order_Stage(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID, ItalyLS_ID=ItalyLS_ID,
                        EuropeanValue_ID=EuropeanValue_ID, OrderStage=OrderStage, EuroBond_ID=EuroBond_ID,
                        Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID, Rosemary_ID=RosemaryID,
                        AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocus_ID, MEAorder_ID=MEAopportunities,
                        LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)
            PendingQuantity = OrderQuantity - ExecutedQuantity
            ExecutedQuantity = ExecutedQuantity
            PendingQuantity = Hex_To_Decimal(PendingQuantity)

            update_q = (
                    "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set PendingQuantity = '" + str(
                PendingQuantity) + "', ExecutedQuantity= '" + str(
                ExecutedQuantity) + "' where ID = '" + str(OrderID) + "'")
            cursor.execute(update_q)
            conn.commit()

            if PendingQuantity > 0:
                DayOfWeek = day_of_week(OrderDate)
                if int(DayOfWeek) == 5:
                    msg = "An order with the remaining quantity has been created for Monday"
                else:
                    msg = "An order with the remaining quantity has been created for tomorrow"

                listOrderDetail = orders_rec

            # ---------------- Fetch data from orders aggregate to feed data insert query -----------------------------

                RecTickerISIN = orders_rec[0][34]
                FundCode = orders_rec[0][14]
                WeightActual = Hex_To_Decimal(orders_rec[0][36])
                NewTargetWeight = Hex_To_Decimal(orders_rec[0][37])
                BuySellCoverShort = orders_rec[0][2]
                ArborProductType = orders_rec[0][3]
                FundName = orders_rec[0][39]
                out_BrokerShortCode = orders_rec[0][9]
                ProductType_2ndCol = orders_rec[0][42]
                PIDTickerISIN = orders_rec[0][4]
                Limit = orders_rec[0][8]
                OrderType = orders_rec[0][7]
                out_BrokerName = orders_rec[0][43]
                expiry = orders_rec[0][10]
                Fund = orders_rec[0][15]
                Custodian = orders_rec[0][16]
                OrderQtyType = orders_rec[0][20]
                Comment = orders_rec[0][23]
                Account = orders_rec[0][17]
                FundNameShort = orders_rec[0][45]
                InstrumentType_1stCol = orders_rec[0][47]
                out_BrokerID = orders_rec[0][54]
                ValueDate = orders_rec[0][6]

                col_name = col_mapping(fund_number)
                LineID = fundId

                # ----------------------------------------- CreateOrders ---------------------------------------------------
                ctime = get_current_time()
                aCurrentDate = get_current_date()
                CreateOrders = (
                        "INSERT INTO [outsys_prod].DBO.[OSUSR_38P_ORDERS]([CreationTime],[TickerISIN],[FundCode],[ActualWeight],[NewTargetWeight],[Side],[Approved],"
                        "[ProductType],[FundName],[Broker],[TradingDeskConfirmation],[bnrProductType],[ProductID],[Limit],[OrderType],"
                        "[bnrBroker],[Expiry],[Routing],[Fund],[Custodian],[OrderQtyType],[UserComment],[Account],[Date],[FundNameShort],[StockName],"
                        "[IntrumentType],[BrokerID_contactTAB],[SettleDate],[TransactionType],""[" + col_name + "]"") VALUES ('" + str(
                    ctime) + "','" + str(RecTickerISIN) + "','" + str(FundCode) + "','" + str(
                    WeightActual) + "','" + str(NewTargetWeight) + "','" + str(
                    BuySellCoverShort) + "','Approved','" + str(ArborProductType) + "','" + str(
                    FundName) + "','" + str(out_BrokerShortCode) + "'," + str(0) + ",'" + str(
                    ProductType_2ndCol) + "','" + str(PIDTickerISIN) + "','" + str(
                    Limit) + "','" + str(OrderType) + "','" + str(out_BrokerName) + "','" + str(
                    expiry) + "','Automated','" + str(Fund) + "','" + str(Custodian) + "','" + str(
                    OrderQtyType) + "','" + str(Comment) + "','" + str(Account) + "','" + str(
                    aCurrentDate) + "','" + str(FundNameShort) + "','" + str(Name) + "','" + str(
                    InstrumentType_1stCol) + "','" + str(out_BrokerID) + "','" + str(
                    ValueDate) + "','" + str(InstrumentType_1stCol) + "','" + str(LineID) + "')")
                cursor.execute(CreateOrders)
                conn.commit()
                NewOrderID = cursor.lastrowid
                # OrderID = 11143

                q1 = ("SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
                      "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '"+str(NewOrderID)+"') ORDER BY [ENORDERS].[SIDE] ASC ")
                cursor.execute(q1)
                ORD = cursor.fetchall()
                WeightTarget = Hex_To_Decimal(ORD[0][37])
                WeightActual = Hex_To_Decimal(ORD[0][36])
                FundCurrency = ORD[0][89]
                StockCurrency = ORD[0][90]
                ShowQuantityBox = True
                PreciseQuantityIndicated = PendingQuantity
                Bbg_Px_Last = Hex_To_Decimal(ORD[0][86])
                ProductType_2ndCol = ORD[0][42]
                NAV_or_PCvalue = Hex_To_Decimal(ORD[0][88])
                QuantityToAvoidShortSelling = PendingQuantity
                bbgCountryISO = ORD[0][113]
                BrokeriD = ORD[0][54]
                Exchange = ORD[0][99]
                bbgFutContSize = ORD[0][119]
                bbgOptContSize = ORD[0][118]
                ExposureCalculationMethod = ORD[0][126]
                UnderlyingPrice = Hex_To_Decimal(ORD[0][125])
                MEA_Multiplier = Multiplier
                SettlementDate = ORD[0][107]

                CounterValueFundCurrency, CounterValueLocalCurrency, Quantity, QuantityRounded, \
                BrokerCommisionsBPS, BrokerBps, BrokerCentPerShare, BrokerCommisionsCENT, PotentialError, FX = \
                    Calculations(WeightTarget=WeightTarget, WeightActual=WeightActual, FundCurrency=FundCurrency,
                                 StockCurrency=StockCurrency, ShowQuantityBox=ShowQuantityBox,
                                 PreciseQuantityIndicated=PreciseQuantityIndicated, Bbg_Px_Last=Bbg_Px_Last,
                                 ProductType_2ndCol=ProductType_2ndCol, NAV_or_PCvalue=NAV_or_PCvalue,
                                 QuantityToAvoidShortSelling=QuantityToAvoidShortSelling, bbgCountryISO=bbgCountryISO,
                                 BrokeriD=BrokeriD,Exchange=Exchange, bbgFutContSize=bbgFutContSize,
                                 bbgOptContSize=bbgOptContSize, ExposureCalculationMethod=ExposureCalculationMethod,
                                 UnderlyingPrice=UnderlyingPrice, MEA_Multiplier=MEA_Multiplier)

                if int(day_of_week(OrderDate)) == 5:
                    new_date = AddDays(OrderDate, 3)
                elif int(day_of_week(OrderDate)) == 6:
                    new_date = AddDays(OrderDate, 2)
                elif int(day_of_week(OrderDate)) == 0:
                    new_date = AddDays(OrderDate, 1)
                else:
                    new_date = OrderDate

                Date = new_date
                OrderQtyValue = PendingQuantity
                ExecutionPrice = 0
                ExecutedQuantity = 0
                Approved = 'Pending'
                Countervalue = 0
                OrderStage = 'Pending'
                BnrOrderPreciseQuantity = PendingQuantity
                PendingQuantity = 0
                TradingDeskConfirmation = False
                IsFromYesterday = True
                OrderClose = False
                TradingCommissionsBPS = BrokerCommisionsBPS
                TradingCommissionsCENT = BrokerCommisionsCENT
                OrderStageOwl = 'WaitingOwlEmsx'
                BBGEMSXrouteID = 0
                BBGEMSXsequence = 0
                BrokerBps = BrokerBps
                BrokerCentPerShare = BrokerCentPerShare

                if int(day_of_week(SettlementDate)) == 5:
                    new_SettlementDate = AddDays(SettlementDate, 3)
                elif int(day_of_week(SettlementDate)) == 6:
                    new_SettlementDate = AddDays(SettlementDate, 2)
                elif int(day_of_week(SettlementDate)) == 0:
                    new_SettlementDate = AddDays(SettlementDate, 1)
                else:
                    new_SettlementDate = SettlementDate

                SettlementDate = new_SettlementDate
                MEAmultiplier = Multiplier

                update_q = (
                        "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] set Date = '" + str(
                    Date) + "', OrderQtyValue= '" + str(OrderQtyValue) + "',ExecutionPrice='" + str(ExecutionPrice) + "',"
                                                                                                                      " ExecutedQuantity='" + str(
                    ExecutedQuantity) + "',Approved='" + str(Approved) + "',"
                                                                         "Countervalue='" + str(
                    Countervalue) + "',OrderStage='" + str(OrderStage) + "',"
                                                                         "BnrOrderPreciseQuantity='" + str(
                    BnrOrderPreciseQuantity) + "',PendingQuantity='" + str(PendingQuantity) + "',"
                                                                                              "TradingDeskConfirmation='" + str(
                    TradingDeskConfirmation) + "',IsFromYesterday='" + str(IsFromYesterday) + "',OrderClose='" + str(
                    OrderClose) + "',TradingCommissionsBPS='" + str(
                    TradingCommissionsBPS) + "',TradingCommissionsCENT='" + str(
                    TradingCommissionsCENT) + "',OrderStageOwl='" + str(OrderStageOwl) + "',BBGEMSXrouteID='" + str(
                    BBGEMSXrouteID) + "',BBGEMSXsequence='" + str(BBGEMSXsequence) + "',BrokerBps='" + str(
                    BrokerBps) + "',BrokerCentPerShare='" + str(BrokerCentPerShare) + "',SettlementDate='" + str(
                    SettlementDate) + "' where ID = '" + str(NewOrderID) + "'")
                cursor.execute(update_q)
                conn.commit()
            UpdatePortfolio(OrderID=OrderID, GiveMeDate=OrderDate)

        else:
            q2 = ("SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[CASA4FUND_FUNDNAME] o76, [ENORDERS].[CURRENCY] o77, [ENORDERS].[CASA4FUNDSECURITYTYPE] o78, [ENORDERS].[EXECUTEDQUANTITY] o79, [ENORDERS].[PENDINGQUANTITY] o80, [ENORDERS].[ORDERFROMDAYBEFORE] o81, [ENORDERS].[REBALANCE] o82, [ENORDERS].[ISFROMYESTERDAY] o83, [ENORDERS].[LAST_PRICE] o84, [ENORDERS].[C4F_BROKERCODE] o85, [ENORDERS].[FUNDNAV] o86, [ENORDERS].[FUNDCURRENCY] o87, [ENORDERS].[STOCKCURRENCY] o88, [ENORDERS].[SETTLEMENTCURRENCY] o89, [ENORDERS].[URGENCY] o90, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o91, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o92, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o93, [ENORDERS].[TRADEQUANTITYCALCULATED] o94, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o95, [ENORDERS].[BBGSECURITYNAME] o96, [ENORDERS].[BBGEXCHANGE] o97, [ENORDERS].[ORDERCLOSE] o98, [ENORDERS].[PRECISEINSTRUCTIONS] o99, [ENORDERS].[ISIN] o100, [ENORDERS].[COUNTRY] o101, [ENORDERS].[ORDERSTAGEOWL] o102, [ENORDERS].[APICORRELATIONID] o103, [ENORDERS].[APIORDERREFID] o104, [ENORDERS].[SETTLEMENTDATE] o105, [ENORDERS].[BBGMESS1] o106, [ENORDERS].[BBGSETTLEDATE] o107, [ENORDERS].[BBGEMSXSTATUS] o108, [ENORDERS].[BBGEMSXSEQUENCE] o109, [ENORDERS].[BBGEMSXROUTEID] o110, [ENORDERS].[BBGCOUNTRYISO] o111, [ENORDERS].[BROKERBPS] o112, [ENORDERS].[BROKERCENTPERSHARE] o113, [ENORDERS].[TRADINGCOMMISSIONSBPS] o114, [ENORDERS].[TRADINGCOMMISSIONSCENT] o115, [ENORDERS].[BBG_OPTCONTSIZE] o116, [ENORDERS].[BBG_FUTCONTSIZE] o117, [ENORDERS].[BBG_IDPARENTCO] o118, [ENORDERS].[BBG_LOTSIZE] o119, [ENORDERS].[BBG_MARKETOPENINGTIME] o120, [ENORDERS].[BBG_MARKETCLOSINGTIME] o121, [ENORDERS].[BBG_PRICEINVALID] o122, [ENORDERS].[BBG_OPT_UNDL_PX] o123, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o124, [ENORDERS].[POTENTIALERROR] o125, [ENORDERS].[RMSTATUS] o126, [ENORDERS].[ACTUALQUANTITY] o127, [ENORDERS].[RM1] o128, [ENORDERS].[RM2] o129, [ENORDERS].[RM3] o130, [ENORDERS].[RM4] o131, [ENORDERS].[RM5] o132, [ENORDERS].[RM6] o133, [ENORDERS].[RM7] o134, [ENORDERS].[RM8] o135, [ENORDERS].[RM9] o136, [ENORDERS].[RM10] o137, [ENORDERS].[BBG_PARENTCOID] o138, [ENORDERS].[BBG_PARENTCONAME] o139, [ENORDERS].[TRADERNOTES] o140, [ENORDERS].[PORTFOLIOUPDATED] o141, [ENORDERS].[UPDALREADYADDED] o142, [ENORDERS].[UPDALREADYSUBTRACTED] o143, [ENORDERS].[BROKERSELMETHOD] o144, [ENORDERS].[BROKERSELREASON] o145, [ENORDERS].[EXECUTORFACTOR_COST] o146, [ENORDERS].[EXECUTORFACTOR_SPEED] o147, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o148, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o149, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o150, [ENORDERS].[EXECUTORFACTOR_NATURE] o151, [ENORDERS].[EXECUTORFACTOR_VENUE] o152, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o153, [ENORDERS].[NEEDCOMMENT] o154, [ENORDERS].[FX_BASKETRUNID] o155, [ENORDERS].[FIX_CONF] o156, [ENORDERS].[FIX_CIORDID] o157, [ENORDERS].[FIX_EXECUTIONID] o158, [ENORDERS].[FIX_AVGPX] o159, [ENORDERS].[FIX_FAR_AVGPX] o160, [ENORDERS].[FIX_LASTQTY] o161, [ENORDERS].[FIX_FAR_LASTQTY] o162, [ENORDERS].[FIX_LEAVESQTY] o163, [ENORDERS].[OUTRIGHTORSWAP] o164, [ENORDERS].[CURRENCY_2] o165, [ENORDERS].[JPMORGANACCOUNT] o166, [ENORDERS].[BROKERCODEAUTO] o167, [ENORDERS].[EXPOSURETRADEID] o168, [ENORDERS].[BROKERHASBEENCHANGED] o169, [ENORDERS].[UBSACCOUNT] o170, [ENORDERS].[RISKMANAGEMENTRESULT] o171, [ENORDERS].[ARRIVALPRICE] o172, [ENORDERS].[ADV20D] o173, [ENORDERS].[MEAMULTIPLIER] o174, [ENORDERS].[LEVEUROSTATEGYID] o175, [ENORDERS].[ISREPO] o176, [ENORDERS].[REPOEPIRYDATE] o177, [ENORDERS].[REPO_CODEDOSSIER] o178, [ENORDERS].[REPO_VALEURTAUX] o179, [ENORDERS].[REPO_BICSENDER] o180, [ENORDERS].[REPO_CODECONTREPARTIE] o181, [ENORDERS].[REPO_COMPARTIMENT] o182, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o183, [ENORDERS].[REPO_NOMTAUX] o184, [ENORDERS].[REPO_REFERENCEEXTERNE] o185, [ENORDERS].[REPO_BASECALCULINTERET] o186, [ENORDERS].[REPO_TERMDATE] o187, [ENORDERS].[REPO_HAIRCUT] o188, [ENORDERS].[REPO_INTEREST_RATE] o189, [ENORDERS].[LEVEUROSETTLEDATE] o190, [ENORDERS].[REPO_2RDLEG_PRICE] o191, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o192, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o193, [ENORDERS].[LEIREPORTINGCODE] o194, [ENORDERS].[BROKERCODE] o195, [ENORDERS].[MASTERAGREEMENT] o196, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o197, [ENORDERS].[REPO_SFTR] o198, [ENORDERS].[REPO_REAL] o199, [ENORDERS].[APPROVALDATETIMEMILLI] o200, [ENORDERS].[REPO_UTI] o201 "
                    "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = " + str(
                OrderID) + ") ORDER BY [ENORDERS].[SIDE] ASC ")
            cursor.execute(q2)
            fetch_order_rec = cursor.fetchall()
            if ItalyLS_ID is not None:
                to = "banorsicav@banorcapital.com"
                cc = "angelo.meda@banor.it; wst02@banorcapital.com; wst01@banorcapital.com "
                Email_Send_Advisory_Medata(fetch_order_rec,to,cc)
            elif EuropeanValue_ID is not None:
                to = "banorsicav@banorcapital.com"
                cc = "angelo.meda@banor.it; wst02@banorcapital.com; wst01@banorcapital.com "
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            elif Chiron_ID is not None or EuroBond_ID is not None:
                to = "banorsicav@banorcapital.com"
                cc = "angelo.meda@banor.it; wst02@banorcapital.com; wst01@banorcapital.com "
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            elif NewFrontiersID is not None:
                to = "banorsicav@banorcapital.com"
                cc = "andrea.federici@banorcapital.com; luca.clementoni@kallistopartners.com; wst02@banorcapital.com; wst01@banorcapital.com "
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            elif AsianAlphaID is not None:
                to = "banorsicav@banorcapital.com"
                cc = ""
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            elif HighFocus_ID is not None:
                to = "simone.cavallarin@banorcapital.com, wenyan.hao@banorcapital.com"
                cc = "wst02@banorcapital.com; wst01@banorcapital.com"
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            elif LevEuroID is not None:
                to = "simone.cavallarin@banorcapital.com, wenyan.hao@banorcapital.com"
                cc = "wst02@banorcapital.com; wst01@banorcapital.com"
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
            else:
                to = "banorsicav@banorcapital.com"
                cc = "wst02@banorcapital.com; wst01@banorcapital.com"
                Email_Send_Advisory_Medata(fetch_order_rec, to, cc)
        conn.close()
        return "success...!!!"

    except Exception as e:
        print(str(e))
        return jsonify({"Error": str(e), "status": 400})
