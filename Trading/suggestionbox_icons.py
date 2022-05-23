from Funds.configuration import get_current_date_time, get_current_date, get_current_time,database_dev
# from Funds.database_connection_dev import database_dev_env
from Funds.email_send_service import Emails_Send


def Order_Stage2(NorthAmericaLS_ID, GreaterChinaLS_ID, ItalyLS_ID, EuropeanValue_ID, OrderStage, EuroBond_ID, Chiron_ID,
                 NewFrontiersID, Rosemary_ID, AsianAlpha_ID, HighFocus_ID, MEAorder_ID, LevEuroID, RaffaelloID,
                 Username):
    """
        This method update order stage for different funds based on its ID.

    :param NorthAmericaLS_ID: NorthAmericaLS ID
    :param GreaterChinaLS_ID: GreaterChinaLS ID
    :param ItalyLS_ID: ItalyLS ID
    :param EuropeanValue_ID: EuropeanValue ID
    :param OrderStage: OrderStage
    :param EuroBond_ID: EuroBond ID
    :param Chiron_ID: Chiron ID
    :param NewFrontiersID: NewFrontiers ID
    :param Rosemary_ID: Rosemary ID
    :param AsianAlpha_ID: AsianAlpha ID
    :param HighFocus_ID: HighFocus ID
    :param MEAorder_ID: MEAorder ID
    :param LevEuroID: LevEuro ID
    :param RaffaelloID: Raffaello ID
    :param Username: Username

    :return: Its update order stage
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
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ITALYLS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(ItalyLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()

        elif EuropeanValue_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_EUROPEANVALUE] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(ItalyLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif GreaterChinaLS_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_GREATERCHINALS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(GreaterChinaLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif NorthAmericaLS_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_NORTHAMERICALS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(NorthAmericaLS_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif EuroBond_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_EUROBOND] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(EuroBond_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif Chiron_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_CHIRON] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(Chiron_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif NewFrontiersID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_NEWFRONTIERS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(NewFrontiersID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif Rosemary_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_ROSEMARY] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(Rosemary_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif AsianAlpha_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_BOL_ASIANALPHA] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(AsianAlpha_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif HighFocus_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_HIGHFOCUS] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(HighFocus_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif MEAorder_ID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_MEAOPPORTUNITIES] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(MEAorder_ID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif LevEuroID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_LEVEURO] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(LevEuroID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        elif RaffaelloID is not None:
            order_stage = OrderStage
            UpdateSr = ("UPDATE [outsys_prod].DBO.[OSUSR_SKP_RAFFAELLO] set OrderStage = '" + str(
                order_stage) + "' where ID = '" + str(RaffaelloID) + "'")
            cursor.execute(UpdateSr)
            conn.commit()
        else:
            mail = "Screen: List suggestions, Action: Rejecting, -BanoCS logic: OrderStage. Error:" \
                   "The last suggestion received by the trading desk has no fund id! User: " + str(Username)
            Emails_Send(mail)
        return None


def OrderReceivedOnTradingDesk(OrderID, Username):
    """
     This method updates the order stage as 'Working suggestion' based on the order id.

    :param Username: Username
    :param OrderID: OrderID

    :return: Update order stage.

    """

    conn = database_dev()
    cursor = conn.cursor()

    select_query = (
            "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
            "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
        OrderID) + "') ORDER BY [ENORDERS].[SIDE] ASC ")
    cursor.execute(select_query)
    order_details = cursor.fetchone()
    approved_stage = order_details[27]

    if approved_stage != "Pending":
        TradingDeskConfirmation = True
        TradingDeskReceptionTime = get_current_date_time()
        Trader = Username
        # isRebalance = Rebalance
        GetOrders_FundName_IsEmpty = FundName = ""
        OrderStage = "Working suggestion"

        Updateodr = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET TradingDeskConfirmation = %s, "
                     "TradingDeskReceptionTime = %s,Trader = %s,OrderStage = %s"
                     " where ([ID] = '" + str(OrderID) + "')")
        values = (TradingDeskConfirmation, TradingDeskReceptionTime, Trader, OrderStage)
        print(Updateodr, values)
        cursor.execute(Updateodr, values)
        conn.commit()
        NorthAmericaLS_ID = order_details[57]
        GreaterChinaLS_ID = order_details[56]
        ItalyLS_ID = order_details[55]
        EuropenValue_ID = order_details[53]
        EuroBond_ID = order_details[58]
        Chiron_ID = order_details[63]
        NewFrontiersID = order_details[64]
        RosemaryID = order_details[65]
        AsianAlphaID = order_details[67]
        HighFocusID = order_details[69]
        MeaOpportunitiesID = order_details[68]
        LevEuroID = order_details[72]
        Raffaello = order_details[75]
        Order_Stage2(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID,
                     ItalyLS_ID=ItalyLS_ID, EuropeanValue_ID=EuropenValue_ID, OrderStage=OrderStage,
                     EuroBond_ID=EuroBond_ID, Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID,
                     Rosemary_ID=RosemaryID, AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocusID,
                     MEAorder_ID=MeaOpportunitiesID, LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)
        conn.close()
        return "Success..!!"
    else:
        msg = "The suggestion has not been approved yet, you can't insert an order into the market without the " \
              "Investment Manager approval. All the functions are disabled, the order " \
              "spreadsheet will not be produced."
        return msg


# OrderReceivedOnTradingDesk(OrderID=11205, Username='p.sonawane')

def OrdersApproving(order_id, UserId, Username):
    """
    This method approves orders. To approve an order update order stage status as Approved.

    :param order_id: order id which need to be approved.
    :param UserId: user id who approve order
    :param Username: username who approve order

    :return: return success message as order approved
    """
    conn = database_dev()
    cursor = conn.cursor()
    orders_approving_filter = (
                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
            order_id) + "')")
    cursor.execute(orders_approving_filter)
    order_details = cursor.fetchone()
    get_user_role = ("select * from [outsys_prod].DBO.[ossys_User_Role] where USER_ID = '" + str(UserId) + "' ")
    cursor.execute(get_user_role)
    user_rec = cursor.fetchall()
    if user_rec:
        CurrDateTimeMS = get_current_date_time()
        select_user = (
                    "SELECT [ENUSER].[ID] o0, [ENUSER].[NAME] o1, [ENUSER].[USERNAME] o2, [ENUSER].[PASSWORD] o3, [ENUSER].[EMAIL] o4, [ENUSER].[MOBILEPHONE] o5, [ENUSER].[EXTERNAL_ID] o6, [ENUSER].[CREATION_DATE] o7, [ENUSER].[LAST_LOGIN] o8, [ENUSER].[IS_ACTIVE] o9 "
                    "FROM [outsys_prod].DBO.[OSSYS_USER_T9] [ENUSER] WHERE ([ENUSER].[ID] = '" + str(
                UserId) + "') ORDER BY [ENUSER].[NAME] ASC ")
        cursor.execute(select_user)
        user_rec = cursor.fetchone()
        InvestmentManager = user_rec[2]
        ApprovalTime = get_current_time()
        Approved = "Suggestion Approved"
        OrderStage = "Approved"
        ApprovalDateTimeMilli = get_current_date()
        update_order = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET InvestmentManager = %s, "
                        "ApprovalTime = %s,Approved = %s,OrderStage = %s , ApprovalDateTimeMilli = %s"
                        " where ([ID] = '" + str(order_id) + "')")
        values = (InvestmentManager, ApprovalTime, Approved, OrderStage, ApprovalDateTimeMilli)
        print(update_order, values)
        cursor.execute(update_order, values)
        conn.commit()
        NorthAmericaLS_ID = order_details[57]
        GreaterChinaLS_ID = order_details[56]
        ItalyLS_ID = order_details[55]
        EuropenValue_ID = order_details[53]
        EuroBond_ID = order_details[58]
        Chiron_ID = order_details[63]
        NewFrontiersID = order_details[64]
        RosemaryID = order_details[65]
        AsianAlphaID = order_details[67]
        HighFocusID = order_details[69]
        MeaOpportunitiesID = order_details[68]
        LevEuroID = order_details[72]
        Raffaello = order_details[75]
        Order_Stage2(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID,
                     ItalyLS_ID=ItalyLS_ID, EuropeanValue_ID=EuropenValue_ID, OrderStage="Suggestion Approved",
                     EuroBond_ID=EuroBond_ID, Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID,
                     Rosemary_ID=RosemaryID, AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocusID,
                     MEAorder_ID=MeaOpportunitiesID, LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)
        conn.close()
        return "Order stage updated successfully..!"
    else:
        msg = "Apologies, only the Investment Manager can approve or reject a suggestion from an advisor"
        return msg


def OrdersRejecting(order_id, UserId, Username):
    """
    This method rejects orders. To reject an order update the order stage status as Rejected.

       :param order_id: order id which going to be rejected.
       :param UserId: user id who reject order
       :param Username: username who reject order

       :return: return success message as order Rejected

       """
    conn = database_dev()
    cursor = conn.cursor()
    reject_query = (
                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
            order_id) + "')")
    cursor.execute(reject_query)
    order_details = cursor.fetchone()
    get_user_role = ("select * from [outsys_prod].DBO.[ossys_User_Role] where USER_ID = '" + str(UserId) + "' ")
    cursor.execute(get_user_role)
    user_rec = cursor.fetchall()
    if user_rec:
        InvestmentManager = Username
        ApprovalTime = get_current_time()
        Approved = "Suggestion Rejected"
        OrderStage = "Rejected"
        ApprovalDateTimeMilli = get_current_date()
        update_order = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET InvestmentManager = %s, "
                        "ApprovalTime = %s,Approved = %s,OrderStage = %s , ApprovalDateTimeMilli = %s"
                        " where ([ID] = '" + str(order_id) + "')")
        values = (InvestmentManager, ApprovalTime, Approved, OrderStage, ApprovalDateTimeMilli)
        print(update_order, values)
        cursor.execute(update_order, values)
        conn.commit()
        NorthAmericaLS_ID = order_details[57]
        GreaterChinaLS_ID = order_details[56]
        ItalyLS_ID = order_details[55]
        EuropenValue_ID = order_details[53]
        EuroBond_ID = order_details[58]
        Chiron_ID = order_details[63]
        NewFrontiersID = order_details[64]
        RosemaryID = order_details[65]
        AsianAlphaID = order_details[67]
        HighFocusID = order_details[69]
        MeaOpportunitiesID = order_details[68]
        LevEuroID = order_details[72]
        Raffaello = order_details[75]
        Order_Stage2(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID,
                     ItalyLS_ID=ItalyLS_ID, EuropeanValue_ID=EuropenValue_ID, OrderStage="Suggestion Rejected",
                     EuroBond_ID=EuroBond_ID, Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID,
                     Rosemary_ID=RosemaryID, AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocusID,
                     MEAorder_ID=MeaOpportunitiesID, LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)
        conn.close()
        return "Order stage updated successfully..!"
    else:
        msg = "Apologies, only the Investment Manager can approve or reject a suggestion from an advisor"
        return msg


def OrdersCancel(order_id, Username):
    """
   This method cancel orders. To cancel an order update the order stage status as Suggestion cancelled.

       :param order_id: order id which need to be cancelled.
       :param UserId: user id who cancelled order
       :param Username: username who cancelled order

       :return: return success message as order Suggestion cancelled

    """
    conn = database_dev()
    cursor = conn.cursor()
    reject_query = (
            "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
            "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
        order_id) + "')")
    cursor.execute(reject_query)
    order_details = cursor.fetchone()

    update_order = ("UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET OrderStage = %s where ([ID] = '" + str(
        order_id) + "')")
    values = ('Cancelled')
    print(update_order, values)
    cursor.execute(update_order, values)
    conn.commit()
    NorthAmericaLS_ID = order_details[57]
    GreaterChinaLS_ID = order_details[56]
    ItalyLS_ID = order_details[55]
    EuropenValue_ID = order_details[53]
    EuroBond_ID = order_details[58]
    Chiron_ID = order_details[63]
    NewFrontiersID = order_details[64]
    RosemaryID = order_details[65]
    AsianAlphaID = order_details[67]
    HighFocusID = order_details[69]
    MeaOpportunitiesID = order_details[68]
    LevEuroID = order_details[72]
    Raffaello = order_details[75]
    OrderStage = "Suggestion cancelled"
    Order_Stage2(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID,
                 ItalyLS_ID=ItalyLS_ID, EuropeanValue_ID=EuropenValue_ID, OrderStage=OrderStage,
                 EuroBond_ID=EuroBond_ID, Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID,
                 Rosemary_ID=RosemaryID, AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocusID,
                 MEAorder_ID=MeaOpportunitiesID, LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)
    conn.close()
    return "Order stage updated successfully..!"


def Disable_connection(id_dis, UserId):
    """
    In this method connection get disable, while disabling the connection we are updating OrderStageOwl
    field value as null from OSUSR_38P_ORDERS.

    :param id_dis:  order id
    :param UserId: user id

    :return: updating OrderStageOwl as null and return success message.

    """
    try:
        conn = database_dev()
        cursor = conn.cursor()
        get_user_role = ("select * from [outsys_prod].DBO.[ossys_User_Role] where USER_ID = '" + str(UserId) + "' ")
        cursor.execute(get_user_role)
        user_rec = cursor.fetchall()
        if user_rec:
            OrderStageOwl = "NULL"
            update_order = (
                        "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET OrderStageOwl = %s where ([ID] = '" + str(
                    id_dis) + "')")
            values = (OrderStageOwl)
            print(update_order, values)
            cursor.execute(update_order, values)
            conn.commit()
            conn.close()
            return "records updated successfully..!"
    except Exception as e:
        print(str(e))
        raise


def OwlAPI(BBG_BrokerCode, Col1, Col2):
    try:
        API_Command = ""
        if BBG_BrokerCode != "":
            if Col1 == "C" or Col1 == "SW":
                if Col2 == "Equity" or Col2 == "Equity - REIT" or Col2 == "Equity - ADR" or \
                        Col2 == "Equity - Savings Share" or Col2 == "Equity - Right" or Col2 == "Equity - ETP" or \
                        Col2 == "Equity - MLP" or Col2 == "Equity B-Shares" or Col2 == "Equity A-Shares" or \
                        Col2 == "Equity Preference" or Col2 == "Option Equity" or Col2 == "Option Index" or \
                        Col2 == "EQ" or Col2 == "OP" or Col2 == "CFD" or Col2 == "FU" or Col2 == "Future Index" or \
                        Col2 == "Single stock future" or Col2 == "Financial commodity future." or Col2 == "GDR":
                    API_Command = "WaitingOwlEmsx"
                else:
                    if Col2 == "Bond":
                        API_Command = "WaitingOwlTSOX"
        return API_Command
    except Exception as e:
        print(str(e))
        raise


def Enable_connection(id_en, UserId):
    """
    In this method connection get enable, while enable the connection we are updating OrderStageOwl
    field value as WaitingOwlEmsx or WaitingOwlTSOX from OSUSR_38P_ORDERS.

    :param id_en: order id
    :param UserId: user id

    :return: updating OrderStageOwl WaitingOwlEmsx or WaitingOwlTSOX and return success message.

    """
    try:
        conn = database_dev()
        cursor = conn.cursor()
        get_user_role = ("select * from [outsys_prod].DBO.[ossys_User_Role] where USER_ID = '" + str(UserId) + "' ")
        cursor.execute(get_user_role)
        user_rec = cursor.fetchall()
        print(user_rec)
        GetOrderById = (
                    "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
                    "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + id_en + "') ORDER BY [ENORDERS].[SIDE] ASC ")
        cursor.execute(GetOrderById)
        GetRecOrderById = cursor.fetchall()
        print(GetRecOrderById)
        Broker, TransactionType, ProductType = '', '', ''
        for j, _ in enumerate(GetRecOrderById):
            rec = GetRecOrderById[j]
            Broker = rec[9]
            TransactionType = rec[48]
            ProductType = rec[3]

        API_Command = OwlAPI(BBG_BrokerCode=Broker, Col1=TransactionType, Col2=ProductType)
        if API_Command != "":
            OrderStageOwl = API_Command
            update_order = (
                    "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET OrderStageOwl = %s where ([ID] = '" + str(
                id_en) + "')")
            values = (OrderStageOwl)
            cursor.execute(update_order, values)
            conn.commit()
            conn.close()
        return "records updated successfully..!"
    except Exception as e:
        print(str(e))
        raise


def DeleteOrder(order_id, userId, Username):
    """
    This method deletes orders. While Deleting order entries get deleted from OSUSR_38P_ORDERS and OSUSR_SKP_ORDERCHANGES
    tables based on specified order id.

       :param order_id: order id which need to be deleted.
       :param UserId: user id who delete order
       :param Username: username who delete order

       :return: return success message as order deleted.

    """
    try:
        conn = database_dev()
        cursor = conn.cursor()
        get_user_role = ("select * from [outsys_prod].DBO.[ossys_User_Role] where USER_ID = '" + str(userId) + "' ")
        cursor.execute(get_user_role)
        user_rec = cursor.fetchall()

        reject_query = (
                "SELECT [ENORDERS].[ID] o0, [ENORDERS].[DATE] o1, [ENORDERS].[SIDE] o2, [ENORDERS].[PRODUCTTYPE] o3, [ENORDERS].[PRODUCTID] o4, [ENORDERS].[SETTLECCY] o5, [ENORDERS].[SETTLEDATE] o6, [ENORDERS].[ORDERTYPE] o7, [ENORDERS].[LIMIT] o8, [ENORDERS].[BROKER] o9, [ENORDERS].[EXPIRY] o10, [ENORDERS].[EXPIRYDATE] o11, [ENORDERS].[ROUTING] o12, [ENORDERS].[OPERATOR] o13, [ENORDERS].[FUNDCODE] o14, [ENORDERS].[FUND] o15, [ENORDERS].[CUSTODIAN] o16, [ENORDERS].[ACCOUNT] o17, [ENORDERS].[STRATEGY] o18, [ENORDERS].[BOOK] o19, [ENORDERS].[ORDERQTYTYPE] o20, [ENORDERS].[ORDERQTYVALUE] o21, [ENORDERS].[ORDERQTYVALUELONGLEG] o22, [ENORDERS].[USERCOMMENT] o23, [ENORDERS].[LIMITONVOLUME] o24, [ENORDERS].[CREATIONTIME] o25, [ENORDERS].[INVESTMENTMANAGER] o26, [ENORDERS].[APPROVED] o27, [ENORDERS].[APPROVALTIME] o28, [ENORDERS].[TRADER] o29, [ENORDERS].[SENTTOTRADINGDESKTIME] o30, [ENORDERS].[NATUREOFTHEORDER] o31, [ENORDERS].[COUNTERVALUE] o32, [ENORDERS].[SUGGESTEDBROKER] o33, [ENORDERS].[TICKERISIN] o34, [ENORDERS].[CHANGINGMODIFICATIONTIME] o35, [ENORDERS].[ACTUALWEIGHT] o36, [ENORDERS].[NEWTARGETWEIGHT] o37, [ENORDERS].[INSTRUCTIONS] o38, [ENORDERS].[FUNDNAME] o39, [ENORDERS].[TRADINGDESKCONFIRMATION] o40, [ENORDERS].[TRADINGDESKRECEPTIONTIME] o41, [ENORDERS].[BNRPRODUCTTYPE] o42, [ENORDERS].[BNRBROKER] o43, [ENORDERS].[BNRORDERPRECISEQUANTITY] o44, [ENORDERS].[FUNDNAMESHORT] o45, [ENORDERS].[STOCKNAME] o46, [ENORDERS].[INTRUMENTTYPE] o47, [ENORDERS].[TRANSACTIONTYPE] o48, [ENORDERS].[ORDERSTAGE] o49, [ENORDERS].[EXECUTIONPRICE] o50, [ENORDERS].[EXECUTIONPRICENET] o51, [ENORDERS].[ADVISOR] o52, [ENORDERS].[EUROPENVALUE_ID] o53, [ENORDERS].[BROKERID_CONTACTTAB] o54, [ENORDERS].[ITALYLS_ID] o55, [ENORDERS].[GREATERCHINALS_ID] o56, [ENORDERS].[NORTHAMERICALS_ID] o57, [ENORDERS].[EUROBOND_ID] o58, [ENORDERS].[CURRENCY_ID] o59, [ENORDERS].[EQUITY_ID] o60, [ENORDERS].[BOND_ID] o61, [ENORDERS].[DERIVATIVE_ID] o62, [ENORDERS].[CHIRON_ID] o63, [ENORDERS].[NEWFRONTIERSID] o64, [ENORDERS].[ROSEMARYID] o65, [ENORDERS].[GLOBALFLEXIBLEID] o66, [ENORDERS].[ASIANALPHAID] o67, [ENORDERS].[MEAOPPORTUNITIESID] o68, [ENORDERS].[HIGHFOCUSID] o69, [ENORDERS].[FUNDSID] o70, [ENORDERS].[ORDERSUPDATING] o71, [ENORDERS].[USERID] o72, [ENORDERS].[LEVEUROID] o73, [ENORDERS].[ASSIMOCOID] o74, [ENORDERS].[RAFFAELLO] o75, [ENORDERS].[MONTECUCCOLIID] o76, [ENORDERS].[IMOLAID] o77, [ENORDERS].[CASA4FUND_FUNDNAME] o78, [ENORDERS].[CURRENCY] o79, [ENORDERS].[CASA4FUNDSECURITYTYPE] o80, [ENORDERS].[EXECUTEDQUANTITY] o81, [ENORDERS].[PENDINGQUANTITY] o82, [ENORDERS].[ORDERFROMDAYBEFORE] o83, [ENORDERS].[REBALANCE] o84, [ENORDERS].[ISFROMYESTERDAY] o85, [ENORDERS].[LAST_PRICE] o86, [ENORDERS].[C4F_BROKERCODE] o87, [ENORDERS].[FUNDNAV] o88, [ENORDERS].[FUNDCURRENCY] o89, [ENORDERS].[STOCKCURRENCY] o90, [ENORDERS].[SETTLEMENTCURRENCY] o91, [ENORDERS].[URGENCY] o92, [ENORDERS].[COUNTERVALUEINFUNDCRNCY] o93, [ENORDERS].[FX_FUNDCRNCYVSFUNDCRNCY] o94, [ENORDERS].[COUNTERVALUEINLOCALCRNCY] o95, [ENORDERS].[TRADEQUANTITYCALCULATED] o96, [ENORDERS].[TRADEQUANTITYCALCULATEDROUND] o97, [ENORDERS].[BBGSECURITYNAME] o98, [ENORDERS].[BBGEXCHANGE] o99, [ENORDERS].[ORDERCLOSE] o100, [ENORDERS].[PRECISEINSTRUCTIONS] o101, [ENORDERS].[ISIN] o102, [ENORDERS].[COUNTRY] o103, [ENORDERS].[ORDERSTAGEOWL] o104, [ENORDERS].[APICORRELATIONID] o105, [ENORDERS].[APIORDERREFID] o106, [ENORDERS].[SETTLEMENTDATE] o107, [ENORDERS].[BBGMESS1] o108, [ENORDERS].[BBGSETTLEDATE] o109, [ENORDERS].[BBGEMSXSTATUS] o110, [ENORDERS].[BBGEMSXSEQUENCE] o111, [ENORDERS].[BBGEMSXROUTEID] o112, [ENORDERS].[BBGCOUNTRYISO] o113, [ENORDERS].[BROKERBPS] o114, [ENORDERS].[BROKERCENTPERSHARE] o115, [ENORDERS].[TRADINGCOMMISSIONSBPS] o116, [ENORDERS].[TRADINGCOMMISSIONSCENT] o117, [ENORDERS].[BBG_OPTCONTSIZE] o118, [ENORDERS].[BBG_FUTCONTSIZE] o119, [ENORDERS].[BBG_IDPARENTCO] o120, [ENORDERS].[BBG_LOTSIZE] o121, [ENORDERS].[BBG_MARKETOPENINGTIME] o122, [ENORDERS].[BBG_MARKETCLOSINGTIME] o123, [ENORDERS].[BBG_PRICEINVALID] o124, [ENORDERS].[BBG_OPT_UNDL_PX] o125, [ENORDERS].[EXPOSURECALCULATIONMETHOD] o126, [ENORDERS].[POTENTIALERROR] o127, [ENORDERS].[RMSTATUS] o128, [ENORDERS].[ACTUALQUANTITY] o129, [ENORDERS].[RM1] o130, [ENORDERS].[RM2] o131, [ENORDERS].[RM3] o132, [ENORDERS].[RM4] o133, [ENORDERS].[RM5] o134, [ENORDERS].[RM6] o135, [ENORDERS].[RM7] o136, [ENORDERS].[RM8] o137, [ENORDERS].[RM9] o138, [ENORDERS].[RM10] o139, [ENORDERS].[BBG_PARENTCOID] o140, [ENORDERS].[BBG_PARENTCONAME] o141, [ENORDERS].[TRADERNOTES] o142, [ENORDERS].[PORTFOLIOUPDATED] o143, [ENORDERS].[UPDALREADYADDED] o144, [ENORDERS].[UPDALREADYSUBTRACTED] o145, [ENORDERS].[BROKERSELMETHOD] o146, [ENORDERS].[BROKERSELREASON] o147, [ENORDERS].[EXECUTORFACTOR_COST] o148, [ENORDERS].[EXECUTORFACTOR_SPEED] o149, [ENORDERS].[EXECUTORFACTOR_LIKELIHOOD] o150, [ENORDERS].[EXECUTORFACTOR_SETTLEMENT] o151, [ENORDERS].[EXECUTORFACTOR_ORDERSIZE] o152, [ENORDERS].[EXECUTORFACTOR_NATURE] o153, [ENORDERS].[EXECUTORFACTOR_VENUE] o154, [ENORDERS].[EXECUTORFACTOR_CONSIDERATION] o155, [ENORDERS].[NEEDCOMMENT] o156, [ENORDERS].[FX_BASKETRUNID] o157, [ENORDERS].[FIX_CONF] o158, [ENORDERS].[FIX_CIORDID] o159, [ENORDERS].[FIX_EXECUTIONID] o160, [ENORDERS].[FIX_AVGPX] o161, [ENORDERS].[FIX_FAR_AVGPX] o162, [ENORDERS].[FIX_LASTQTY] o163, [ENORDERS].[FIX_FAR_LASTQTY] o164, [ENORDERS].[FIX_LEAVESQTY] o165, [ENORDERS].[OUTRIGHTORSWAP] o166, [ENORDERS].[CURRENCY_2] o167, [ENORDERS].[JPMORGANACCOUNT] o168, [ENORDERS].[BROKERCODEAUTO] o169, [ENORDERS].[EXPOSURETRADEID] o170, [ENORDERS].[BROKERHASBEENCHANGED] o171, [ENORDERS].[UBSACCOUNT] o172, [ENORDERS].[RISKMANAGEMENTRESULT] o173, [ENORDERS].[ARRIVALPRICE] o174, [ENORDERS].[ADV20D] o175, [ENORDERS].[MEAMULTIPLIER] o176, [ENORDERS].[LEVEUROSTATEGYID] o177, [ENORDERS].[ISREPO] o178, [ENORDERS].[REPOEPIRYDATE] o179, [ENORDERS].[REPO_CODEDOSSIER] o180, [ENORDERS].[REPO_VALEURTAUX] o181, [ENORDERS].[REPO_BICSENDER] o182, [ENORDERS].[REPO_CODECONTREPARTIE] o183, [ENORDERS].[REPO_COMPARTIMENT] o184, [ENORDERS].[REPO_EXPRESSIONQUANTITESJ] o185, [ENORDERS].[REPO_NOMTAUX] o186, [ENORDERS].[REPO_REFERENCEEXTERNE] o187, [ENORDERS].[REPO_BASECALCULINTERET] o188, [ENORDERS].[REPO_TERMDATE] o189, [ENORDERS].[REPO_HAIRCUT] o190, [ENORDERS].[REPO_INTEREST_RATE] o191, [ENORDERS].[LEVEUROSETTLEDATE] o192, [ENORDERS].[REPO_2RDLEG_PRICE] o193, [ENORDERS].[REPO_BROKERLOCALCUSTODIABIC] o194, [ENORDERS].[REPO_BROKERBENIFICIARYBIC] o195, [ENORDERS].[LEIREPORTINGCODE] o196, [ENORDERS].[BROKERCODE] o197, [ENORDERS].[MASTERAGREEMENT] o198, [ENORDERS].[MASTERAGREEMENTVERSION_DATE] o199, [ENORDERS].[REPO_SFTR] o200, [ENORDERS].[REPO_REAL] o201, [ENORDERS].[APPROVALDATETIMEMILLI] o202, [ENORDERS].[REPO_UTI] o203 "
                "FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] [ENORDERS] WHERE ([ENORDERS].[ID] = '" + str(
            order_id) + "')")
        cursor.execute(reject_query)
        order_details = cursor.fetchone()

        NorthAmericaLS_ID = order_details[57]
        GreaterChinaLS_ID = order_details[56]
        ItalyLS_ID = order_details[55]
        EuropenValue_ID = order_details[53]
        EuroBond_ID = order_details[58]
        Chiron_ID = order_details[63]
        NewFrontiersID = order_details[64]
        RosemaryID = order_details[65]
        AsianAlphaID = order_details[67]
        HighFocusID = order_details[69]
        MeaOpportunitiesID = order_details[68]
        LevEuroID = order_details[72]
        Raffaello = order_details[75]
        OrderStage = ""
        Order_Stage2(NorthAmericaLS_ID=NorthAmericaLS_ID, GreaterChinaLS_ID=GreaterChinaLS_ID,
                     ItalyLS_ID=ItalyLS_ID, EuropeanValue_ID=EuropenValue_ID, OrderStage=OrderStage,
                     EuroBond_ID=EuroBond_ID, Chiron_ID=Chiron_ID, NewFrontiersID=NewFrontiersID,
                     Rosemary_ID=RosemaryID, AsianAlpha_ID=AsianAlphaID, HighFocus_ID=HighFocusID,
                     MEAorder_ID=MeaOpportunitiesID, LevEuroID=LevEuroID, RaffaelloID=Raffaello, Username=Username)

        if user_rec:
            del_order_changes_tbl = "DELETE FROM [outsys_prod].DBO.[OSUSR_SKP_ORDERCHANGES]" \
                                    "WHERE ([ORDERSID] = '" + str(
                order_id) + "') "
            cursor.execute(del_order_changes_tbl)
            conn.commit()

            del_order_tbl = "DELETE FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS]" \
                            " WHERE ([ID] = '" + str(order_id) + "')"
            cursor.execute(del_order_tbl)
            conn.commit()

            conn.close()
            return "Order deleted successfully..!"
    except Exception as e:
        print(str(e))
        raise


def Broker_sel_method(BrokerSelMethod, BrokerSelReason, ExecutorFactor_Consideration, ExecutorFactor_Cost,
                      ExecutorFactor_Likelihood, ExecutorFactor_OrderSize, ExecutorFactor_Settlement,
                      ExecutorFactor_Speed, ExecutorFactor_Venue,ExecutorFactor_Nature,order_id):
    """

    In this method, orders get updated based on the below parameters. As well as in this method validation handle for
    below fields

    :param BrokerSelMethod: Broker Sell Method A,B and C
    :param BrokerSelReason: Broker Sell Reason
    :param ExecutorFactor_Consideration: ExecutorFactor Consideration
    :param ExecutorFactor_Cost: ExecutorFactor Cost
    :param ExecutorFactor_Likelihood: ExecutorFactor Likelihood
    :param ExecutorFactor_OrderSize: ExecutorFactor OrderSize
    :param ExecutorFactor_Settlement: ExecutorFactor Settlement
    :param ExecutorFactor_Speed: ExecutorFactor Speed
    :param ExecutorFactor_Venue: ExecutorFactor Venue
    :param ExecutorFactor_Nature: ExecutorFactor Nature
    :param order_id: order id

    :return:

    """
    conn = database_dev()
    cursor = conn.cursor()
    try:
        if BrokerSelMethod == '':
            return "BrokerSelMethod should not be empty"
        elif BrokerSelReason == 0:
            return "BrokerSelReason should not be empty"
        elif ExecutorFactor_Consideration == "":
            return "ExecutorFactor_Consideration should not be empty"
        elif ExecutorFactor_Cost == "" or ExecutorFactor_Cost == 0:
            return "ExecutorFactor_Cost should not be empty"
        elif ExecutorFactor_Likelihood == "":
            return "ExecutorFactor_Likelihood should not be empty"
        elif ExecutorFactor_Nature == 0:
            return "ExecutorFactor_Nature should not be empty"
        elif ExecutorFactor_OrderSize == 0:
            return "ExecutorFactor_OrderSize should not be empty"
        elif ExecutorFactor_Settlement == 0:
            return "ExecutorFactor_Settlement should not be empty"
        elif ExecutorFactor_Speed == 0:
            return "ExecutorFactor_Speed should not be empty"
        elif ExecutorFactor_Venue == 0:
            return "ExecutorFactor_Venue should not be empty"
        else:
            update_order = (
                    "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET BrokerSelMethod = %s,"
                    "BrokerSelReason=%s,ExecutorFactor_Consideration=%s,ExecutorFactor_Cost=%s,"
                    "ExecutorFactor_Likelihood=%s, ExecutorFactor_Nature= %s,ExecutorFactor_OrderSize=%s,"
                    "ExecutorFactor_Settlement=%s,ExecutorFactor_Speed= %s,ExecutorFactor_Venue= %s"
                    "where ([ID] = '" + str(order_id) + "')")
            values = (BrokerSelMethod, BrokerSelReason, ExecutorFactor_Consideration, ExecutorFactor_Cost,
                      ExecutorFactor_Likelihood, ExecutorFactor_Nature, ExecutorFactor_OrderSize,
                      ExecutorFactor_Settlement, ExecutorFactor_Speed, ExecutorFactor_Venue)
            cursor.execute(update_order, values)
            conn.commit()
            conn.close()
            return "records updated successfully..!"
    except Exception as e:
        print(str(e))
        raise


def save_insert_repo_info(OrderID, Date, ExecutionPrice, LeiReportingCode, PendingQuantity, BrokerCode, MasterAgreement,
                          MasterAgreementVersion_date, REPO_CodeDossier, REPO_ValeurTaux, REPO_Compartiment,
                          REPO_ExpressionQuantiteSJ, REPO_NomTaux, REPO_ReferenceExterne, REPO_BaseCalculInteret,
                          REPO_TermDate, LevEuroSettleDate, REPO_UTI, REPO_Haircut, REPO_Interest_rate,
                          REPO_2rdleg_Price, REPO_BrokerLocalCustodiaBIC, REPO_BrokerBenificiaryBIC, REPO_BicSender,ExecutedQuantity):
    try:
        conn = database_dev()
        cursor = conn.cursor()
        update_order = (
                "UPDATE [outsys_prod].DBO.[OSUSR_38P_ORDERS] SET Date = %s,ExecutionPrice= %s,"
                "LeiReportingCode=%s,PendingQuantity=%s,BrokerCode=%s,MasterAgreement=%s,"
                "MasterAgreementVersion_date=%s,REPO_CodeDossier=%s,REPO_ValeurTaux=%s,REPO_Compartiment=%s,"
                "REPO_ExpressionQuantiteSJ=%s,REPO_NomTaux=%s,REPO_ReferenceExterne=%s,REPO_BaseCalculInteret=%s,"
                "REPO_TermDate=%s,LevEuroSettleDate=%s,REPO_UTI=%s,REPO_Haircut=%s,REPO_Interest_rate=%s,"
                "REPO_2rdleg_Price=%s,REPO_BrokerLocalCustodiaBIC=%s,REPO_BrokerBenificiaryBIC=%s,"
                "REPO_BicSender=%s,ExecutedQuantity=%s where ([ID] = '" + str(OrderID) + "')")
        values = (Date, ExecutionPrice, LeiReportingCode, PendingQuantity, BrokerCode, MasterAgreement,
                  MasterAgreementVersion_date, REPO_CodeDossier, REPO_ValeurTaux, REPO_Compartiment,
                  REPO_ExpressionQuantiteSJ, REPO_NomTaux, REPO_ReferenceExterne, REPO_BaseCalculInteret,
                  REPO_TermDate, LevEuroSettleDate, REPO_UTI, REPO_Haircut, REPO_Interest_rate,
                  REPO_2rdleg_Price, REPO_BrokerLocalCustodiaBIC, REPO_BrokerBenificiaryBIC, REPO_BicSender,ExecutedQuantity)
        cursor.execute(update_order, values)
        conn.commit()
        conn.close()
        return "Record updated"
    except Exception as e:
        print(str(e))
        raise


def PopoluateInfoBroker(OrderID):
    try:
        conn = database_dev()
        cursor = conn.cursor()
        select_query = ("SELECT BrokerID_contactTAB FROM [outsys_prod].DBO.[OSUSR_38P_ORDERS] "
                        "WHERE ([ID] =  '" + str(OrderID) + "') ORDER BY [SIDE] ASC ")
        cursor.execute(select_query)
        FromOrdersGetBrokerID_rec = cursor.fetchone()
        BrokerId = FromOrdersGetBrokerID_rec
        print("BrokerId", BrokerId)
        BrokerId = BrokerId[0] if BrokerId else 'NULL'

        select_broker = ("SELECT  REPOBrokerLocalCustodiaCode,REPOBeneficaryBIC FROM [outsys_prod].DBO."
                         "[OSUSR_38P_BROKER] WHERE ([ID] = '" + str(BrokerId) + "') ORDER BY [NAME] ASC ")
        cursor.execute(select_broker)
        FromOrdersGetBrokerID_rec = cursor.fetchone()
        REPOBrokerLocalCustodiaCode = FromOrdersGetBrokerID_rec[0] if FromOrdersGetBrokerID_rec else ''
        REPOBeneficaryBIC = FromOrdersGetBrokerID_rec[1] if FromOrdersGetBrokerID_rec else ''
        print(FromOrdersGetBrokerID_rec)
        context = {"REPOBrokerLocalCustodiaCode": REPOBrokerLocalCustodiaCode, "REPOBeneficaryBIC": REPOBeneficaryBIC}
        return context
    except Exception as e:
        print(str(e))
        raise
