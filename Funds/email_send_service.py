from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import sys


def sendemail(subject,title):
    """

    In this function, email send with context message of various process.

    :param subject: subject
    :param title:  title
    :return:  Email send with context.

    """
    Destinatari = 'ashutosh.pagare@banorcapital.com'  # 'oliver.silvey@banorcapital.com' # 'wenyan.hao@banorcapital.com' # 'dario.giannini@banor.it, roberto.bianchi@banor.it'   #   'oliver.silvey@banorcapital.com' #

    # recipients = ["andrea.geninatti@banor.it", "simone.cavallarin@banorcapital.com"]  'wenyan.hao@banorcapital.com' #

    cc = 'ashutosh.pagare@banorcapital.com'  # 'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com, ashutosh.pagare@banorcapital.com' #  'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #   'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #  'wenyan.hao@banorcapital.com' #  'giacomo.mergoni@banorcapital.com, james.george@banorcapital.com, banorsicav@banorcapital.com'  #  'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #

    recipients = cc.split(",") + Destinatari.split(",")
    to = Destinatari
    Subj = subject
    emsg = ''
    print('str: ', emsg)
    Txt = (emsg)
    Sender = 'bps@banorcapital.com'
    hostsys = 'relay.banorcapital.com'
    email_title = title if title else None
    text = Txt  # build_table(gdp_data, 'blue_light', font_size = '10px', text_align = 'right')
    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Cc'] = cc
    outer['Date'] = ''  # formatdate(localtime=True)
    outer['Subject'] = Subj
    outer['Title'] = email_title
    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    var = Server.ehlo
    print(var)

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()


# subject = "La determinazione del broker Equity cash non ha funzionato"
# title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
# sendemail(subject,title)


def Email_Send_ErrorAlarm(subject, title):
    """
    Send email while error occurred.
    :param subject: subject
    :param title: title
    :return: email send with error context.

    """
    Destinatari = 'ashutosh.pagare@banorcapital.com'
    cc = 'ashutosh.pagare@banorcapital.com'
    recipients = cc.split(",") + Destinatari.split(",")
    to = Destinatari
    Subj = subject
    emsg = ''
    print('str: ', emsg)
    Txt = (emsg)
    Sender = 'bps@banorcapital.com'
    hostsys = 'relay.banorcapital.com'
    email_title = title if title else None

    text = Txt  # build_table(gdp_data, 'blue_light', font_size = '10px', text_align = 'right')

    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Cc'] = cc
    outer['Date'] = ''  # formatdate(localtime=True)
    outer['Subject'] = Subj
    # outer['Title'] = email_title
    outer['Fund'] = "North America"
    # outer['body'] = non funziona

    # Txt = text

    body = MIMEText(Txt, "html")  # convert the body to a MIME compatible string
    body = MIMEText(Txt)  # convert the body to a MIME compatible string
    outer.attach(body)  # attach it to your main message

    # outer.preamble = 'You will not see this in a MIME-aware mail reader.'

    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    var = Server.ehlo
    print(var)

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()


# subject = "La determinazione del broker Equity cash non ha funzionato"
# title = "Cerca di risolvere andando ad editare la selezione del Broker Cash"
# sendemail(subject,title)

def Emails_Send(subject):
    """

    :param subject:
    :return:  send  email
    """
    Destinatari = "simone.cavallarin@banorcapital.com"
    cc = 'ashutosh.pagare@banorcapital.com'  # 'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com, ashutosh.pagare@banorcapital.com' #  'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #   'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #  'wenyan.hao@banorcapital.com' #  'giacomo.mergoni@banorcapital.com, james.george@banorcapital.com, banorsicav@banorcapital.com'  #  'wenyan.hao@banorcapital.com, simone.cavallarin@banorcapital.com' #

    recipients = cc.split(",") + Destinatari.split(",")
    to = Destinatari
    Subj = subject
    emsg = ''
    print('str: ', emsg)
    Sender = 'bps@banorcapital.com'
    hostsys = 'relay.banorcapital.com'
    email_title = "Not able to determinate correct fund"

    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Cc'] = cc
    outer['Date'] = ''  # formatdate(localtime=True)
    outer['Subject'] = Subj
    outer['Title'] = email_title

    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    var = Server.ehlo
    print(var)

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()


def Email_Send_Advisory(IsLogContent, FundName, To,BuySell, Ticker, StockName, PreviousWeight, NewWeight, IntrumentType,
                        Message, Urgency, SuggestionGenerated, PairTrade, QuantityIndicated):
    """

    :param IsLogContent:  IsLogContent
    :param FundName: Fund name
    :param To:  receiver
    :param BuySell:  BuySell
    :param Ticker:  TickerISIN
    :param StockName:  Stock name
    :param PreviousWeight: PreviousWeight
    :param NewWeight:  NewWeight
    :param IntrumentType: IntrumentType
    :param Message:  Message
    :param Urgency: Urgency
    :param SuggestionGenerated:  SuggestionGenerated
    :param PairTrade:  PairTrade
    :param QuantityIndicated:  QuantityIndicated
    :return: Send mail with Advisory

    """
    To = To
    cc = "davide.verardi@banor.it; Tomaso.Mariotti@banor.it; nicolo.digiacomo@banor.it"
    recipients = cc.split(";") + To.split(";")
    to = To
    Subj = "Sys : Advisory"
    Sender = "sys@banorcapital.com"
    hostsys = 'relay.banorcapital.com'

    emsg = f"""<html> <head></head><body><br>
    
       <table cellpadding="0" cellspacing="0" border="0">
       <tr><td>IsLogContent:</td><td>{IsLogContent}</td></tr>
       <tr><td>FundName:</td><td>{FundName}</td></tr>
       <tr><td>BuySell:</td><td>{BuySell}</td></tr>
       <tr><td>Ticker:</td><td>{Ticker}</td></tr>
       <tr><td>StockName:</td><td>{StockName}</td></tr>
       <tr><td>PreviousWeight:</td><td>{PreviousWeight}</td></tr>
       <tr><td>NewWeight:</td><td>{NewWeight}</td></tr>
       <tr><td>IntrumentType:</td><td>{IntrumentType}</td></tr>
       <tr><td>Message:</td><td>{Message}</td></tr>
       <tr><td>Urgency:</td><td>{Urgency}</td></tr>
       <tr><td>SuggestionGenerated:</td><td>{SuggestionGenerated}</td></tr>
       <tr><td>PairTrade:</td><td>{PairTrade}</td></tr>
       <tr><td>QuantityIndicated:</td><td>{QuantityIndicated}</td></tr>
       </table></body></html>"""

    print('str: ', emsg)

    Txt = (emsg)
    text = Txt  # build_table(gdp_data, 'blue_light', font_size = '10px', text_align = 'right')

    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Cc'] = cc
    outer['Date'] = ''  # formatdate(localtime=True)
    outer['Subject'] = Subj
    # outer['body'] = non funziona

    Txt = text

    body = MIMEText(Txt, 'html')  # convert the body to a MIME compatible string
    outer.attach(body)  # attach it to your main message
    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    Server.ehlo

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()


def Email_Send_Advisory_Medata(fetch_order_rec,To,cc):
    """

    :param fetch_order_rec:  records of advisory
    :param To: receiver
    :param cc:
    :return: send mail as advisory metadata
    """
    To = To
    # cc = "davide.verardi@banor.it; Tomaso.Mariotti@banor.it; nicolo.digiacomo@banor.it"
    cc =cc
    recipients = cc.split(";") + To.split(";")
    to = To
    Subj = "Sys : Advisory - Suggestion has been modified"
    Sender = "sys@banorcapital.com"
    hostsys = 'relay.banorcapital.com'

    for i, _ in enumerate(fetch_order_rec):
        ordrec = fetch_order_rec[i]
        FundName = ordrec[45]
        Buysell = ordrec[2]
        Ticker = ordrec[4]
        StockName = ordrec[46]
        Quantity = 0 if ordrec[21] == "" else ordrec[21]
        NewWeight = ordrec[35]
        IntrumentType = ordrec[47]
        Urgency = ordrec[90]
        message = "Change of existing suggestion"
        suggestionGenerated = ordrec[25]
        Instructions = ordrec[38]
        QuantityIndicated = ordrec[21]
        comment = ordrec[23]
        Limit = ordrec[8]
        Broker = ordrec[43]
        BrokerShortCode = ordrec[9]
        orderId = ordrec[0]
        bbgSecurityName = ordrec[96]
        ExposureType = ordrec[124]
        bbg_Opt_Undl_px = ordrec[123]
        bbgExchange = ordrec[97]
        Expiry = ordrec[10]
        IsRepoLevEuro = False
        TradingCommissionsCENT = ordrec[115]
        suggestionType = ordrec[20]

    emsg = f"""<html> <head></head><body><br>

       <table cellpadding="0" cellspacing="0" border="0">
       <tr><td>FundName:</td><td>{FundName}</td></tr>
       <tr><td>Buysell:</td><td>{Buysell}</td></tr>
       <tr><td>Ticker:</td><td>{Ticker}</td></tr>
       <tr><td>StockName:</td><td>{StockName}</td></tr>
       <tr><td>Quantity:</td><td>{Quantity}</td></tr>
       <tr><td>NewWeight:</td><td>{NewWeight}</td></tr>
       <tr><td>IntrumentType:</td><td>{IntrumentType}</td></tr>
       <tr><td>Urgency:</td><td>{Urgency}</td></tr>
       <tr><td>message:</td><td>{message}</td></tr>
       <tr><td>suggestionGenerated:</td><td>{suggestionGenerated}</td></tr>
       <tr><td>Instructions:</td><td>{Instructions}</td></tr>
       <tr><td>QuantityIndicated:</td><td>{QuantityIndicated}</td></tr>
       <tr><td>comment:</td><td>{comment}</td></tr>
       <tr><td>Limit:</td><td>{Limit}</td></tr>
       <tr><td>Broker:</td><td>{Broker}</td></tr>
       <tr><td>BrokerShortCode:</td><td>{BrokerShortCode}</td></tr>
       <tr><td>bbgSecurityName:</td><td>{bbgSecurityName}</td></tr>
       <tr><td>ExposureType:</td><td>{ExposureType}</td></tr>
       <tr><td>bbg_Opt_Undl_px:</td><td>{bbg_Opt_Undl_px}</td></tr>
       <tr><td>bbgExchange:</td><td>{bbgExchange}</td></tr>
       <tr><td>Expiry:</td><td>{Expiry}</td></tr>
       <tr><td>IsRepoLevEuro:</td><td>{IsRepoLevEuro}</td></tr>
       <tr><td>TradingCommissionsCENT:</td><td>{TradingCommissionsCENT}</td></tr>
       <tr><td>suggestionType:</td><td>{suggestionType}</td></tr>
       </table></body></html>"""

    # print('str: ', emsg)

    Txt = (emsg)
    text = Txt  # build_table(gdp_data, 'blue_light', font_size = '10px', text_align = 'right')

    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Cc'] = cc
    outer['Date'] = ''  # formatdate(localtime=True)
    outer['Subject'] = Subj
    # outer['body'] = non funziona

    Txt = text

    # body = MIMEText(Txt, "html") # convert the body to a MIME compatible string
    body = MIMEText(Txt, 'html')  # convert the body to a MIME compatible string
    outer.attach(body)  # attach it to your main message
    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    # Server.ehlo

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()


def Email_Send_ServiceEmail(Username):
    """

    :param subject:
    :return:  send  email
    """
    Destinatari = "james.grant@banorcapital.com; james.george@banorcapital.com; simone.cavallarin@banorcapital.com; wenyan.hao@banorcapital.com; david.howells@banorcapital.com"
    to = Destinatari
    Subj = "sys: Advisory NEW FX HAS BEEN INSERTED"
    Sender = "sys@banorcapital.com"
    hostsys = 'relay.banorcapital.com'
    email_title = "Fx suggestion inserted by: " + str(Username)
    recipients = to

    outer = MIMEMultipart()
    outer['From'] = Sender
    outer['To'] = to
    outer['Subject'] = Subj
    outer['Title'] = email_title

    try:
        print("done")
    except:
        print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
        raise

    composed = outer.as_string()
    Server = smtplib.SMTP(hostsys, 25)
    Server.ehlo()
    Server.starttls()
    var = Server.ehlo
    print(var)

    try:
        Server.login('sys', psw)
        print('logged in!')
    except:
        print("Unable to log in. Error: ", sys.exc_info()[0])

    try:
        Server.sendmail(Sender, recipients, outer.as_string())
        print('email sent...')
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])

    Server.quit()

