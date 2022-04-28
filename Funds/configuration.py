import os
import pymssql
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def database_dev():
    """
    This function called for databased connectivity.
    :return:  database connection object.
    """
    conn = pymssql.connect('172.16.5.7', 'aress_pyt', 'Aress2021ss.', "outsys_prod")
    return conn


def get_current_date():
    """
    Datetime to date convertor into %Y-%m-%d format
    :return: date in '%Y-%m-%d' format
    """
    now = datetime.now()
    CurDate = now.strftime('%Y-%m-%d')
    return CurDate


def get_current_time():
    """
    This function pickup time from datetime into "%H:%M:%S" format
    :return: return time "%H:%M:%S" format
    """
    from datetime import datetime
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time


def get_current_date_time():
    """
    convart datetime object into string %Y-%m-%d %H:%M:%S format
    :return: return datetime in %Y-%m-%d %H:%M:%S format
    """
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    return dt_string


def log_configurations():
    """
    Get current path of directory
    :return: current path of directory
    """
    os.getcwd()
    directory = os.getcwd()
    file_path = directory + "\Funds_logs_1.txt"
    return file_path
