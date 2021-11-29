import datetime
import pytz


def to_list(x):
    """
    To list
    """

    __list__ = x.copy() if type(x) is list else [x]

    return __list__


def datetime_mx(dt, timezone="America/Mexico_City") -> datetime:
    """
    Datetime with timezone
    """

    dt_tz = dt.astimezone(pytz.timezone(timezone))
    
    return dt_tz


def str_sys_datetime() -> str:
    """
    System datetime
    """

    mx_dt = datetime.datetime.now()
    mx_dt = datetime_mx(mx_dt)
    str_dt = mx_dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    return str_dt


def logging(name=None, level="INFO", message=None, file=None) -> None:
    """
    Print message
    """

    __message__ = "[%s]" % str_sys_datetime()

    if level is not None:
        __message__ += " - [%s]" % level
    
    if name is not None:
        _name = to_list(name)
        __name__ = ".".join(_name)
        __message__ += " - [%s]" %  __name__
    
    if message is not None:
        __message__ += " :: %s" % message
    
    print(__message__)

    if file is not None:
        file.write(__message__)
