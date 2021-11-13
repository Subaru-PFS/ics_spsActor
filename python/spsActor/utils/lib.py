import time
from datetime import datetime as dt


def fromisoformat(date, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    return dt.strptime(date, fmt)


def wait(ti=0.001):
    time.sleep(ti)
