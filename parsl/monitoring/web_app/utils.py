from datetime import datetime


DB_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def timestamp_to_float(time, format=DB_DATE_FORMAT):
    return datetime.strptime(time, format).timestamp()


def timestamp_to_int(time):
    return int(timestamp_to_float(time))


def num_to_timestamp(n):
    return datetime.fromtimestamp(n)
