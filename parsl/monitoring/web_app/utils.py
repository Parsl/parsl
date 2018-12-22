from datetime import datetime


DB_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

RUN_ID_REGEX = r'([a-z]|[0-9]){8}-([a-z]|[0-9]){4}-([a-z]|[0-9]){4}-([a-z]|[0-9]){4}-([a-z]|[0-9]){12}'


def timestamp_to_float(time, format=DB_DATE_FORMAT):
    return datetime.strptime(time, format).timestamp()


def timestamp_to_int(time):
    return int(timestamp_to_float(time))


def num_to_timestamp(n):
    return datetime.fromtimestamp(n)
