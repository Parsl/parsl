import logging

logger = logging.getLogger(__name__)


def wtime_to_minutes(time_string):
    ''' wtime_to_minutes

    Convert standard wallclock time string to minutes.

    Args:
        - Time_string in HH:MM:SS format

    Returns:
        (int) minutes

    '''
    hours, mins, seconds = time_string.split(':')
    return int(hours) * 60 + int(mins) + 1
