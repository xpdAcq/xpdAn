import os
import datetime

def _clean_info(input_str):
    return input_str.strip().replace(' ', '_')

def _timestampstr(timestamp):
    ''' convert timestamp to strftime formate '''
    timestring = datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y%m%d-%H%M')
    return timestring
