# utils.py - Utility functions

import hashlib

def get_id(chatID, obj):
  # id = hashlib.md5(str(chatID)+str(obj).encode('utf-8')).hexdigest()
  id = hashlib.md5((str(chatID) + str(obj)).encode('utf-8')).hexdigest()
  return id

import math

SECONDS_IN_MINUTE = 60  
SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE
SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR
SECONDS_IN_WEEK = 7 * SECONDS_IN_DAY
SECONDS_IN_MONTH = 30 * SECONDS_IN_DAY

ROUNDING_THRESHOLD = 0.1

units = [
    ("seconds", SECONDS_IN_MINUTE),
    ("minutes", SECONDS_IN_HOUR), 
    ("hours", SECONDS_IN_DAY),
    ("days", SECONDS_IN_WEEK),
    ("weeks", SECONDS_IN_MONTH)
]

def format_duration(seconds):
    for name, length in units:
        unit = seconds // length
        remainder = seconds % length
        
        if unit < 1:
            continue
            
        if remainder > length * ROUNDING_THRESHOLD:
            unit += 1
            
        if remainder > length / 2:
            return "{:.1f} and a half {}".format(unit, name)
            
        if unit < 2:
            return "about {:.1f} {}".format(unit, name)

    return "{} seconds".format(seconds)