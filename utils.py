# utils.py - Utility functions

import hashlib

def get_id(chatID, obj):
  id = hashlib.md5(str(chatID)+str(obj).encode('utf-8')).hexdigest()
  return id
