# utils.py - Utility functions

import hashlib

def get_id(chatID, obj):
  # id = hashlib.md5(str(chatID)+str(obj).encode('utf-8')).hexdigest()
  id = hashlib.md5((str(chatID) + str(obj)).encode('utf-8')).hexdigest()
  return id


def is_valid_number_or_percentage(value):
    try:
        # Attempt to convert the value to a float
        float_value = float(value)
        return True
    except ValueError:
        # The value is not a valid number, check if it's a valid percentage
        if value.endswith('%'):
            # Remove the percentage sign and attempt to convert to a float
            try:
                float_value = float(value[:-1])
                return True
            except ValueError:
                return False
        else:
            return False

def human_format_seconds(seconds):
    units = [(30 * 24 * 60 * 60, "month"), (7 * 24 * 60 * 60, "week"), (24 * 60 * 60, "day"), (60 * 60, "hour"), (60, "minute"), (1, "second")]

    result = ""
    for divider, unit in units:
        quantity = seconds // divider
        seconds %= divider

        # Check if the remainder is within 10% of the current unit
        if seconds >= 0.9 * divider:
            quantity += 1
            seconds = 0

        # Check if the quantity is half of the current unit
        if quantity == 0.5:
            result += f"half a {unit} "
            continue

        # Check if the quantity is about half of the current unit
        if 0.4 <= quantity < 0.6:
            result += f"about half a {unit} "
            continue

        # Add to the result string
        if quantity > 0:
            result += f"{'about ' if 0.9 <= quantity < 1 else ''}{int(quantity)} {unit if quantity == 1 else unit + 's'} "

    return result.strip()

def parse_name_from_command(command_text):
    """
    Parse name from command text using 'called "name"' syntax.
    Returns tuple: (command_without_name, name or None)
    
    Example:
        '/higher btc 86993.1 called "breakout target"' -> 
        ('/higher btc 86993.1', 'breakout target')
    """
    import re
    
    # Match 'called "..."' at the end of the command
    # This pattern allows for both single and double quotes
    pattern = r'\s+called\s+["\']([^"\']+)["\']\s*$'
    match = re.search(pattern, command_text, re.IGNORECASE)
    
    if match:
        name = match.group(1)
        command_without_name = command_text[:match.start()]
        return (command_without_name, name)
    
    return (command_text, None)

