import hashlib
import traceback
import math, time, requests, pickle, traceback, sys, os
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logger_config
import config
from cache import cache
from repository.market import MarketRepository
from formating import format_price
from api.binance_rest import CandleInterval
from command_handler import CommandHandler
from tg_api import TgApi

class TgBotService(object):
    def processMessage(self, message):
        if "text" not in message:
            self.log.debug("IGNORING MSG [NO TEXT]")
            return
        if('entities' in message and message['entities'][0]['type'] == 'bot_command'):
            self.command_handler.dispatch(message)
        else:
            self.log.debug("IGNORING MSG [NON-COMMAND]")


    def removeAlert(self, fsym, tsym, target, chatId, op):
        alerts = self.db['alerts']
        alerts[chatId][fsym][op][tsym].remove(target)
        if len(alerts[chatId][fsym][op][tsym]) == 0:
            alerts[chatId][fsym][op].pop(tsym)
            if len(alerts[chatId][fsym][op]) == 0:
                alerts[chatId][fsym].pop(op)
                if len(alerts[chatId][fsym]) == 0:
                    alerts[chatId].pop(fsym)
                    if len(alerts[chatId]) == 0:
                        alerts.pop(chatId)


    def processAlerts(self):
        if 'alerts' not in self.db:
            return
        self.log.debug('processing alerts')
        higher = 'HIGHER'
        lower = 'LOWER'
        alerts = self.db['alerts']
        toRemove = []
        for chatId in alerts:
            for fsym in alerts[chatId]:
                ops = alerts[chatId][fsym]
                for op in ops:
                    tsyms = ops[op]
                    for tsym in tsyms:
                        # self.log.info("ath for {} is {}".format(fsym, self.repository.get_ath(fsym, tsym)[0]))
                        targets = tsyms[tsym]
                        price = self.repository.get_price_if_valid(fsym, tsym)
                        for target in targets:
                            if op == lower and price < target or op == higher and price > target:
                                self.api.sendMessage(f"{fsym} is {'below' if op == lower else 'above'} {format_price(target)} at {format_price(price)} {tsym}", chatId)
                                toRemove.append((fsym, tsym, target, chatId, op))

        for tr in toRemove:
            self.removeAlert(tr[0], tr[1], tr[2], tr[3], tr[4])

    def processWatches(self):
        self.log.debug('processing watches')
        if 'watches' not in self.db:
            return  
        # self.log.debug(f'processing { len(self.db['watches']) } watches')
        i = 0
        while i < len (self.db['watches']):
            print(f'processing watch [{i}] of {len(self.db["watches"])}')
            watch = self.db['watches'][i]
            # if watch is ath true then get the ath and athdate
            if watch['from_ath']:
                comparitorprice, comparitordate = self.repository.get_ath(watch['fsym'], watch['tsym']) 

                # convert comparitordate from epoch ms to datetime
                comparitordate = datetime.fromtimestamp(comparitordate/1000)
                comparitordate_str = comparitordate.strftime('%d-%b-%Y')
            else:
                # caluclate periodindays from duration and duration_type
                duration = watch['duration'] 
                if watch['duration_type'][:3] == "day":
                    durationindays = duration
                elif watch['duration_type'][:4] == "week":
                    durationindays = duration * 7
                elif watch['duration_type'][:5] == "month":
                    durationindays = duration * 30
                elif watch['duration_type'][:4] == "year":
                    durationindays = duration * 365

                durationindays = int(durationindays)

                # at this point we have durationindays and can work backwards from now to find the comparitor date
                comparitordate = datetime.now() - timedelta(days=durationindays)

                # date rounding to nearest day for comparitor date
                comparitordate = comparitordate.replace(hour=0, minute=0, second=0, microsecond=0)


                # get the price for that symbol pair on that date
                comparitorprice = self.repository.get_day_price(watch['fsym'], watch['tsym'], comparitordate)

                # create string version of comparitor date in dd-mmm-yyyy format
                comparitordate_str = comparitordate.strftime('%d-%b-%Y')

            # log info the comparitor price and date
            self.log.debug(f"comparitor price: {comparitorprice} on {comparitordate}")

            # get the current price
            currentprice = self.repository.get_price_if_valid(watch['fsym'], watch['tsym'])



           # convert percentage values to absolute
            # if target contains a percentage then convert to absolute
            if '%' in watch['target']:
                targetpercentage = float(watch['target'].replace('%', ''))
                target = comparitorprice * (targetpercentage / 100)
            else:
                target = int(watch['target'])


            # lets see if this watch is persistent
            persistent = False
            last_notify = 0
            notify_frequency = 24 * 60 * 60
            if 'persistent' in watch:
                if watch['persistent']:
                    persistent = True
                if 'last_notify' in watch:
                    last_notify = watch['last_notify']
                    # convert last_notify from int as epoch to datetime
                if 'notify_frequency' in watch:
                    notify_frequency = watch['notify_frequency']
            last_notify = datetime.fromtimestamp(last_notify/1000)
                 
                

           # do the comparison
            if watch['op'] == 'drop':
                if currentprice < comparitorprice - target:
                    if persistent: # have to check it hasn't been notified too recently                        
                        if datetime.now() - last_notify < timedelta(seconds=notify_frequency):
                            self.log.debug("persistent watch, not notifying")
                            i += 1
                            continue
                                        
                    self.api.sendMessage(f"Drop watch: {watch['fsym']} is {currentprice} {watch['tsym']} which is at least {watch['target']} lower than it was at {comparitordate_str} when it was {format_price(comparitorprice)} ", watch['chatId'])
                    if not persistent:
                        self.log.debug("removing completed drop watch")
                        del self.db['watches'][i]
                    # set the most recent notify key as now epoch as int
                    self.db['watches'][i]['last_notify'] = int(datetime.now().timestamp()) * 1000
                
                else:
                    i += 1
            elif watch['op'] == 'rise':
                    if currentprice >  comparitorprice + target:
                        if persistent:
                            if datetime.now() - last_notify < timedelta(seconds=notify_frequency):
                                self.log.debug("persistent watch, not notifying")
                                i += 1
                                continue

                        self.api.sendMessage(f"Rise watch: {watch['fsym']} is {currentprice} {watch['tsym']} which is at least {watch['target']} higher than it was at {comparitordate_str} when it was {format_price(comparitorprice)} ", watch['chatId'])
                        if not persistent:
                            self.log.debug("removing completed rise watch")
                            del self.db['watches'][i]
                        else:
                            # set the most recent notify key as now epoch as int
                            self.db['watches'][i]['last_notify'] = int(datetime.now().timestamp()) * 1000                        

                    else:
                        i += 1
            elif watch['op'] == 'stable':
                # logic for stable is:
                
                # if it is persistent then check the last notify because if that fails then there is no point in calculating the stabilit
                if persistent:
                    if datetime.now() - last_notify < timedelta(seconds=notify_frequency):
                        self.log.debug("persistent watch, not notifying")
                        i += 1
                        continue

                # get current price
                currentprice = self.repository.get_price(watch['fsym'], watch['tsym'])

                if '%' in watch['target']:
                    pricerangepercentage = float(watch['target'].replace('%', ''))
                    pricerange = comparitorprice * (pricerangepercentage / 100)
                else:
                    pricerange = int(watch['target'])

                # work out the bounds
                #   stable_price_lower_bound
                stable_price_lower_bound = currentprice - pricerange


                #   stable_price_higher_bound
                stable_price_higher_bound = currentprice + pricerange
                
                stable = True
                testday_datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

                # Loop through the last n days
                for day in range(durationindays):
                    current_day = testday_datetime - timedelta(days=day)
                    
                    # For each day, get the price and see if it is more than the higher bound
                    testdayprice = self.repository.get_day_price(watch['fsym'], watch['tsym'], current_day)
                    if day == 0 and testdayprice == None: # sometimes binance won't say todays price, so we get current price instead
                        testdayprice = self.repository.get_price(watch['fsym'], watch['tsym'])
                    if testdayprice > stable_price_higher_bound or testdayprice < stable_price_lower_bound:
                        # Out of range, not stable
                        stable = False
                        break

                if stable:                    
                    self.api.sendMessage(f"Stable watch: {watch['fsym']} at {currentprice} is within +/- {watch['target']} range for {durationindays} days ", watch['chatId'])
                    if not persistent:
                        self.log.debug("removing completed Stable watch")
                        del self.db['watches'][i]
                    else:
                        # set the most recent notify key as now epoch as int
                        self.db['watches'][i]['last_notify'] = int(datetime.now().timestamp()) * 1000                        
                        i += 1
                else:
                    i += 1
                 
            else: # this item is invalid, delete it
                self.log.error(f"invalid watch op: {watch['op']}")
                del self.db['watches'][i]



            
        return

    def processUpdates(self, updates):
        for update in updates:
            self.last_update = self.db['last_update'] = update['update_id']            
            self.log.debug(f"processing update: {update}")
            if 'message' in update:
                message = update['message']
            elif "edited_message" in update:
                message = update['edited_message']
            else:
                self.log.debug(f"no message in update: {update}")
                return

            try:
                self.processMessage(message)                
            except:
                self.log.exception(f"error processing update: {update}")




    def persist_db(self):
        self.log.debug('persisting db')
        new_filename = config.DB_FILENAME + ".temp"  # New temporary file name
        
        if hashlib.md5(repr(self.db).encode('utf-8')).hexdigest() == self.dbmd5:
            self.log.debug('no change')
        else:
            self.log.debug('writing data to a new file')

            # Validate database structure before writing
            if not self._validate_db_structure(self.db):
                self.log.error('Database structure validation failed, skipping persistence')
                return

            try:
                # Write data to the new temporary file
                with open(new_filename, 'wb') as fp:
                    pickle.dump(self.db, fp)
                
                # Verify the written data by reading it back
                with open(new_filename, 'rb') as fp:
                    test_db = pickle.load(fp)
                    
                if not self._validate_db_structure(test_db):
                    self.log.error('Written database failed validation, aborting persistence')
                    if os.path.isfile(new_filename):
                        os.remove(new_filename)
                    return
                    
                # Check if written data matches what we intended to write
                if repr(test_db) != repr(self.db):
                    self.log.error('Written database does not match in-memory database, aborting persistence')
                    if os.path.isfile(new_filename):
                        os.remove(new_filename)
                    return
                
                self.log.debug('temporary file written and verified successfully')
                
            except Exception as e:
                self.log.error(f'Failed to write temporary database file: {e}')
                if os.path.isfile(new_filename):
                    os.remove(new_filename)
                return
            
            # Perform atomic file operations after confirming the data is saved to the new file
            if os.path.isfile(new_filename):
                self.log.debug('performing atomic file operations')
                
                # Create backup generations (keep last 3 backups)
                backup_filename = config.DB_FILENAME + ".backup"
                backup2_filename = config.DB_FILENAME + ".backup.2"
                backup3_filename = config.DB_FILENAME + ".backup.3"
                
                try:
                    # Rotate backups: backup.2 -> backup.3, backup -> backup.2
                    if os.path.isfile(backup2_filename):
                        if os.path.isfile(backup3_filename):
                            os.remove(backup3_filename)
                        os.rename(backup2_filename, backup3_filename)
                    
                    if os.path.isfile(backup_filename):
                        os.rename(backup_filename, backup2_filename)
                    
                    # Create new backup from current file
                    if os.path.isfile(config.DB_FILENAME):
                        os.rename(config.DB_FILENAME, backup_filename)
                    
                    # Make the new file current
                    os.rename(new_filename, config.DB_FILENAME)
                    
                    # Update MD5 hash after successful write
                    self.dbmd5 = hashlib.md5(repr(self.db).encode('utf-8')).hexdigest()
                    
                    self.log.debug('file persistence completed successfully')
                    
                except Exception as e:
                    self.log.error(f'Error occurred during atomic file operations: {e}')
                    
                    # Try to recover if possible
                    if os.path.isfile(backup_filename) and not os.path.isfile(config.DB_FILENAME):
                        try:
                            os.rename(backup_filename, config.DB_FILENAME)
                            self.log.info('Recovered main database file from backup')
                        except Exception as recovery_error:
                            self.log.error(f'Failed to recover database: {recovery_error}')
                    
                    # Clean up temporary file
                    if os.path.isfile(new_filename):
                        os.remove(new_filename)
            else:
                self.log.error('temporary file not found after write operation')

    def _validate_db_structure(self, db):
        """Validate that the database has the expected structure"""
        try:
            if not isinstance(db, dict):
                if hasattr(self, 'log'):
                    self.log.error('Database must be a dictionary')
                return False
                
            # Check alerts structure if present
            if 'alerts' in db:
                alerts = db['alerts']
                if not isinstance(alerts, dict):
                    if hasattr(self, 'log'):
                        self.log.error('alerts must be a dictionary')
                    return False
                    
                for chat_id, chat_alerts in alerts.items():
                    if not isinstance(chat_id, (int, str)):
                        if hasattr(self, 'log'):
                            self.log.error(f'Invalid chat_id type: {type(chat_id)}')
                        return False
                    if not isinstance(chat_alerts, dict):
                        if hasattr(self, 'log'):
                            self.log.error('chat alerts must be a dictionary')
                        return False
                        
                    for fsym, fsym_alerts in chat_alerts.items():
                        if not isinstance(fsym, str):
                            if hasattr(self, 'log'):
                                self.log.error(f'Invalid fsym type: {type(fsym)}')
                            return False
                        if not isinstance(fsym_alerts, dict):
                            if hasattr(self, 'log'):
                                self.log.error('fsym alerts must be a dictionary')
                            return False
            
            # Check watches structure if present
            if 'watches' in db:
                watches = db['watches']
                if not isinstance(watches, list):
                    if hasattr(self, 'log'):
                        self.log.error('watches must be a list')
                    return False
                    
                for i, watch in enumerate(watches):
                    if not isinstance(watch, dict):
                        if hasattr(self, 'log'):
                            self.log.error(f'watch {i} must be a dictionary')
                        return False
                        
                    required_fields = ['chatId', 'fsym', 'tsym', 'op', 'target']
                    for field in required_fields:
                        if field not in watch:
                            if hasattr(self, 'log'):
                                self.log.error(f'watch {i} missing required field: {field}')
                            return False
            
            # Check last_update if present
            if 'last_update' in db:
                if not isinstance(db['last_update'], (int, float)):
                    if hasattr(self, 'log'):
                        self.log.error('last_update must be a number')
                    return False
            
            return True
            
        except Exception as e:
            if hasattr(self, 'log'):
                self.log.error(f'Error validating database structure: {e}')
            return False

    def run(self, debug=True):
            self.log = logger_config.instance
            if debug:
                self.log.setLevel(logger_config.logging.DEBUG)
            else:
                self.log.setLevel(logger_config.logging.INFO)

            cache.log = self.log
            
            # Initialize database
            self.db = {}
            self.dbmd5 = ""
            
            # Try to load database with multiple fallback options
            loaded_successfully = False
            
            # Option 1: Load from main database file
            if not loaded_successfully and os.path.isfile(config.DB_FILENAME) and os.path.getsize(config.DB_FILENAME) > 0:
                try:
                    with open(config.DB_FILENAME, 'rb') as fp:
                        test_db = pickle.load(fp)
                    
                    if self._validate_db_structure(test_db):
                        self.db = test_db
                        self.dbmd5 = hashlib.md5(repr(self.db).encode('utf-8')).hexdigest()
                        self.log.info(f"Loaded database from main file with {len(self.db)} top-level keys")
                        loaded_successfully = True
                    else:
                        self.log.error("Main database file failed validation")
                        
                except Exception as e:
                    self.log.error(f"Failed to load main database file: {e}")
            
            # Option 2: Load from backup file
            if not loaded_successfully:
                backup_filename = config.DB_FILENAME + ".backup"
                if os.path.isfile(backup_filename) and os.path.getsize(backup_filename) > 0:
                    try:
                        with open(backup_filename, 'rb') as fp:
                            test_db = pickle.load(fp)
                        
                        if self._validate_db_structure(test_db):
                            self.db = test_db
                            self.dbmd5 = hashlib.md5(repr(self.db).encode('utf-8')).hexdigest()
                            self.log.warning(f"Loaded database from backup file with {len(self.db)} top-level keys")
                            loaded_successfully = True
                        else:
                            self.log.error("Backup database file failed validation")
                            
                    except Exception as e:
                        self.log.error(f"Failed to load backup database file: {e}")
            
            # Option 3: Load from backup.2 file
            if not loaded_successfully:
                backup2_filename = config.DB_FILENAME + ".backup.2"
                if os.path.isfile(backup2_filename) and os.path.getsize(backup2_filename) > 0:
                    try:
                        with open(backup2_filename, 'rb') as fp:
                            test_db = pickle.load(fp)
                        
                        if self._validate_db_structure(test_db):
                            self.db = test_db
                            self.dbmd5 = hashlib.md5(repr(self.db).encode('utf-8')).hexdigest()
                            self.log.warning(f"Loaded database from backup.2 file with {len(self.db)} top-level keys")
                            loaded_successfully = True
                        else:
                            self.log.error("Backup.2 database file failed validation")
                            
                    except Exception as e:
                        self.log.error(f"Failed to load backup.2 database file: {e}")
            
            # Option 4: Load from backup.3 file  
            if not loaded_successfully:
                backup3_filename = config.DB_FILENAME + ".backup.3"
                if os.path.isfile(backup3_filename) and os.path.getsize(backup3_filename) > 0:
                    try:
                        with open(backup3_filename, 'rb') as fp:
                            test_db = pickle.load(fp)
                        
                        if self._validate_db_structure(test_db):
                            self.db = test_db
                            self.dbmd5 = hashlib.md5(repr(self.db).encode('utf-8')).hexdigest()
                            self.log.warning(f"Loaded database from backup.3 file with {len(self.db)} top-level keys")
                            loaded_successfully = True
                        else:
                            self.log.error("Backup.3 database file failed validation")
                            
                    except Exception as e:
                        self.log.error(f"Failed to load backup.3 database file: {e}")
            
            # Final fallback: Create empty database
            if not loaded_successfully:
                self.log.warning("All database files failed to load or were invalid. Creating empty database.")
                self.db = {}
                self.dbmd5 = ""
                
                # Save the current state immediately to prevent future issues
                if loaded_successfully:
                    self.persist_db()

            self.api = TgApi(self.log)
            self.repository = MarketRepository(self.log)
            self.command_handler = CommandHandler(self.api, self.repository, self.db, self.log)

            self.log.debug("db at start: {}".format(self.db))
            self.last_update = self.db['last_update'] if 'last_update' in self.db else 0
            # main loop
            loop = True
            sequence_id = 0
            while loop:
                sequence_id += 1
                time.sleep(1)
                try:                
                    updates = self.api.getUpdates(self.last_update)   
    
                    if updates is None:
                        self.log.error('get update request failed')
                    else:
                        if len(updates) > 0:
                            self.processUpdates(updates)
                            # if we have just done an update then we should process alerts and watches
                            self.processAlerts()
                            self.processWatches()

                    # processing Alerts is quite cheap, do it every 3 seconds, if the current_seconds mod 2 = 0 then
                    if sequence_id % 3 == 0:
                        try:
                            self.processAlerts()
                        except:
                            self.log.exception("exception at processing alerts")

                    # processing watches is quite expensive, do it every 29 seconds, if the current_seconds mod 10 = 0
                    if sequence_id % 29 == 0:
                        try:
                            self.processWatches()
                        except:
                            self.log.exception("exception at processing watches")

                    

                except KeyboardInterrupt:
                    self.log.info("interrupt received, stopping…")
                    loop = False
                except requests.exceptions.ConnectionError as e:
                    # A serious problem happened, like DNS failure, refused connection, etc.
                    updates = None   
                except:            
                    self.log.exception("exception at processing updates")
                    loop = False

                self.persist_db()
                cache.persist()

if __name__ == "__main__":
    service = TgBotService()
    debug= True
    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        debug=True    
    service.run(debug)
