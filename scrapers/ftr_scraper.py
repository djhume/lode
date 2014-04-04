#!/usr/bin/python

'''
ftr - automatic webscrapper for FTR data
(https://www.ftr.co.nz)

Copyright (C) 2014 Electricty Authority, New Zealand.

This is the ftr class.  It is used to connect, login, download,
convert and save bi-monthly ftr data.

FTR auctions take place every second Wednesday (primary) and every third
Wednesday (secondary) of the month.   Results are normally published by
the FTR manager by mid-day on the Thursday.

So, this python program is used with the following crontab:

0 14 8-21 * 4 /usr/bin/python /home/dave/python/ftr/ftr.py --ftr_pass='password' >> /home/dave/python/ftr/ftr_CRON.log 2>&1

Or at 2pm every second and third Thursday of the month - run this script and
download the FTR auction results.  Cool eh?

D J Hume, 26 March, 2014.
'''

import pandas as pd
import mechanize
from io import StringIO
import logging
import logging.handlers
import argparse
import os

#Setup command line option and argument parsing

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--ftr_host', action="store", dest='ftr_host', default='https://www.ftr.co.nz')
parser.add_argument('--ftr_user', action="store", dest='ftr_user', default='david.hume@ea.govt.nz')
parser.add_argument('--ftr_pass', action="store", dest='ftr_pass')
parser.add_argument('--ftr_path', action="store", dest='ftr_path', default=os.getcwd() + '/')

cmd_line = parser.parse_args()

#Setup logging

formatter = logging.Formatter('|%(asctime)-6s|%(message)s|', '%Y-%m-%d %H:%M:%S')
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(formatter)
logging.getLogger('').addHandler(consoleLogger)
fileLogger = logging.handlers.RotatingFileHandler(filename=cmd_line.ftr_path + 'ftr.log', maxBytes=1024 * 1024, backupCount=9)
fileLogger.setLevel(logging.ERROR)
fileLogger.setFormatter(formatter)
logging.getLogger('').addHandler(fileLogger)
logger = logging.getLogger('FTR ')
logger.setLevel(logging.INFO)


class ftr_scraper():
    """Used to login and update FTR data"""
    def __init__(self, ftr_host, ftr_user, ftr_pass, ftr_path):
        self.ftr_host = ftr_host
        self.ftr_user = ftr_user
        self.ftr_pass = ftr_pass
        self.ftr_path = ftr_path
        self.ftr_data = "https://www.ftr.co.nz/mui-register-ems-ihedge/XLSdownload/FTR.csv"
        self.ftr_csv = self.ftr_path + "FTR.csv"
        self.ml = 88

    def enter(self):
        """Setup browser to open FTR website"""
        try:
            self.br = mechanize.Browser()    # Browser
            self.br.set_handle_robots(False)
            self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
            self.br.open(self.ftr_host)
            msg = "Opening %s" % self.ftr_host
            logger.info(msg.center(self.ml, ' '))
        except:
            msg = "Unable to access %s" % str(self.ftr_host)
            logger.error(msg.center(self.ml, '*'))

    def login(self):
        """Attempt login"""
        try:
            self.br.select_form("_58_fm")  # select form
            self.br['_58_login'] = self.ftr_user  # enter email address
            self.br['_58_password'] = self.ftr_pass  # enter password
            self.br.set_all_readonly(False)
            self.r = self.br.submit()  # submit
            msg = "Login successful, updatng data..."
            logger.info(msg.center(self.ml, ' '))
        except:
            msg = "Unable to login to %s" % self.ftr_host
            logger.error(msg.center(self.ml, '*'))

    def grab_data(self):
        """Grab the csvfile and save to disk"""
        try:
            response = self.br.open(self.ftr_data).read()
            bufferIO = StringIO()
            bufferIO.write(unicode(response))
            bufferIO.seek(0)
            data = pd.read_csv(bufferIO, index_col=0)
            data.to_csv(self.ftr_csv)
            msg = "Updated FTR data to %s" % self.ftr_csv
            logger.info(msg.center(self.ml, ' '))
        except:
            msg = "Unable to download latest FTR data..."
            logger.error(msg.center(self.ml, '*'))

#Start the programme
if __name__ == '__main__':
    f = ftr_scraper(cmd_line.ftr_host, cmd_line.ftr_user,
                    cmd_line.ftr_pass, cmd_line.ftr_path)  # run instance
    f.enter()
    f.login()
    f.grab_data()
