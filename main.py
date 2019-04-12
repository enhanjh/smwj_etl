# python 3.6 32bit
# installed package
# 1. pythoncom
# 2. mysql-connector-python
# 3. sqlalchemy
# 4. odo and [datapipelines, networkx 1.11, cassiopeia]
# 5. request
# 6. python-telegram-bot

import logging
import os
import sys
import time
import datetime as dt
import urllib.request as req
import urllib.parse as pars
import const.stat as ic
import api.ebest as eb
import interface.bot as bot
import xml.etree.ElementTree as et
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Operator:
    def __init__(self):
        # variable init
        self.bot_smwj = object()
        self.logger = object()
        self.db_session = object()
        self.logger = object()
        self.engine = object()
        self.bind = str()
        self.today = time.strftime("%Y%m%d")

        # sub classes init
        self.logger_start()
        self.chatbot_start()
        self.orm_init()

        # business day check
        if len(sys.argv) > 1 and sys.argv[1] == 'server':
            if self.bizday_check():
                # etl run
                if len(sys.argv) > 2 and sys.argv[2] is not None:
                    self.etl_run(sys.argv[2])
                else:
                    self.etl_run(self.today)
            else:
                self.shut_down()

    def chatbot_start(self):
        self.bot_smwj = bot.BotSmwj(self)
        self.bot_smwj.start()
        self.bot_smwj.send_message("smwj-etl is starting up")

    def logger_start(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
        self.logger = logging.getLogger()

        fh = TimedRotatingFileHandler("C:\SMWJ_LOG\\etl", when="midnight")
        fh.setFormatter(formatter)
        fh.suffix = "_%Y%m%d.log"

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.INFO)

    def orm_init(self):
        scott = ic.dbconfig["user"]
        tiger = ic.dbconfig["password"]
        host  = ic.dbconfig["host"]
        self.bind = 'mysql+mysqlconnector://' + scott + ':' + tiger + '@' + host + ':3306/smwj'

        self.engine = create_engine(self.bind)
        dbsession = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.db_session = dbsession()

    def etl_run(self, edate):
        eb.login(self.logger)
        eb.retrieve_item_mst(self.logger, self.bind)

        #edate = self.today
        row_cnt = "1"

        d = datetime.today() - timedelta(days=10)
        sdate = d.strftime("%Y%m%d")

        eb.retrieve_daily_chart(self.logger, self.bind, self.db_session, edate, edate)
        eb.retrieve_investor_volume(self.logger, self.bind, edate, edate)
        eb.retrieve_market_index_tr_amt(self.logger, self.bind, edate, edate)
        eb.retrieve_abroad_index(self.logger, self.bind, edate, row_cnt)
        eb.retrieve_short_selling(self.logger, self.bind, edate, edate)
        eb.retrieve_market_liquidity(self.logger, self.bind, self.engine, edate, sdate, row_cnt)        

        self.shut_down()

    def bizday_check(self):
        url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getHoliDeInfo'
        query_params = '?' + pars.urlencode(
            {pars.quote_plus('serviceKey'): ic.publicdata['key'], pars.quote_plus('solYear'): self.today[:4],
             pars.quote_plus('solMonth'): self.today[4:6]})

        request = req.Request(url + query_params)
        request.get_method = lambda: 'GET'
        response_body = req.urlopen(request).read()

        root = et.fromstring(response_body)
        holidays = list()
        for locdate in root.iter('locdate'):
            holidays.append(locdate.text)

        self.logger.info("holiday list")
        self.logger.info(holidays)

        bizday = True
        if dt.datetime.today().weekday() >= 5:
            bizday = False
            self.bot_smwj.send_message("today is weekend")
        elif self.today in holidays:
            bizday = False
            self.bot_smwj.send_message("today is holiday")
        elif self.today[4:8] == '0501':
            bizday = False
            self.bot_smwj.send_message("today is mayday")

        return bizday

    def shut_down(self):
        self.bot_smwj.send_message("smwj-etl is shutting down")

        os._exit(0)


if __name__ == "__main__":
    op = Operator()
