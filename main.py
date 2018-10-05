# python 3.6 32bit
# installed package
# 1. pythoncom
# 2. mysql-connector-python
# 3. sqlalchemy
# 4. odo and [datapipelines, networkx 1.11, cassiopeia]
# 5. request

import logging
import os
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
        self.botSmwj = object()
        self.logger = object()
        self.db_session = object()
        self.logger = object()
        self.bind = str()
        self.today = time.strftime("%Y%m%d")

        # sub classes init
        self.logger_start()
        self.chatbot_start()
        self.orm_init()

        # business day check
        if self.bizday_check():
            # etl run
            self.etl_run()
        else:
            self.shut_down()

    def chatbot_start(self):
        self.botSmwj = bot.BotSmwj(self)
        self.botSmwj.start()
        self.botSmwj.send_message("smwj-etl is starting up")

    def logger_start(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        self.logger = logging.getLogger("myLogger")
        formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
        fh = TimedRotatingFileHandler("C:\SMWJ_LOG\\etl", when="midnight")
        fh.setFormatter(formatter)
        fh.suffix = "_%Y%m%d.log"
        self.logger.addHandler(fh)
        self.logger.setLevel(logging.DEBUG)

    def orm_init(self):
        scott = ic.dbconfig["user"]
        tiger = ic.dbconfig["password"]
        self.bind = 'mysql+mysqlconnector://' + scott + ':' + tiger + '@localhost:3306/smwj'

        engine = create_engine(self.bind)
        DBSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.db_session = DBSession()

    def etl_run(self):
        eb.login(self.logger)
        eb.retrieve_item_mst(self.logger, self.bind)

        edate = self.today
        sdate = self.today
        row_cnt = "1"
        if dt.datetime.today().weekday() == 4:  # friday
            d = datetime.today() - timedelta(days=3650)
            sdate = d.strftime("%Y%m%d")
            row_cnt = "800"

        eb.retrieve_daily_chart(self.logger, self.bind, self.db_session, edate, sdate)
        eb.retrieve_investor_volume(self.logger, self.bind, edate, sdate)
        eb.retrieve_market_index_tr_amt(self.logger, self.bind, edate, edate)
        eb.retrieve_abroad_index(self.logger, self.bind, edate, row_cnt)

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
            self.botSmwj.send_message("today is weekend")
        elif self.today in holidays:
            bizday = False
            self.botSmwj.send_message("today is holiday")
        elif self.today[4:8] == '0501':
            bizday = False
            self.botSmwj.send_message("today is mayday")

        return bizday

    def shut_down(self):
        self.botSmwj.send_message("smwj-etl is shutting down")

        os._exit(0)


if __name__ == "__main__":
    op = Operator()
