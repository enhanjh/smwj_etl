import win32com.client
import pythoncom
import time
import odo
import pandas as pd
import const.stat as ic
import sqlalchemy as sa


# class definition
# 1. login
class XASessionEventHandler:
    login_state = 0
    logger = None

    def OnLogin(self, code, msg):
        if code == "0000":
            self.logger.info("로그인 성공")
            XASessionEventHandler.login_state = 1
        else:
            self.logger.info("로그인 실패")


# 2. retrieve item master
class XAQueryEventHandlerT8436:
    query_state = 0
    item_cd_list = list()

    def OnReceiveData(self, code):
        XAQueryEventHandlerT8436.query_state = 1


# 3. retrieve daily chart
class XAQueryEventHandlerT8413:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT8413.query_state = 1


# 4. transaction volume by investor group
class XAQueryEventHandlerT1717:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT1717.query_state = 1


# 5. market index update
# 5.1 korean market index
class XAQueryEventHandlerT1514:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT1514.query_state = 1


# 5.2 market index tr vol by investor group
class XAQueryEventHandlerT1665:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT1665.query_state = 1


# 5.3 abroad market index
class XAQueryEventHandlerT3518:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT3518.query_state = 1


# function definition
# 1. login
def login(obj):
    XASessionEventHandler.logger = obj
    inst_xa_session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEventHandler)

    user_id = ic.api_cred["id"]
    user_pw = ic.api_cred["idpw"]
    cert_pw = ic.api_cred["certpw"]

    inst_xa_session.ConnectServer("hts.ebestsec.co.kr", 20001)
    inst_xa_session.Login(user_id, user_pw, cert_pw, 0, 0)

    while XASessionEventHandler.login_state == 0:
        pythoncom.PumpWaitingMessages()


# 2. item master update
def retrieve_item_mst(logger, bind):
    logger.info("retrieve item master")

    XAQueryEventHandlerT8436.logger = logger

    instXAQueryT8436 = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEventHandlerT8436)
    instXAQueryT8436.ResFileName = "C:\\eBEST\\xingAPI\\Res\\T8436.res"

    instXAQueryT8436.SetFieldData("t8436InBlock", "gubun", 0, "0")  # 0:all, 1:kospi, 2:kosdaq

    instXAQueryT8436.Request(0)

    while XAQueryEventHandlerT8436.query_state == 0:
        pythoncom.PumpWaitingMessages()

    count = instXAQueryT8436.GetBlockCount("t8436OutBlock")

    item_nm = []
    item_sp = []
    etf_sp = []
    spac_sp = []

    for i in range(count):
        XAQueryEventHandlerT8436.item_cd_list.append(instXAQueryT8436.GetFieldData("t8436OutBlock", "shcode", i).strip())
        item_nm.append(instXAQueryT8436.GetFieldData("t8436OutBlock", "hname", i).strip())
        item_sp.append(instXAQueryT8436.GetFieldData("t8436OutBlock", "gubun", i).strip())
        etf_sp.append(instXAQueryT8436.GetFieldData("t8436OutBlock", "etfgubun", i).strip())
        spac_sp.append(instXAQueryT8436.GetFieldData("t8436OutBlock", "spac_gubun", i).strip())

    dict_rslt = {
        "item": XAQueryEventHandlerT8436.item_cd_list,
        "item_nm": item_nm,
        "item_sp": item_sp,
        "etf_sp": etf_sp,
        "spac_sp": spac_sp
    }
    columns = ["item", "item_nm", "item_sp", "etf_sp", "spac_sp"]
    df_rslt = pd.DataFrame(dict_rslt, columns=columns)

    df_rslt.to_sql('item', con=bind, if_exists='replace', index=False)


# 3. retrieve daily chart
def retrieve_daily_chart(logger, bind, db_session, edate, sdate):
    logger.info("retrieve daily prices")

    metadata = sa.MetaData(bind=bind)
    tbl = sa.Table('price'
                   , metadata
                   , sa.Column('item', sa.String, primary_key=True)
                   , sa.Column('tran_day', sa.String, primary_key=True)
                   , sa.Column('open', sa.Integer)
                   , sa.Column('high', sa.Integer)
                   , sa.Column('low', sa.Integer)
                   , sa.Column('close', sa.Integer)
                   )

    # truncate price_temp table when periodic search
    if edate != sdate:
        db_session.execute("truncate table price")
        db_session.commit()

    items = list()

    instXAQueryT8413 = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEventHandlerT8413)
    instXAQueryT8413.ResFileName = "C:\\eBEST\\xingAPI\\Res\\T8413.res"

    for idx, val in enumerate(XAQueryEventHandlerT8436.item_cd_list):
        logger.info(str(idx) + " : " + str(val))
        instXAQueryT8413.SetFieldData("t8413InBlock", "shcode", 0, val)
        instXAQueryT8413.SetFieldData("t8413InBlock", "gubun", 0, "2")
        instXAQueryT8413.SetFieldData("t8413InBlock", "qrycnt", 0, "2000")
        instXAQueryT8413.SetFieldData("t8413InBlock", "sdate", 0, sdate)
        instXAQueryT8413.SetFieldData("t8413InBlock", "edate", 0, edate)
        instXAQueryT8413.SetFieldData("t8413InBlock", "comp_yn", 0, "Y")  # compress y/n

        time.sleep(3)

        XAQueryEventHandlerT8413.query_state = 0
        instXAQueryT8413.Request(0)

        while XAQueryEventHandlerT8413.query_state == 0:
            pythoncom.PumpWaitingMessages()

        if sdate == edate:  # today's price only
            row = list()
            row.append(val)
            row.append(edate)
            row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock", "disiga", 0)))
            row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock", "dihigh", 0)))
            row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock", "dilow", 0)))
            row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock", "diclose", 0)))

            items.append(row)

        else:  # periodic price
            decompSize = instXAQueryT8413.Decompress("t8413OutBlock1")
            count = instXAQueryT8413.GetBlockCount("t8413OutBlock1")

            items.clear()

            for i in range(count):
                row = list()
                row.append(val)
                row.append(instXAQueryT8413.GetFieldData("t8413OutBlock1", "date", i))
                row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock1", "open", i)))
                row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock1", "high", i)))
                row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock1", "low", i)))
                row.append(int(instXAQueryT8413.GetFieldData("t8413OutBlock1", "close", i)))

                items.append(row)

            odo.odo(items, tbl)

    if sdate == edate:
        odo.odo(items, tbl)


# 4. transaction volume by investor group
def retrieve_investor_volume(logger, bind, edate, sdate):
    logger.info("retrieve tr volume of investor")

    metadata = sa.MetaData(bind=bind)
    tbl = sa.Table('investor'
                   , metadata
                   , sa.Column('item', sa.String, primary_key=True)
                   , sa.Column('tran_day', sa.String, primary_key=True)
                   , sa.Column('close', sa.Integer)
                   , sa.Column('diff', sa.Integer)
                   , sa.Column('diff_rate', sa.Float)
                   , sa.Column('tot_vol', sa.Float)
                   , sa.Column('pfund', sa.Float)
                   , sa.Column('sec', sa.Float)
                   , sa.Column('insure', sa.Float)
                   , sa.Column('trust', sa.Float)
                   , sa.Column('bank', sa.Float)
                   , sa.Column('aggre_finan', sa.Float)
                   , sa.Column('pension', sa.Float)
                   , sa.Column('etcfirm', sa.Float)
                   , sa.Column('ant', sa.Float)
                   , sa.Column('regi_fore', sa.Float)
                   , sa.Column('nonregi_fore', sa.Float)
                   , sa.Column('country', sa.Float)
                   , sa.Column('inst', sa.Float)
                   , sa.Column('fore', sa.Float)
                   , sa.Column('etc', sa.Float)
                   )

    instXAQueryT1717 = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEventHandlerT1717)
    instXAQueryT1717.ResFileName = "C:\\eBEST\\xingAPI\\Res\\T1717.res"

    items = list()

    for idx, val in enumerate(XAQueryEventHandlerT8436.item_cd_list):
        logger.info(str(idx) + " : " + str(val))
        instXAQueryT1717.SetFieldData("t1717InBlock", "shcode", 0, val)
        instXAQueryT1717.SetFieldData("t1717InBlock", "gubun", 0, "0")  # 0: net buy, 1: cumsum
        instXAQueryT1717.SetFieldData("t1717InBlock", "fromdt", 0, sdate)
        instXAQueryT1717.SetFieldData("t1717InBlock", "todt", 0, edate)

        time.sleep(3)

        XAQueryEventHandlerT1717.query_state = 0
        instXAQueryT1717.Request(0)

        while XAQueryEventHandlerT1717.query_state == 0:
            pythoncom.PumpWaitingMessages()

        count = instXAQueryT1717.GetBlockCount("t1717OutBlock")

        for i in range(count):
            row = list()
            row.append(val)
            row.append(instXAQueryT1717.GetFieldData("t1717OutBlock", "date", i))
            row.append(int(instXAQueryT1717.GetFieldData("t1717OutBlock", "close", i)))
            row.append(int(instXAQueryT1717.GetFieldData("t1717OutBlock", "change", i)))
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "diff", i)))
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "volume", i)))
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0000_vol", i)))  # 사모펀드
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0001_vol", i)))  # 증권
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0002_vol", i)))  # 보험
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0003_vol", i)))  # 투신
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0004_vol", i)))  # 은행
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0005_vol", i)))  # 종금
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0006_vol", i)))  # 기금
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0007_vol", i)))  # 기타법인
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0008_vol", i)))  # 개인
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0009_vol", i)))  # 등록외국인
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0010_vol", i)))  # 미등록외국인
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0011_vol", i)))  # 국가외
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0018_vol", i)))  # 기관
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0016_vol", i)))  # 외인계(등록+미등록)
            row.append(float(instXAQueryT1717.GetFieldData("t1717OutBlock", "tjj0017_vol", i)))  # 기타계(기타+국가)

            items.append(row)

    odo.odo(items, tbl)

# 5. market index update
def retrieve_abroad_index(logger, bind, today, row_cnt):
    logger.info("retrieve abroad indices")

    metadata = sa.MetaData(bind=bind)
    tbl = sa.Table('market_index'
                   , metadata
                   , sa.Column('item', sa.String, primary_key=True)
                   , sa.Column('tran_day', sa.String, primary_key=True)
                   , sa.Column('open', sa.Integer)
                   , sa.Column('high', sa.Integer)
                   , sa.Column('low', sa.Integer)
                   , sa.Column('close', sa.Integer)
                   )

    items = {"R-USDKRWSMBS", "S-DJI@DJI", "S-NAS@IXIC", "S-SPI@SPX", "S-NII@NI225"}  # 원달러, 다우존스산업

    instXAQueryT3518 = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEventHandlerT3518)
    instXAQueryT3518.ResFileName = "C:\\eBEST\\xingAPI\\Res\\T3518.res"

    rslt = list()

    for idx, val in enumerate(items):
        logger.info(str(idx) + " : " + str(val))
        temp = val.split("-")
        instXAQueryT3518.SetFieldData("t3518InBlock", "kind", 0, temp[0])
        instXAQueryT3518.SetFieldData("t3518InBlock", "symbol", 0, temp[1])
        instXAQueryT3518.SetFieldData("t3518InBlock", "cnt", 0, row_cnt)  # 입력건수
        instXAQueryT3518.SetFieldData("t3518InBlock", "jgbn", 0, "0")  # 0: 일, 1: 주, 2: 월, 3: 분

        rslt = rslt + retrieve_abroad_index_api_call(instXAQueryT3518, 0, temp, today, "", row_cnt)

    odo.odo(rslt, tbl)


# 5.1 api call (devided for continuous search)
def retrieve_abroad_index_api_call(instXAQueryT3518, cont_yn, temp, cts_date, cts_time, row_cnt):
    time.sleep(3)

    instXAQueryT3518.SetFieldData("t3518InBlock", "cts_date", 0, cts_date)
    instXAQueryT3518.SetFieldData("t3518InBlock", "cts_time", 0, cts_time)

    XAQueryEventHandlerT3518.query_state = 0
    instXAQueryT3518.Request(cont_yn)

    while XAQueryEventHandlerT3518.query_state == 0:
        pythoncom.PumpWaitingMessages()

    return retrieve_abroad_index_api_callback(instXAQueryT3518, temp, row_cnt)


# 5.2 api callback (devided for continuous search)
def retrieve_abroad_index_api_callback(instXAQueryT3518, temp, row_cnt):
    cts_date = instXAQueryT3518.GetFieldData("t3518OutBlock", "cts_date", 0)
    cts_time = instXAQueryT3518.GetFieldData("t3518OutBlock", "cts_time", 0)

    count = instXAQueryT3518.GetBlockCount("t3518OutBlock1")

    rslt = list()

    for i in range(count):
        row = list()
        row.append(temp[1])
        row.append(instXAQueryT3518.GetFieldData("t3518OutBlock1", "date", i))

        mul = 1
        if temp[0] == "S":
            mul = 100  # 해외지수는 100 을 곱해야 맞음

        row.append(float(instXAQueryT3518.GetFieldData("t3518OutBlock1", "open", i)) * mul)
        row.append(float(instXAQueryT3518.GetFieldData("t3518OutBlock1", "high", i)) * mul)
        row.append(float(instXAQueryT3518.GetFieldData("t3518OutBlock1", "low", i)) * mul)
        row.append(float(instXAQueryT3518.GetFieldData("t3518OutBlock1", "price", i)) * mul)

        rslt.append(row)

    if not row_cnt == "1":
        rslt = rslt + retrieve_abroad_index_api_call(instXAQueryT3518, 1, temp, cts_date, cts_time, row_cnt)

    return rslt