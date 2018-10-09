import const.stat as ic
import api.ebest as eb
import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


class TelegramBot:
    def __init__(self, name, token):
        self.core = telegram.Bot(token)
        self.updater = Updater(token=token)
        self.dispatcher = self.updater.dispatcher

        self.text_print_handler = MessageHandler(Filters.text, self.print_message)
        self.dispatcher.add_handler(self.text_print_handler)

        # 013 : me, 535 : song
        self.id = ic.telegram["chat_id"]
        self.name = name

    def print_message(self, bot, text):
        print(bot)
        print(text)

    def send_message(self, text):
        for one_id in self.id:
            self.core.sendMessage(chat_id=one_id, text=text)

    def stop(self):
        self.updater.start_polling()
        self.updater.dispatcher.stop()
        self.updater.job_queue.stop()
        self.updater.stop()


class BotSmwj(TelegramBot):
    def __init__(self, object):
        # parent object
        self.par = object

        self.token = ic.telegram["token"]
        TelegramBot.__init__(self, 'smwj', self.token)

        self.add_handler("shutdown", self.shut_down)
        self.add_handler("investor", self.retrieve_investor_volume)
        self.add_handler("abort", self.abort)
        self.add_handler("indexamt", self.retrieve_market_index_tr_amt)
        self.add_handler("liquidity", self.retrieve_market_liquidity)
        self.updater.stop()

    def add_handler(self, cmd, func):
        self.dispatcher.add_handler(CommandHandler(cmd, func))

    def shut_down(self, bot, update):
        self.par.logger.info("shutdown command is accepted")

        self.par.shut_down()

    def retrieve_investor_volume(self, bot, update):
        self.par.logger.info("investor command is accepted")
        self.send_message("investor command is accepted")
        eb.pythoncom.CoInitialize()

        if eb.XASessionEventHandler.login_state == 0:
            eb.login(self.par.logger)

        if len(eb.XAQueryEventHandlerT8436.item_cd_list) <= 0:
            eb.retrieve_item_mst(self.par.logger, self.par.bind)

        param = update.message.text
        if " " in param:
            eb.retrieve_investor_volume(self.par.logger, self.par.bind, param.split(" ")[1], param.split(" ")[2])
            self.send_message("loaded investor volume from " + param.split(" ")[2] + " to " + param.split(" ")[1])

    def retrieve_market_index_tr_amt(self, bot, update):
        self.par.logger.info("indexamt command is accepted")
        self.send_message("indexamt command is accepted")
        eb.pythoncom.CoInitialize()

        if eb.XASessionEventHandler.login_state == 0:
            eb.login(self.par.logger)

        if len(eb.XAQueryEventHandlerT8436.item_cd_list) <= 0:
            eb.retrieve_item_mst(self.par.logger, self.par.bind)

        param = update.message.text
        if " " in param:
            eb.retrieve_market_index_tr_amt(self.par.logger, self.par.bind, param.split(" ")[1], param.split(" ")[2])
            self.send_message("loaded market tr amount from " + param.split(" ")[2] + " to " + param.split(" ")[1])

    def retrieve_market_liquidity(self, bot, update):
        self.par.logger.info("liquidity command is accepted")
        self.send_message("liquidity command is accepted")
        eb.pythoncom.CoInitialize()

        if eb.XASessionEventHandler.login_state == 0:
            eb.login(self.par.logger)

        if len(eb.XAQueryEventHandlerT8436.item_cd_list) <= 0:
            eb.retrieve_item_mst(self.par.logger, self.par.bind)

        param = update.message.text
        if " " in param:
            eb.retrieve_market_liquidity(self.par.logger
                                         , self.par.bind
                                         , param.split(" ")[1]
                                         , param.split(" ")[2]
                                         , param.split(" ")[3])
            self.send_message("loaded market liquidity from " + param.split(" ")[2] + " to " + param.split(" ")[1])

    def abort(self, bot, update):
        self.par.logger.info("abort command is accepted")

    def start(self):
        self.par.logger.info("chatbot started")
        self.updater.start_polling()
