import const.stat as ic
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
        # self.id = [695462013]
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
        self.updater.stop()

    def add_handler(self, cmd, func):
        self.dispatcher.add_handler(CommandHandler(cmd, func))

    def shut_down(self, bot, update):
        self.par.logger.info("shutdown command is accepted")

        self.par.shut_down()

    def start(self):
        self.par.logger.info("chatbot started")
        self.updater.start_polling()
