#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import peewee
import requests
from telebot import TeleBot
from datetime import datetime
from fast_bitrix24 import Bitrix
from urllib.parse import urlparse
from playhouse.db_url import connect
from configparser import ConfigParser


db_proxy = peewee.DatabaseProxy()


def markdownv2_converter(text):
    """
    Функция преобразует текст с учётом экранирования требуемых символов:
    https://core.telegram.org/bots/api#markdownv2-style
    """
    symbols_for_replace = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for symbol in symbols_for_replace:
        text = text.replace(symbol, '\\' + symbol)
    return text


def check_online(url):
    """
    Функция проверяет доступность домена из ссылки
    """
    parsed_url = urlparse(url)
    base_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url)
    if requests.get(base_url).status_code == 200:
        return True
    else:
        return False


class BaseModel(peewee.Model):
    class Meta:
        database = db_proxy


class Deals(BaseModel):
    """
    Скелет БД
    """
    id = peewee.IntegerField()
    category_id = peewee.IntegerField()
    assigned_by_id = peewee.IntegerField()
    date_create = peewee.DateTimeField()
    title = peewee.TextField()
    comments = peewee.TextField()
    message_id = peewee.IntegerField()


class Bitrix24Parser:

    def __init__(self, settings):
        self.settings = settings
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.online = check_online(self.settings.webhook)
        self.connect = Bitrix(self.settings.webhook, verbose=False)
        self.users = {}
        self.deals_opened = []
        self.deals_new = []
        self.deals_db = Deals
        self.db = connect(self.settings.db_url)
        db_proxy.initialize(self.db)
        self.db.create_tables([self.deals_db])
        self.emoji = {
            'person': '\U0001F9D1',
            'pin': '\U0001F4CC',
            'doc': '\U0001F4CB'
        }

    def run(self):
        self.generate_users()
        self.generate_opened_deals()
        self.remove_closed_deals_db()
        self.check_new_deals()
        if self.deals_new:
            self.update_db_and_send_new_deals()

    def check_new_deals(self):
        for deal in self.deals_opened:
            if not self.deal_in_db(deal['ID']):
                self.deals_new.append(deal)

    def deal_in_db(self, deal_id):
        try:
            self.deals_db.get(
                self.deals_db.id == deal_id,
            )
        except self.deals_db.DoesNotExist:
            return False
        else:
            return True

    def deal_in_deals_opened(self, deal_id):
        for deal in self.deals_opened:
            if deal['ID'] == deal_id:
                return True
        return False

    def update_db_and_send_new_deals(self):
        deals_new_lower = []
        for deal in self.deals_new:
            deal_lower = {}
            message_id = None
            for key in deal.keys():
                if key == 'DATE_CREATE':
                    date_time_str = deal[key][0:18]
                    date_time_form = '%Y-%m-%dT%H:%M:%S'
                    date_time = datetime.strptime(date_time_str, date_time_form)
                    deal_lower[key.lower()] = date_time
                else:
                    deal_lower[key.lower()] = deal[key]
            message_text = self.generate_message(deal_lower)
            message_id = self.bot.send_text_message(message_text)
            if message_id:
                deal_lower['message_id'] = message_id
                deals_new_lower.append(deal_lower)
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                time.sleep(3.5)
        self.deals_db.insert_many(deals_new_lower).execute()

    def generate_message(self, deal):
        user_name = markdownv2_converter(self.users[deal['assigned_by_id']])
        bid = f'{self.emoji["pin"]}Заявка №*{markdownv2_converter(deal["id"])}*'
        responsible = f'Ответственный: {self.emoji["person"]}__*{user_name}*__'
        message_text = f'{self.emoji["doc"]}{markdownv2_converter(deal["title"])}'
        message = f'{bid}\n{responsible}\n\n{message_text}'
        return message

    def remove_closed_deals_db(self):
        for deal in self.deals_db.select():
            if not self.deal_in_deals_opened(str(deal.id)):
                deal.delete_instance()

    def generate_users(self):
        bitrix24_users = self.connect.get_all(
            'user.get',
            params={
                'select': ['ID', 'NAME', 'LAST_NAME'],
                'filter': {'ACTIVE': 'True'}
            }
        )
        for user in bitrix24_users:
            self.users[user['ID']] = f'{user["NAME"]} {user["LAST_NAME"]}'

    def generate_opened_deals(self):
        self.deals_opened = self.connect.get_all(
            'crm.deal.list',
            params={
                'select': ['ID', 'ASSIGNED_BY_ID', 'TITLE', 'COMMENTS', 'DATE_CREATE', 'CATEGORY_ID'],
                'filter': {'CLOSED': 'N'}
            }
        )


class Conf:

    def __init__(self):
        self.work_dir = os.path.join(os.getenv('HOME'), '.config', 'Bitrix24toTelegram')
        self.config_file = os.path.join(self.work_dir, 'settings.conf')
        self.config = ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read('Telegram', 'botid')
        self.chatid = self.read('Telegram', 'chatid')
        self.webhook = self.read('Bitrix24', 'webhook')
        self.db_url = self.db_url_insert_path(self.read('System', 'db'))

    def exist(self):
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        if not os.path.exists(self.config_file):
            try:
                self.create()
            except FileNotFoundError as exc:
                print(exc)

    def create(self):
        self.config.add_section('Telegram')
        self.config.add_section('Bitrix24')
        self.config.add_section('System')
        self.config.set('Telegram', 'botid', '000000000:00000000000000000000000000000000000')
        self.config.set('Telegram', 'chatid', '00000000000000')
        self.config.set('Bitrix24', 'webhook', 'https://0000000000.bitrix24.ru/rest/00/0000000000000000/')
        self.config.set('System', 'db', 'sqlite:///bitrix24deals.db')
        with open(self.config_file, 'w') as config_file:
            self.config.write(config_file)
        raise FileNotFoundError(f'Required to fill data in config: {self.config_file}')

    def read(self, section, setting):
        value = self.config.get(section, setting)
        return value

    def db_url_insert_path(self, db_url):
        pattern = r'(^[A-z]*:\/\/\/)(.*$)'
        parse = re.match(pattern, db_url)
        prefix = parse.group(1)
        db_name = parse.group(2)
        path = os.path.join(self.work_dir, db_name)
        db_converted_url = prefix + path
        return db_converted_url


class TlgrmBot:

    def __init__(self, botid, chatid):
        self.botid = botid
        self.chatid = chatid
        self.bot = TeleBot(self.botid)

    def send_text_message(self, text):
        message = self.bot.send_message(
            chat_id=self.chatid,
            text=text,
            parse_mode='MarkdownV2',
            disable_web_page_preview=True,
        )
        return message.message_id

    def alive(self):
        try:
            self.bot.get_me()
        except Exception:
            return False
        else:
            return True


if __name__ == '__main__':
    config = Conf()
    btrx24 = Bitrix24Parser(config)
    if btrx24.online and btrx24.bot.alive():
        btrx24.run()
