#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import peewee
import requests
from time import sleep
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


def dict_key_lower(dict_orig):
    dict_lower = {}
    for key in dict_orig.keys():
        if key == 'DATE_CREATE':
            date_time_str = dict_orig[key][0:18]
            date_time_form = '%Y-%m-%dT%H:%M:%S'
            date_time = datetime.strptime(date_time_str, date_time_form)
            dict_lower[key.lower()] = date_time
        else:
            dict_lower[key.lower()] = dict_orig[key]
    return dict_lower


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
    message_text = peewee.TextField()


class Bitrix24Parser:

    def __init__(self, settings):
        self.settings = settings
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.online = check_online(self.settings.webhook)
        self.connect = Bitrix(self.settings.webhook, verbose=False)
        self.users = {}
        self.categories = {'0': 'Общее'}
        self.deals_opened = []
        self.deals_new = []
        self.deals_change_assigned = []
        self.deals_change_category = []
        self.deals_db = Deals
        self.db = connect(self.settings.db_url)
        db_proxy.initialize(self.db)
        self.db.create_tables([self.deals_db])
        self.emoji = {
            'person': '\U0001F9D1',     # 🧑
            'pin': '\U0001F4CC',        # 📌
            'doc': '\U0001F4CB',        # 📋
            'recycle': '\U0000267B',    # ♻
            'category': '\U0001F4CE',   # 📎
            'check': '\U00002705',      # ✅
        }

    def run(self):
        self.generate_users()
        self.generate_categories()
        self.generate_opened_deals()
        self.remove_closed_deals()
        self.check_new_deals()
        if self.deals_change_category:
            self.update_db_and_change_category()
        if self.deals_change_assigned:
            self.update_db_and_change_assigned()
        if self.deals_new:
            self.update_db_and_send_new_deals()

    def check_new_deals(self):
        for deal in self.deals_opened:
            if not self.deal_in_db(deal['ID']):
                self.deals_new.append(deal)
            elif self.deal_in_db(deal['ID']):
                category_changed, assigned_changed = self.data_changed(
                    deal['ID'],
                    deal['CATEGORY_ID'],
                    deal['ASSIGNED_BY_ID'],
                )
                if category_changed:
                    self.deals_change_category.append(deal)
                if assigned_changed:
                    self.deals_change_assigned.append(deal)

    def data_changed(self, deal_id, category_id, assigned_id):
        deal = self.deals_db.get(
            self.deals_db.id == int(deal_id),
        )
        category_changed = (deal.category_id != int(category_id))
        assigned_changed = (deal.assigned_by_id != int(assigned_id))
        return category_changed, assigned_changed

    def deal_in_db(self, deal_id):
        try:
            self.deals_db.get(
                self.deals_db.id == int(deal_id),
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
            deal_lower = dict_key_lower(deal)
            message_text = self.generate_message(deal=deal_lower)
            message_id = self.bot.send_text_message(message_text)
            if message_id:
                deal_lower['message_id'] = message_id
                deal_lower['message_text'] = message_text
                deals_new_lower.append(deal_lower)
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                sleep(3.5)
        self.deals_db.insert_many(deals_new_lower).execute()

    def update_db_and_change_assigned(self):
        for deal in self.deals_change_assigned:
            deal = dict_key_lower(deal)
            deal_id = int(deal['id'])
            assigned_by_id = int(deal['assigned_by_id'])
            deal_in_db = self.deals_db.get(
                self.deals_db.id == deal_id,
            )
            assigned_by_id_old = str(deal_in_db.assigned_by_id)
            message_text = self.generate_message(
                deal=deal,
                new_message=False,
                old_responsible_id=assigned_by_id_old
            )
            message_id = self.bot.send_text_message(message_text)
            if message_id:
                self.bot.delete_message(deal_in_db.message_id)
                deal_in_db.assigned_by_id = assigned_by_id
                deal_in_db.message_id = message_id
                deal_in_db.message_text = message_text
                self.deals_db.bulk_update([deal_in_db], fields=[
                    self.deals_db.assigned_by_id,
                    self.deals_db.message_id,
                    self.deals_db.message_text,
                ])
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                sleep(3.5)

    def update_db_and_change_category(self):
        for deal in self.deals_change_category:
            deal = dict_key_lower(deal)
            deal_id = int(deal['id'])
            category_id = deal['category_id']
            category_text = (markdownv2_converter(self.categories[category_id])).replace(' ', '_')
            deal_in_db = self.deals_db.get(
                self.deals_db.id == deal_id,
            )
            category_id_old = str(deal_in_db.category_id)
            category_text_old = (markdownv2_converter(self.categories[category_id_old])).replace(' ', '_')
            message_text_old = deal_in_db.message_text
            message_text = message_text_old.replace(category_text_old, category_text)
            message_id = deal_in_db.message_id
            check_message_id = self.bot.edit_exist_message(message_id, message_text)
            if check_message_id:
                deal_in_db.category_id = category_id
                deal_in_db.message_text = message_text
                self.deals_db.bulk_update([deal_in_db], fields=[
                    self.deals_db.category_id,
                    self.deals_db.message_text,
                ])
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                sleep(3.5)

    def generate_message(self, deal, new_message=True, old_responsible_id=None):
        bitrix24_id = deal['assigned_by_id']
        deal_id = markdownv2_converter(deal['id'])
        user_name = self.generate_responsible(bitrix24_id)
        bid = f'{self.emoji["pin"]}Заявка №*{deal_id}*'
        category_name = markdownv2_converter(self.categories[deal['category_id']])
        if new_message:
            responsible = f'Ответственный: {user_name}'
        else:
            old_user_name = self.generate_responsible(old_responsible_id, new_message)
            change_responsible = f'{old_user_name} → {user_name}'
            responsible = f'{self.emoji["recycle"]}Смена ответственного: {change_responsible}'
        category = f'{self.emoji["category"]}*\#{category_name}*'
        message_text = f'{self.emoji["doc"]}{markdownv2_converter(deal["title"])}'
        message = f'{bid}\n{responsible}\n{category}\n\n{message_text}'
        return message

    def generate_responsible(self, user_id, new_message=True):
        user_name = markdownv2_converter(self.users[user_id])
        if new_message:
            try:
                telegram_id = self.settings.tlgrm_id[user_id]
            except KeyError:
                name = f'{self.emoji["person"]}__*{user_name}*__'
            else:
                name = f'{self.emoji["person"]}[__*{user_name}*__](tg://user?id={telegram_id})'
        else:
            name = f'{self.emoji["person"]}__{user_name}__'
        return name

    def remove_closed_deals(self):
        for deal in self.deals_db.select():
            if not self.deal_in_deals_opened(str(deal.id)):
                new_message_text = f'{self.emoji["check"]}Закрыта\\!\n\n{deal.message_text}'
                message_id = self.bot.edit_exist_message(deal.message_id, new_message_text)
                if message_id:
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
        if not os.path.exists(self.settings.telegram_id_list_file):
            self.settings.create_telegram_id_list(self.users)

    def generate_categories(self):
        bitrix24_categories = self.connect.get_all(
            'crm.dealcategory.list',
            params={
                'select': ['ID', 'NAME'],
                'filter': {'IS_LOCKED': 'N'}
            }
        )
        for category in bitrix24_categories:
            category_without_spaces = str(category['NAME']).replace(' ', '_')
            self.categories[category['ID']] = category_without_spaces

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
        self.telegram_id_list_file = os.path.join(self.work_dir, 'telegram_id.list')
        self.config = ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read_conf('Telegram', 'botid')
        self.chatid = self.read_conf('Telegram', 'chatid')
        self.webhook = self.read_conf('Bitrix24', 'webhook')
        self.db_url = self.db_url_insert_path(self.read_conf('System', 'db'))
        self.tlgrm_id = {}
        self.re_tlgrm_id = r'^(\d+)=(\d+)?#(.+)$'
        if os.path.exists(self.telegram_id_list_file):
            self.tlgrm_id = self.read_telegram_id_list()

    def exist(self):
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        if not os.path.exists(self.config_file):
            try:
                self.create_conf()
            except FileNotFoundError as exc:
                print(exc)

    def create_conf(self):
        self.config.add_section('Telegram')
        self.config.add_section('Bitrix24')
        self.config.add_section('System')
        self.config.set('Telegram', 'botid', '000000000:00000000000000000000000000000000000')
        self.config.set('Telegram', 'chatid', '00000000000000')
        self.config.set('Bitrix24', 'webhook', 'https://0000000000.bitrix24.ru/rest/00/0000000000000000/')
        self.config.set('System', 'db', 'sqlite:///bitrix24deals.db')
        with open(self.config_file, 'w') as config_file:
            self.config.write(config_file)
        raise FileNotFoundError(f'Требуется внести данные в конфиг: {self.config_file}')

    def create_telegram_id_list(self, users):
        users_for_write = ''
        for user in users:
            users_for_write += f'{user}=#{users[user]}\n'
        with open(self.telegram_id_list_file, 'w') as list_file:
            list_file.write(users_for_write)
        print(
            f'Можно привязать пользовательские ID Телеграма '
            f'(для упоминаний с оповещением) в файле: {self.telegram_id_list_file}\n'
            f'Формат записи (одна на строку):\n'
            f'<ID Bitrix24>=<ID Телеграм>#<Имя Фамилия (или любой другой текст)>'
        )

    def read_conf(self, section, setting):
        value = self.config.get(section, setting)
        return value

    def read_telegram_id_list(self):
        telegram_id_list = {}
        with open(self.telegram_id_list_file, 'r+') as file:
            for line in file.readlines():
                line = line.rstrip('\n')
                re_line = re.match(self.re_tlgrm_id, line)
                try:
                    bitrix24_id = re_line.group(1)
                    tlgrm_id = re_line.group(2)
                    # name = re_line.group(3)
                except AttributeError:
                    continue
                if tlgrm_id:
                    telegram_id_list[bitrix24_id] = tlgrm_id
        return telegram_id_list

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

    def edit_exist_message(self, message_id, message_text):
        message = self.bot.edit_message_text(
            text=message_text,
            chat_id=self.chatid,
            message_id=message_id,
            parse_mode='MarkdownV2',
            disable_web_page_preview=True,
        )
        return message.message_id

    def delete_message(self, message_id):
        self.bot.delete_message(
            chat_id=self.chatid,
            message_id=message_id,
        )

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
