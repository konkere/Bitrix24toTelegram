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
from telebot.apihelper import ApiTelegramException


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


def str2bool(text):
    text = text.lower()
    true_variants = ['true', '1', 'yes', 'да', 'y', 'д', 't', 'правда', 'истина']
    answer = text in true_variants
    return answer


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


def read_id_list(file_with_list, pattern):
        telegram_id_list = {}
        with open(file_with_list, 'r+') as file:
            for line in file.readlines():
                line = line.rstrip('\n')
                re_line = re.match(pattern, line)
                try:
                    bitrix24_id = re_line.group(1)
                    tlgrm_id = re_line.group(2)
                    # name = re_line.group(3)
                except AttributeError:
                    continue
                if tlgrm_id:
                    telegram_id_list[bitrix24_id] = tlgrm_id
        return telegram_id_list


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
        self.bot_alive = True
        self.bot = self.generate_bot()
        self.online = check_online(self.settings.webhook)
        self.connect = Bitrix(self.settings.webhook, verbose=False)
        self.users = {}
        self.categories = {'0': 'Общее'}
        self.departments = {}
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
            'warning': '\U000026A0',    # ⚠️
        }

    def generate_bot(self):
        bots = {}
        for chat_id in self.settings.chat_id.keys():
            bots[chat_id] = TlgrmBot(
                botid=self.settings.botid,
                chatid=self.settings.chat_id[chat_id],
            )
            if not bots[chat_id].alive():
                self.bot_alive = False
        return bots

    def run(self):
        self.generate_users()
        self.generate_categories()
        self.generate_departments()
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
        deals_new = []
        deals_change_category = []
        deals_change_assigned = []
        for deal in self.deals_opened:
            if not self.deal_in_db(deal['ID']):
                deals_new.append(deal)
            elif self.deal_in_db(deal['ID']):
                category_changed, assigned_changed = self.data_changed(
                    deal['ID'],
                    deal['CATEGORY_ID'],
                    deal['ASSIGNED_BY_ID'],
                )
                if category_changed:
                    deals_change_category.append(deal)
                if assigned_changed and not category_changed:
                    deals_change_assigned.append(deal)
        self.deals_new = sorted(deals_new, key=lambda x: int(x['ID']))
        self.deals_change_category = sorted(deals_change_category, key=lambda x: int(x['ID']))
        self.deals_change_assigned = sorted(deals_change_assigned, key=lambda x: int(x['ID']))

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
        for deal in self.deals_new:
            deal_lower = dict_key_lower(deal)
            category_id = deal_lower['category_id']
            message_text = self.generate_message(deal=deal_lower)
            try:
                message_id = self.bot[category_id].send_text_message(message_text)
            except KeyError:
                continue
            else:
                deal_lower['message_id'] = message_id
                deal_lower['message_text'] = message_text
                self.deals_db.insert(deal_lower).execute()
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                sleep(3.5)

    def update_db_and_change_assigned(self):
        for deal in self.deals_change_assigned:
            deal = dict_key_lower(deal)
            deal_id = int(deal['id'])
            assigned_by_id = int(deal['assigned_by_id'])
            deal_in_db = self.deals_db.get(
                self.deals_db.id == deal_id,
            )
            category_id = deal['category_id']
            assigned_by_id_old = str(deal_in_db.assigned_by_id)
            message_text_old = deal_in_db.message_text
            message_text = self.generate_message(
                deal=deal,
                new_message=False,
                old_responsible_id=assigned_by_id_old
            )
            message_id_old = deal_in_db.message_id
            message_id = self.bot[category_id].send_text_message(message_text)
            if message_id:
                self.check_deprecated_message(category_id, message_id_old, message_text_old)
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
            assigned_by_id = deal['assigned_by_id']
            deal_in_db = self.deals_db.get(
                self.deals_db.id == deal_id,
            )
            category_id_old = str(deal_in_db.category_id)
            message_text_old = deal_in_db.message_text
            message_text = self.generate_message(deal)
            message_id_old = deal_in_db.message_id
            try:
                message_id = self.bot[category_id].send_text_message(message_text)
            except KeyError:
                self.check_deprecated_message(category_id_old, message_id_old, message_text_old)
                deal_in_db.delete_instance()
            else:
                self.check_deprecated_message(category_id_old, message_id_old, message_text_old)
                deal_in_db.category_id = category_id
                deal_in_db.assigned_by_id = assigned_by_id
                deal_in_db.message_text = message_text
                deal_in_db.message_id = message_id
                self.deals_db.bulk_update([deal_in_db], fields=[
                    self.deals_db.category_id,
                    self.deals_db.assigned_by_id,
                    self.deals_db.message_id,
                    self.deals_db.message_text,
                ])
                # Задержка из-за ограничения отправки ботом в чят не более 20 сообщений в минуту
                sleep(3.5)

    def check_deprecated_message(self, category_id, message_id, text):
        """
        Бот не может удалить сообщение старше 48 часов
        https://core.telegram.org/bots/api#deletemessage
        """
        bot = self.bot[category_id]
        try:
            bot.delete_message(message_id)
        except ApiTelegramException:
            new_message_text = f'{self.emoji["warning"]}Устаревшее сообщение\!\n\n~{text}~'
            bot.edit_exist_message(message_id, new_message_text)

    def generate_message(self, deal, new_message=True, old_responsible_id=None):
        bitrix24_id = deal['assigned_by_id']
        deal_id = markdownv2_converter(deal['id'])
        user_name = self.generate_responsible(bitrix24_id)
        bid = f'{self.emoji["pin"]}Заявка №*{deal_id}*'
        if new_message:
            responsible = f'Ответственный: {user_name}'
        else:
            old_user_name = self.generate_responsible(old_responsible_id, new_message)
            change_responsible = f'{old_user_name} → {user_name}'
            responsible = f'{self.emoji["recycle"]}Смена ответственного: {change_responsible}'
        message_text = f'{self.emoji["doc"]}{markdownv2_converter(deal["title"])}'
        message = f'{bid}\n{responsible}\n\n{message_text}'
        return message

    def generate_responsible(self, user_id, new_message=True):
        try:
            user_name = markdownv2_converter(self.users[user_id]['name'])
        except KeyError:
            user_name = 'Неизвестный'
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
                category = str(deal.category_id)
                new_message_text = f'{self.emoji["check"]}Закрыта\\!\n\n~{deal.message_text}~'
                message_id = self.bot[category].edit_exist_message(deal.message_id, new_message_text)
                if message_id:
                    deal.delete_instance()

    def generate_users(self):
        bitrix24_users = self.connect.get_all(
            'user.get',
            params={
                'select': ['ID', 'NAME', 'LAST_NAME', 'UF_DEPARTMENT'],
                # 'filter': {'ACTIVE': 'True'}
            }
        )
        for user in bitrix24_users:
            department = str(user["UF_DEPARTMENT"][0])
            self.users[user['ID']] = {
                'name': f'{user["NAME"]} {user["LAST_NAME"]}',
                'department': department
            }
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
            self.categories[category['ID']] = category['NAME']
        if not os.path.exists(self.settings.category_id_list_file):
            self.settings.create_category_id_list(self.categories)

    def generate_departments(self):
        bitrix24_departments = self.connect.get_all(
            'department.get',
            params={
                'select': ['ID', 'NAME'],
            }
        )
        for department in bitrix24_departments:
            self.departments[department['ID']] = department['NAME']
        if not os.path.exists(self.settings.department_id_list_file):
            self.settings.create_department_id_list(self.departments)

    def generate_opened_deals(self):
        deals_opened = self.connect.get_all(
            'crm.deal.list',
            params={
                'select': ['ID', 'ASSIGNED_BY_ID', 'TITLE', 'COMMENTS', 'DATE_CREATE', 'CATEGORY_ID',],
                'filter': {'CLOSED': 'N'}
            }
        )
        for deal in deals_opened:
            if self.settings.chat_by_department:
                category_id = self.users[deal['ASSIGNED_BY_ID']]['department']
                deal['CATEGORY_ID'] = category_id
            else:
                category_id = deal['CATEGORY_ID']
            if category_id in self.settings.chat_id.keys():
                self.deals_opened.append(deal)


class Conf:

    def __init__(self):
        self.work_dir = os.path.join(os.getenv('HOME'), '.config', 'Bitrix24toTelegram')
        self.config_file = os.path.join(self.work_dir, 'settings.conf')
        self.telegram_id_list_file = os.path.join(self.work_dir, 'telegram_id.list')
        self.category_id_list_file = os.path.join(self.work_dir, 'category_id.list')
        self.department_id_list_file = os.path.join(self.work_dir, 'department_id.list')
        self.config = ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read_conf('Telegram', 'botid')
        self.chat_by_department = str2bool(self.read_conf('Telegram', 'chat_by_department'))
        self.webhook = self.read_conf('Bitrix24', 'webhook')
        self.db_url = self.db_url_insert_path(self.read_conf('System', 'db'))
        self.tlgrm_id = {}
        self.chat_id = {}
        self.category_id = {}
        self.department_id = {}
        self.re_tlgrm_id = r'^(\d+)=(\d+)?#(.+)$'
        self.re_chat_id = r'^(\d+)=(-?\d+)?#(.+)$'
        self.generate_ids()

    def generate_ids(self):
        if os.path.exists(self.telegram_id_list_file):
            self.tlgrm_id = read_id_list(self.telegram_id_list_file, self.re_tlgrm_id)
        if os.path.exists(self.category_id_list_file):
            self.category_id = read_id_list(self.category_id_list_file, self.re_chat_id)
        if os.path.exists(self.department_id_list_file):
            self.department_id = read_id_list(self.department_id_list_file, self.re_chat_id)
        if self.chat_by_department:
            self.chat_id = self.department_id
        else:
            self.chat_id = self.category_id

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
        self.config.set('Telegram', 'chat_by_department', 'False')
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

    def create_category_id_list(self, categories):
        categories_for_write = ''
        for category in categories:
            categories_for_write += f'{category}=#{categories[category]}\n'
        with open(self.category_id_list_file, 'w') as list_file:
            list_file.write(categories_for_write)
        print(
            f'Нужно привязать ID чатов/групп Телеграма '
            f'в файле: {self.category_id_list_file}\n'
            f'Формат записи (одна на строку):\n'
            f'<ID Bitrix24>=<ID чата Телеграм>#<Название категории (или любой другой текст)>'
        )

    def create_department_id_list(self, departments):
        departments_for_write = ''
        for department in departments:
            departments_for_write += f'{department}=#{departments[department]}\n'
        with open(self.department_id_list_file, 'w') as list_file:
            list_file.write(departments_for_write)
        print(
            f'Можно привязать ID чатов/групп Телеграма '
            f'в файле: {self.department_id_list_file}\n'
            f'Формат записи (одна на строку):\n'
            f'<ID Bitrix24>=<ID чата Телеграм>#<Название отдела/department (или любой другой текст)>'
        )

    def read_conf(self, section, setting):
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
    if btrx24.online and btrx24.bot_alive:
        btrx24.run()
