import telebot
from telebot import apihelper
from telebot import types 
import datetime
import requests
import json
import telegram.ext
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram.ext import messagequeue as mq
import base64
import time
from Avia_api_test_sabre import test_sabre_api
from Avia_api_test_sirena import test_sirena_api
from Bus_api_test_Busfor import test_busfor_api
from Bus_api_test_Etraffic import test_etraffic_api
from Bus_api_test_IMS import test_ims_api
from Trains import test_trains_api
import logging
import psycopg2
import configparser
from psycopg2 import OperationalError
import pandas as pd


config = configparser.ConfigParser()
config.read('config.ini')

is_test_working = True
client_status = {}
token = config['telegram']['token']
chat_with_tanya = config['telegram']['chat_with_tanya']
sls_chat = config['telegram']['sls_chat']
bot = telebot.TeleBot(token=token)
apihelper.proxy = {'https': 'socks5://power:380log8048@185.213.209.137:1080'}
login = config['jira']['user']
password = config['jira']['password']
login_test = config['api_test']['user']
password_test = config['api_test']['password']
proxy_url = 'socks5://power:380log8048@185.213.209.137:1080'
jira_lp = base64.b64encode('{}:{}'.format(login, password).encode()).decode("ascii")
proxy_kwargs = {'username': 'power', 'password': '380log8048@'}
headers = {'Content-type': 'application/json',
           'Accept': 'text/plain',
           'Pos': login_test}

prod_db = config['pgsql']['database']
user_prod_db = config['pgsql']['user']
pwd_prod_db = config['pgsql']['password']
host_prod_db = config['pgsql']['host']
port = config['pgsql']['port']

test_db = config['pgsql_test']['database']
user_test_db = config['pgsql_test']['user']
pwd_test_db = config['pgsql_test']['password']
host_test_db = config['pgsql_test']['host']

test_api_url = config['api_test']['url']
jira_url = config['jira']['url']
jira_filter = 'project%20%3D%20SLS%20AND%20status%20%3D%20\"Ожидание%20тех%20поддержки\"%20AND%20assignee%20in%20(EMPTY)'
jira_cookie = ''
old_issues = dict()
new_issues_count = 0
jira_all_issues = 'project%20%3D%20SLS%20AND%20created%20>%3D%20-22h'
jira_closed_issues ='project%20%3D%20SLS%20AND%20status%20in%20(Решен%2C%20Отменено%2C%20Закрыто)%20AND%20created%20>%3D%20-22h'

jira_closed_by_khasanov = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(r.khasanov)"
jira_closed_by_akimov = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(n.akimov)"
jira_closed_by_rogachev = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(i.rogachev)"
jira_closed_by_karamyshev = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(a.karamyshev)"
jira_closed_by_toporyschev = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(m.toporishev)"
jira_closed_by_yablochkin = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(a.yablochkin)"
jira_closed_by_dorzhiev = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-24h%20AND%20assignee%20in%20(a.dorzhiev)"

jira_closed_by_karamyshev_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(a.karamyshev)"
jira_closed_by_toporyschev_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(m.toporishev)"
jira_closed_by_rogachev_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(i.rogachev)"
jira_closed_by_khasanov_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(r.khasanov)"
jira_closed_by_akimov_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(n.akimov)"
jira_closed_by_yablochkin_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(a.yablochkin)"
jira_closed_by_dorzhiev_by_week = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-7d%20AND%20assignee%20in%20(a.dorzhiev)"

jira_closed_by_khasanov_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(r.khasanov)"
jira_closed_by_akimov_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(n.akimov)"
jira_closed_by_rogachev_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(i.rogachev)"
jira_closed_by_karamyshev_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(a.karamyshev)"
jira_closed_by_toporyschev_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(m.toporishev)"
jira_closed_by_yablochkin_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(a.yablochkin)"
jira_closed_by_dorzhiev_by_month = "project%20%3D%20SLS%20AND%20status%20%3D%20Решен%20AND%20created%20>%3D%20-30d%20AND%20assignee%20in%20(a.dorzhiev)"


# ФОРМИРОВАНИЕ КЛАВИАТУРЫ
markup_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
# btn_address = types.KeyboardButton("Я здесь", request_location=True)
btn_commands = types.KeyboardButton("Доступные команды")
markup_menu.add(btn_commands)

jira_buttons = types.InlineKeyboardMarkup()
week_btn = types.InlineKeyboardButton('За неделю', callback_data='week')
month_btn = types.InlineKeyboardButton('За месяц', callback_data='month')
jira_buttons.add(week_btn, month_btn)


service_buttons = types.InlineKeyboardMarkup()
btn_avia = types.InlineKeyboardButton("Авиа", callback_data="Avia")
btn_zhd = types.InlineKeyboardButton("ЖД", callback_data="ZHD")
btn_bus = types.InlineKeyboardButton("Автобусы", callback_data="Bus")
service_buttons.add(btn_avia, btn_bus, btn_zhd)

bus_carriers_buttons = types.InlineKeyboardMarkup()
btn_busfor = types.InlineKeyboardButton("Busfor", callback_data="busfor")
btn_ims = types.InlineKeyboardButton("IMS", callback_data="ims")
btn_etraffic = types.InlineKeyboardButton("E-Traffic", callback_data="etraffic")
bus_carriers_buttons.add(btn_busfor, btn_ims, btn_etraffic)

avia_carriers_buttons = types.InlineKeyboardMarkup()
btn_sirena = types.InlineKeyboardButton("Sirena", callback_data="sirena")
btn_sabre = types.InlineKeyboardButton("Sabre", callback_data="sabre")
avia_carriers_buttons.add(btn_sirena, btn_sabre)


logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                    filename='test.txt', level=logging.INFO)


def complex_jira_request(url, filter):
    try:
        req_all_issues = requests.get('{}/rest/api/2/search?jql={}'.format(url, filter),
                                      cookies={'JSESSIONID': jira_cookie})
        if req_all_issues.status_code != 200:
            cookie = jira_cookie_refresh(jira_url, jira_lp)
            req_all_issues = requests.get('{}/rest/api/2/search?jql={}'.format(url, filter),
                                          cookies={'JSESSIONID': cookie})
        issues = json.loads(req_all_issues.content)['total']
        return issues
    except Exception as e:
        print(e)


def datetime_to_standart(datetime):
    return datetime.strftime("%d-%m-%Y")


def ranking(callback, update):
    print(callback)
    print(update)
    now = datetime.datetime.now()
    closed_by_akimov = complex_jira_request(jira_url, jira_closed_by_akimov)
    closed_by_khasanov = complex_jira_request(jira_url, jira_closed_by_khasanov)
    closed_by_rogachev = complex_jira_request(jira_url, jira_closed_by_rogachev)
    closed_by_toporyschev = complex_jira_request(jira_url, jira_closed_by_toporyschev)
    closed_by_karamyshev = complex_jira_request(jira_url, jira_closed_by_karamyshev)
    closed_by_yablochkin = complex_jira_request(jira_url, jira_closed_by_yablochkin)
    closed_by_dorzhiev = complex_jira_request(jira_url, jira_closed_by_dorzhiev)
    output_msg = '''
    Статистика по закрытым обращениям за последние сутки
    Хасанов: {}
    Акимов: {}
    Рогачев: {}
    Карамышев: {}
    Топорищев: {}
    Яблочкин: {}
    Доржиев: {}
    '''.format(closed_by_khasanov, closed_by_akimov, closed_by_rogachev, closed_by_karamyshev, closed_by_toporyschev,
               closed_by_yablochkin, closed_by_dorzhiev)
    bot.send_message(callback.message.chat.id, text=output_msg, reply_markup=jira_buttons)


def ranking_by_week(callback):
    khasanov = complex_jira_request(jira_url, jira_closed_by_khasanov_by_week)
    akimov = complex_jira_request(jira_url, jira_closed_by_akimov_by_week)
    rogachev = complex_jira_request(jira_url, jira_closed_by_rogachev_by_week)
    toporyschev = complex_jira_request(jira_url, jira_closed_by_toporyschev_by_week)
    karamyshev = complex_jira_request(jira_url, jira_closed_by_karamyshev_by_week)
    yablochkin = complex_jira_request(jira_url, jira_closed_by_yablochkin_by_week)
    dorzhiev = complex_jira_request(jira_url, jira_closed_by_dorzhiev_by_week)
    output_msg = '''
        Статистика по закрытым обращениям за неделю
        Хасанов: {}
        Акимов: {}
        Рогачев: {}
        Карамышев: {}
        Топорищев: {}
        Яблочкин: {}
        Доржиев: {}
        '''.format(khasanov, akimov, rogachev, karamyshev, toporyschev, yablochkin, dorzhiev)
    return output_msg


def ranking_by_month(callback):
    khasanov = complex_jira_request(jira_url, jira_closed_by_khasanov_by_month)
    akimov = complex_jira_request(jira_url, jira_closed_by_akimov_by_month)
    rogachev = complex_jira_request(jira_url, jira_closed_by_rogachev_by_month)
    toporyschev = complex_jira_request(jira_url, jira_closed_by_toporyschev_by_month)
    karamyshev = complex_jira_request(jira_url, jira_closed_by_karamyshev_by_month)
    yablochkin = complex_jira_request(jira_url, jira_closed_by_yablochkin_by_month)
    dorzhiev = complex_jira_request(jira_url, jira_closed_by_dorzhiev_by_month)
    output_msg = '''
        Статистика по закрытым обращениям за месяц
        Хасанов: {}
        Акимов: {}
        Рогачев: {}
        Карамышев: {}
        Топорищев: {}
        Яблочкин: {}
        Доржиев: {}
        '''.format(khasanov, akimov, rogachev, karamyshev, toporyschev, yablochkin, dorzhiev)
    return output_msg


def create_connection(db_user, db_password, db_name, db_host, db_port):
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )

        return connection
    except Exception as e:
        logging.warning(f'Error {e} occured')
        print(e)


def execute_long_reserve(callback, update):
    try:
        bot.send_message(callback.message.chat.id, text='Выполняю поиск залипших (>3hours) заказов...')
        query = '''
        select "OrderItemId" from "OrderItem" oi 
        left join "Order" o on oi."OrderId" = o."OrderId" 
        where oi."OperationType" = 1
        and oi."Status" >= 110
        and oi."Status" < 200
        and now() - oi."ConfirmTill" > '03 hours'
        and o."WorkMode" = 0 and oi."Created" >= '2020-03-01'
        '''
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgresSQL DB successful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        orderitems = []
        result = cursor.fetchall()
        connection_prod.close()
        if len(result) > 0:
            for i in result:
                for item in i:
                    orderitems.append(item)
            bot.send_message(callback.message.chat.id, text='OrderItemIds: ' + str(orderitems))
        else:
            bot.send_message(callback.message.chat.id, text='Залипших заказов не обнаружено')
            logging.info('Поиск залипших заказов успешно выполнен')
    except OperationalError as e:
        logging.warning(f"The error '{e}' occured")


def changed_confirm_date(callback, update):
    try:
        bot.send_message(callback.message.chat.id, text='Выполняю поиск заказов с переходящей датой...')
        query = '''
        select "OrderItemId", "Amount", "OperationType", "ReservNumber", "Confirmed",  "ConfirmedByProvider"
        from "OrderItem"
        where "Status">=200 and "ServiceType"=2 and "Created" between '20200316' and '20200316' --указать интервал даты расхождения
        group by "OrderItemId", "Amount", "OperationType", "ReservNumber"
        having date_trunc('day',"Confirmed")!=date_trunc('day', "ConfirmedByProvider")
        '''
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgresSQL DB successful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        orderitems = []
        result = cursor.fetchall()
        connection_prod.close()
        if len(result) > 0:
            for i in result:
                for item in i:
                    orderitems.append(item)
            bot.send_message(callback.message.chat.id, text='OrderItemIds: ' + str(orderitems))
        else:
            bot.send_message(callback.message.chat.id, text='Заказов с переходящей датой не обнаружено')
            logging.info('Поиск заказов с переходящей датой успешно выполнен')
    except OperationalError as e:
        logging.warning(f"The error '{e}' occured")


def execute_integration_doubles(callback, update):
    try:
        integration_ids = []
        bot.send_message(callback.message.chat.id, text='Выполняю поиск дублей интеграции...')
        query = '''
        select "PartnerContractRatePlanIntegrationId" from "PartnerContractRatePlanIntegration"
        where "PartnerContractRatePlanIntegrationId" in 
        (select aaa."PartnerContractRatePlanIntegrationId"
        from "PartnerContractRatePlanIntegration" aaa
        join 
        (select aaa."PartnerId", aaa."Amount", aaa."Created", aaa."ContractRatePlanAccountId", aaa."OperationType", count(*) 
        from "PartnerContractRatePlanIntegration" aaa
        where "Created" >= '2020-01-01'
        group by aaa."PartnerId", aaa."Amount", aaa."Created", aaa."ContractRatePlanAccountId", aaa."OperationType"
        having count(*) > 1) bbb
        on 
        aaa."Created" = bbb."Created" and aaa."PartnerId" = bbb."PartnerId" and aaa."Amount" = bbb."Amount" and 
        aaa."ContractRatePlanAccountId" = bbb."ContractRatePlanAccountId" and aaa."OperationType" = bbb."OperationType")
        '''
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgreSQL DB sucessful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        connection_prod.close()
        if len(result) > 0:
            for i in result:
                for id in i:
                    integration_ids.append(id)
            bot.send_message(callback.message.chat.id, text=str(integration_ids))
        else:
            bot.send_message(callback.message.chat.id, text='Дубли интеграции не обнаружены')
            logging.info('Поиск дублей интеграции успешно выполнен')
    except OperationalError as e:
        logging.warning(f"The error '{e}' occured")


def jira_cookie_refresh(url, jira_lp):
    jira_cookie = requests.Session().get(url,
                                         headers={"Authorization": "Basic {}".format(jira_lp)}).cookies['JSESSIONID']
    print('jira_cookies refresh')
    return jira_cookie


def jira_issues_to_dict(jira_filter, jira_cookie):  # Формирование словаря с задачами вида {SLS-****:{key:info}}
    ret = dict()
    req = requests.get('{}/rest/api/2/search?jql={}&maxResults=500'.format(jira_url, jira_filter),
                                                                                cookies={'JSESSIONID': jira_cookie})
    if req.status_code != 200:
        cookie = jira_cookie_refresh(jira_url, jira_lp)
        req = requests.get('{}/rest/api/2/search?jql={}&maxResults=500'.format(jira_url, jira_filter),
                                                                                        cookies={'JSESSIONID': cookie})
        if req.status_code != 200:
            logging.info('jira_bc got to be changed')
            print('jira_bc got to be changed')
    # print(req.content.decode('utf8'))
    if req.status_code == 200 and int(json.loads(req.content)['total']) > 0:
        resp = json.loads(req.content)
        for i in resp['issues']:
            ret.update({i['key']: {'priority': i['fields']['priority']['name'],
                                   'summary': i['fields']['summary'],
                                   'url': '{}/browse/{}'.format(jira_url, i['key'])}})

    return ret


def jira_issues_to_chat(callback):  # Формирование и отправка текста с новыми задачами
    try:
        new_issues = jira_issues_to_dict(jira_filter, jira_cookie)
        newest_issues_keys = [x for x in new_issues.keys() if x not in old_issues.keys()]
        if len(newest_issues_keys) > 0:
            if len(newest_issues_keys) == 1:
                issues_text = 'Новая задача!'
            else:
                issues_text = 'Новые задачи!'
            for issue in newest_issues_keys:
                old_issues.update({issue: new_issues[issue]})
                issue_str = '\n{}\n{}'.format(new_issues[issue]['summary'], new_issues[issue]['url'])
                issues_text += issue_str
            bot.send_message(sls_chat, issues_text)
            logging.info('Новые задачи отправлены')
        else:
            logging.info("Новых задач нет")
    except Exception as e:
        logging.warning('При получении новый задач возникла ошибка:\n' + str(e))


def daily_report(callback):
    print('Начинаем формирование отчета..')
    now_day = datetime.datetime.now()
    req_all_issues = requests.get('{}/rest/api/2/search?jql={}'.format(jira_url, jira_all_issues),
                                  cookies={'JSESSIONID': jira_cookie})

    req_closed_issues = requests.get('{}/rest/api/2/search?jql={}'.format(jira_url, jira_closed_issues),
                                     cookies={'JSESSIONID': jira_cookie})

    if req_all_issues.status_code != 200:
        cookie = jira_cookie_refresh(jira_url, jira_lp)
        req_all_issues = requests.get('{}/rest/api/2/search?jql={}'.format(jira_url, jira_all_issues),
                                      cookies={'JSESSIONID': cookie})
        if req_all_issues.status_code != 200:
            logging.info('jira_bc got to be changed')
            print('jira_bc got to be changed')

    if req_closed_issues.status_code != 200:
        cookie = jira_cookie_refresh(jira_url, jira_lp)
        req_closed_issues = requests.get('{}/rest/api/2/search?jql={}'.format(jira_url, jira_closed_issues),
                                         cookies={'JSESSIONID': cookie})
        if req_closed_issues.status_code != 200:
            logging.info('jira_bc got to be changed')
            print('jira_bc got to be changed')

    if req_all_issues.status_code == 200 and req_closed_issues.status_code == 200:
        all_issues = json.loads(req_all_issues.content)["total"]
        closed_issues = json.loads(req_closed_issues.content)["total"]
        try:
            bot.send_message(chat_id=sls_chat, text='''
        Статистика за {}:
        Cоздано {} заявок.
        Решено {} заявок.
        Молодцы. Отлично поработали!
        '''.format(now_day.strftime('%d.%m'), all_issues, closed_issues, parse_mode='HTML'))
        except Exception as e:
            logging.warning('При отправке статистики возникла ошибка:\n' + str(e))
        print('Статистика успешно отправлена')


class MQBot(telegram.bot.Bot):
    """A subclass of Bot which delegates send method handling to MQ"""

    def __init__(self, is_queued_def=True, mqueue=None, *args, **kwargs):
        super(MQBot, self).__init__(*args, **kwargs)
        # below 2 attributes should be provided for decorator usage
        self._is_messages_queued_default = is_queued_def
        self._msg_queue = mqueue or mq.MessageQueue()

    def __del__(self):
        try:
            self._msg_queue.stop()
        except Exception as e:
            print(e)
            pass
        super(MQBot, self).bot.__del__()


def datetime_to_im_time(date):
    if type(date) is datetime.datetime:
        return date.strftime('%Y-%m-%dT00:00:00')
    return None


def execute_sevzapppk_sales():
    try:
        now_day = today_to_dbtime()
        yesterday = yesterday_to_dbtime()
        query = '''
        select sum(oi."Amount") from "Order" o 
        join "OrderItem" oi on o."OrderId" = oi."OrderId"
        where o."AgentId" = 430 and oi."OperationType" = 1 and oi."Status" = 200 and 
        oi."Confirmed" >= '{}' and oi."Confirmed" < '{}';
        '''.format(yesterday, now_day)
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgreSQL DB sucessful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        connection_prod.close()
        if result[0][0] is not None:
            return result[0][0]
        else:
            return 0
    except OperationalError as e:
       print(e)


def execute_all_sales_bo():
    try:
        now_day = today_to_dbtime()
        yesterday = yesterday_to_dbtime()
        query = '''
        select sum(case when i."OperationType" = 1 then i."Amount" end) "Сумма покупок (OI)"
        from "OrderItem" i join "Partner" p on p."PartnerId" = i."FinProviderId"
        where "Status">=200 and "Confirmed" between '{}' and '{}' and p."PartnerId" != 294 
        and i."ServiceType" = 2 and i."PosId" not in (select "PosId" from "Pos" where "PartnerId" in (430,434))
        '''.format(yesterday, now_day)
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgreSQL DB sucessful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        connection_prod.close()
        if result[0][0] is not None:
            return result[0][0]
        else:
            return 0
    except OperationalError as e:
       print(e)


def execute_zhd_talons_bo():
    try:
        now_day = today_to_dbtime()
        yesterday = yesterday_to_dbtime()
        query = '''
        select sum("Amount") from "OrderItem"
        where "OperationType" = 1 and "Status" = 200 and "Confirmed" >= '{}'
        and "Confirmed" < '{}' and "ServiceType" = 21
        '''.format(yesterday, now_day)
        connection_prod = create_connection(user_prod_db, pwd_prod_db, prod_db, host_prod_db, port)
        print("Connection to PostgreSQL DB sucessful")
        connection_prod.autocommit = True
        cursor = connection_prod.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        connection_prod.close()
        if result[0][0] is not None:
            return result[0][0]
        else:
            return 0
    except OperationalError as e:
       print(e)


def today_to_dbtime():
    now = datetime.datetime.now()
    now_day = '{}-{}-{}'.format(now.year, now.month, now.day)
    return now_day


def yesterday_to_dbtime():
    month30 = [4, 6, 9, 11]
    d = datetime.datetime.now()
    if d.day == 1:
        if d.month == 1:
            yesterday = '{}-12-31'.format(d.year)
            return yesterday
        else:
            if d.month in month30:
                yesterday = '{}-{}-31'.format(d.year, d.month - 1)
                return yesterday
            else:
                yesterday = '{}-{}-30'.format(d.year, d.month - 1)
                return yesterday
    yesterday = '{}-{}-{}'.format(d.year, d.month, d.day - 1)
    return yesterday


def get_yesterday_datetime():
    month30 = [4, 6, 9, 11]
    d = datetime.datetime.now()
    if d.day == 1:
        if d.month == 1:
            return str(31), str(12), str(d.year - 1)
        else:
            if d.month in month30:
                return str(31), str(d.month - 1), str(d.year)
            else:
                return str(30), str(d.month - 1), str(d.year)
    return str(d.day - 1), str(d.month), str(d.year)


def etinfo_complex_request(input, headers, url):
    try:
        req = requests.post(url, data=input, headers=headers)
        df = pd.read_html(req.content.decode('windows-1251').replace('<p>', ' <p>'), thousands=None)[0]
        return max(df['Сумма'])

    except Exception as e:
        print(e)
        print('Check ETinfo Auth cookies')


def fin_differenses(updates):

    now_day, now_month, now_year = get_yesterday_datetime()
    etinfo_url = config['jira']['et_info']
    etinfo_msk_data = {'WINDOWID': 'STARTPAGE',
                       "day": now_day,
                       "month": now_month,
                       'year': now_year,
                       'rtype': 'html',
                       'onlyrdors': 'on',
                       'onlydoritog': 'on',
                       'etid2nombl14': '0',
                       'rid': '0',
                       'dor': '',
                       'agent': '7',
                       'plagn': '-1',
                       'vidr': '0',
                       'mopl': '1',
                       'tmuch': '0',
                       'saleobj': '0',
                       'perev': '-1',
                       'ago': '-1',
                       'trmFinAddr': '',
                       'send': '%CE%F2%EE%F1%EB%E0%F2%FC+%E7%E0%EF%F0%EE%F1+%ED%E0+%EF%EE%E8%F1%EA'}

    etinfo_szppk_data = {'WINDOWID': 'STARTPAGE',
                         "day": now_day,
                         "month": now_month,
                         'year': now_year,
                         'rtype': 'html',
                         'onlyrdors': 'on',
                         'onlydoritog': 'on',
                         'etid2nombl14': '0',
                         'rid': '0',
                         'dor': '',
                         'agent': '7',
                         'plagn': '-1',
                         'vidr': '0',
                         'mopl': '1',
                         'tmuch': '0',
                         'saleobj': '0',
                         'perev': '-1',
                         'ago': '@20023',
                         'trmFinAddr': '',
                         'send': '%CE%F2%EE%F1%EB%E0%F2%FC+%E7%E0%EF%F0%EE%F1+%ED%E0+%EF%EE%E8%F1%EA'}

    headers_etinfo = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                      'Accept-Encoding': 'gzip, deflate',
                      'Content-Type': 'application/x-www-form-urlencoded',
                      'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                      'Content-Length': '257',
                      'Upgrade-Insecure-Requests': '1'
                      }

    etinfo_szppk_sum = etinfo_complex_request(etinfo_szppk_data, headers_etinfo, etinfo_url)
    szppk_all_sales_sum = etinfo_complex_request(etinfo_msk_data, headers_etinfo, etinfo_url)
    szppk_sales_bo = execute_sevzapppk_sales()
    all_bo_sales = execute_all_sales_bo()
    zh_talons = execute_zhd_talons_bo()
    szppk = round(float(szppk_all_sales_sum) - float(etinfo_szppk_sum) - float(szppk_sales_bo),2)
    bo = round(float(all_bo_sales), 2) + round(float(zh_talons), 2)
    if szppk == bo:
        text = 'Расхождений с ЕТинфо по продажам не найдено'
        bot.send_message(sls_chat, text)
        logging.info(text)
    else:
        text = 'Обнаружено расхождение с ЕТинфо по продажам на сумму: ' + str(szppk - bo) + ' рублей'
        bot.send_message(sls_chat, text)
        logging.info(text)


def complex_request(input, headers, url, output):
    start_time = time.time()
    req = requests.post(url, data=json.dumps(input), headers=headers, auth=(login_test, password_test))
    url = url.split("/")
    run_time = time.time() - start_time
    if req.status_code == 200:
        output += "{} выполнен за {} sec со статусом {}\n".format(url[len(url)-1], round(run_time), req.status_code)
    else:
        output += '{} выполнен за {} sec со статусом: {}\n{}'.format(url[len(url)-1], round(run_time), req.status_code, req.json())
    return req.status_code, req.json(), output


def test_api_check(callback):
    try:
        global is_test_working
        origincode = "2000000"
        destenationcode = "2030100"
        departuredate = datetime_to_im_time(datetime.datetime.now() + datetime.timedelta(days=20))
        trainpricing_url = test_api_url + "Railway/V1/Search/TrainPricing"
        output_data = ""
        trainpricing_data = {
            "Origin": origincode,
            "Destination": destenationcode,
            "DepartureDate": departuredate
        }
        trainpricing_sc, trainpricing, output_data = complex_request(trainpricing_data, headers, trainpricing_url,
                                                                     output_data)
        if trainpricing_sc != 200 and is_test_working == True:
            bot.send_message(chat_id=sls_chat, text='Кажется, тест не работает')
            is_test_working = False
            logging.info('Тестовый Экспресс недоступен')
        if trainpricing_sc == 200 and is_test_working == False:
            bot.send_message(chat_id=sls_chat, text='Тест заработал!')
            is_test_working = True
            logging.info('Тестовый Экспресс доступен')
    except Exception as e:
        logging.warning('При проверке тестовой среды возникла ошибка:' + str(e))


def buttons(callback, update):
    bot.send_message(callback.message.chat.id, "Выбери вид перевозок", reply_markup=service_buttons)


def call_back(call, update):
    print(call)
    if call.callback_query.data == "Avia":
        bot.send_message(call.message.chat.id, text="Выбери систему бронирования", reply_markup=avia_carriers_buttons)
    elif call.callback_query.data == "ZHD":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_trains_api()
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == "Bus":
        bot.send_message(call.callback_query.message.chat.id, text="Выбери перевозчика", reply_markup=bus_carriers_buttons)
    elif call.callback_query.data == "sabre":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_sabre_api(headers, test_api_url)
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == "sirena":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_sirena_api(headers, test_api_url)
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == "ims":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_ims_api(headers, test_api_url)
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == "busfor":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_busfor_api(headers, test_api_url)
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == "etraffic":
        bot.send_message(call.callback_query.message.chat.id, text="Выполняется проверка... ")
        output_data = test_etraffic_api(headers, test_api_url)
        bot.send_message(call.callback_query.message.chat.id, text=output_data)
    elif call.callback_query.data == 'week':
        msg = ranking_by_week(call)
        bot.send_message(call.callback_query.message.chat.id, text=msg, reply_markup=jira_buttons)
    elif call.callback_query.data == 'month':
        msg = ranking_by_month(call)
        bot.send_message(call.callback_query.message.chat.id, text=msg, reply_markup=jira_buttons)


def main():

    q = mq.MessageQueue()
    sender_bot = MQBot(mqueue=q, token=token, request=telegram.bot.Request(
                               proxy_url=proxy_url, con_pool_size=10))

    u = Updater(bot=sender_bot, use_context=True)
    u.start_polling()
    j = u.job_queue
    dp = u.dispatcher
    dp.add_handler(CommandHandler('long_reservation', execute_long_reserve))
    dp.add_handler((CommandHandler('integration_doubles', execute_integration_doubles)))
    dp.add_handler((CommandHandler('api_test', buttons)))
    dp.add_handler((CommandHandler('statistics', ranking)))
    dp.add_handler((CommandHandler('changed_confirm_date', changed_confirm_date)))
    dp.add_handler(CallbackQueryHandler(call_back))

    j.run_repeating(jira_issues_to_chat, interval=60, first=0, name='jira_subs')
    j.run_repeating(test_api_check, interval=600, first=0, name='api_checking')
    j.run_daily(daily_report, time=datetime.time(22, 00, 00), name='daily_report')
    j.run_daily(fin_differenses, time=datetime.time(9, 00, 00))


if __name__ == '__main__':
    main()
