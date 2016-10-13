import aiohttp
import asyncio
import csv
import datetime
import json
import logging
import re
import requests

from dateutil.relativedelta import relativedelta
from lxml import html

logger = logging.getLogger('itau.client')

ITAU_DOMAIN = 'https://www.itaulink.com.uy'


class ItauClient:

    LOGIN_URL = ITAU_DOMAIN + '/appl/servlet/FeaServlet'
    SECOND_LOGIN_URL = ITAU_DOMAIN + '/trx/loginParalelo'
    MAIN_URL = ITAU_DOMAIN + '/trx/home'
    HISTORY_ACCOUNT_URL = ITAU_DOMAIN + '/trx/cuentas/{type}/{hash}/{month}/{year}/consultaHistorica'
    CURRENT_ACCOUNT_URL = ITAU_DOMAIN + '/trx/cuentas/{type}/{hash}/mesActual'

    ACCOUNT_TYPES = {
        'savings_account': 'caja_de_ahorro',
        'transactional_account': 'cuenta_corriente',
        'collections_account': 'cuenta_recaudadora',
        'junior_savings_account': 'cuenta_de_ahorro_junior',
    }

    CURRENCIES = {
        'URGP': {
            'iso': 'UYU',
            'display': '$'
        },
        'US.D': {
            'iso': 'USD',
            'display': 'U$S'
        }
    }

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.cookies = self.login()

    def parse_accounts(self, accounts):
        accounts_json = accounts['cuentas']
        self.accounts = []
        for account_type, account_key in self.ACCOUNT_TYPES.items():
            for account_json in accounts_json.get(account_key, []):
                currency = self.CURRENCIES[account_json['moneda']]
                cleaned_account = {
                    'type': account_type,
                    'currency_id': currency['iso'],
                    'currency_display': currency['display'],
                    'id': account_json['idCuenta'],
                    'name': account_json['nombreTitular'],
                    'hash': account_json['hash'],
                    'balance': account_json['saldo'],
                    'account_type_id': account_json['tipoCuenta'],
                    'original': account_json,
                }
                self.accounts.append(cleaned_account)

    def parse_transaction(self, raw_tx):
        if raw_tx['tipo'] == 'D':
            transaction_type = 'debit'
        elif raw_tx['tipo'] == 'C':
            transaction_type = 'credit'
        else:
            logger.warning('Invalid trasaction type: {}'.format(
                raw_tx))
            return

        tx = {
            'description': ' '.join(
                raw_tx['descripcion'].split()
            ),
            'additional_description': ' '.join(
                raw_tx['descripcionAdicional'].split()
            ),
            'type': transaction_type,
            'amount': raw_tx['importe'],
            'balance': raw_tx['saldo'],
            'date': datetime.date(
                raw_tx['fecha']['year'],
                raw_tx['fecha']['monthOfYear'],
                raw_tx['fecha']['dayOfMonth']
            ),
            'meta': {}
        }

        if tx['description'].startswith('COMPRA '):
            # Debit card purchase
            tx['meta']['debit_card_purchase'] = True

        if tx['description'].startswith('RETIRO '):
            # ATM
            tx['meta']['atm'] = True
            tx['description'] = 'RETIRO BANRED'

        if tx['description'].startswith('DEBITO BANKING CARD'):
            tx['meta']['bank_costs'] = True

        if tx['description'].startswith('TRASPASO DE'):
            tx['meta']['bank_transfer'] = True
            tx['meta']['bank_transfer_from'] = self.only_num(tx['description'])

        if tx['description'].startswith('TRASPASO A'):
            tx['meta']['bank_transfer'] = True
            tx['meta']['bank_transfer_to'] = self.only_num(tx['description'])

        if tx['description'].startswith('REDIVA 1921'):
            tx['meta']['tax_return'] = True

        return tx

    def only_num(self, txt):
        return re.sub('[^0-9]', '', txt)

    def parse_transactions(self, details_json):
        transactions = []
        data = details_json['itaulink_msg']['data']
        if 'movimientosHistoricos' in data:
            movements = data['movimientosHistoricos']['movimientos']
        elif 'movimientosMesActual' in data:
            movements = data['movimientosMesActual']['movimientos']

        for raw_transaction in movements:
            tx = self.parse_transaction(raw_transaction)
            if tx:
                transactions.append(tx)

        return transactions

    async def get_month_account_details(self, account, month_date):
        today = datetime.date.today()
        if month_date.month == today.month and month_date.year == today.year:
            url = self.CURRENT_ACCOUNT_URL.format(
                type=account['account_type_id'],
                hash=account['hash'],
            )
        else:
            url = self.HISTORY_ACCOUNT_URL.format(
                type=account['account_type_id'],
                hash=account['hash'],
                month=month_date.strftime('%m'),
                year=month_date.strftime('%y'),
            )

        logger.debug('Fetching month={}-{} for {}'.format(
            month_date.year, month_date.month, account['id']
        ))

        payload = '0:{}:{}:{}-{}:'.format(
            account['original']['moneda'],
            account['hash'],
            month_date.strftime('%m'),
            month_date.strftime('%y')
        )

        try:
            payload = bytes(payload, 'utf-8')
            cookies = dict(self.cookies)
            async with aiohttp.ClientSession(cookies=cookies) as session:
                async with session.post(url, data=payload) as r:
                    trans_json = await r.json()
                    return self.parse_transactions(trans_json)
        except Exception as e:
            logger.debug('Error fetching {}. Ignoring'.format(
                month_date.isoformat()[:8]))
            return []

    def account_detail(self, account, from_date=None):
        if not from_date:
            from_date = datetime.date(2013, 5, 1)

        today = datetime.date.today()
        transactions = []

        tasks = []
        while today > from_date:
            tasks.append(self.get_month_account_details(account, today))
            today -= relativedelta(months=1)

        loop = asyncio.get_event_loop()
        monthly_transactions = loop.run_until_complete(asyncio.gather(*tasks))

        for month_transactions in monthly_transactions:
            transactions.extend(month_transactions)

        return transactions

    def save(self):
        for account in self.accounts:
            filename = '{}-{}.csv'.format(
                account['id'], account['currency_id'])
            with open(filename, 'w') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow([
                    'account', 'currency', 'date', 'description',
                    'additional_description', 'type', 'debit', 'credit',
                    'balance', 'debit card purchase', 'atm', 'bank transfer',
                    'tax return'
                ])
                for tx in account['transactions']:
                    debit = ''
                    credit = ''
                    amount = '{:.2f}'.format(tx['amount'])
                    if tx['type'] == 'debit':
                        debit = amount
                    elif tx['type'] == 'credit':
                        credit = amount

                    writer.writerow([
                        account['id'], account['currency_id'],
                        tx['date'].isoformat(), tx['description'],
                        tx['additional_description'], tx['type'],
                        debit,
                        credit,
                        tx['balance'],
                        tx['meta'].get('debit_card_purchase'),
                        tx['meta'].get('atm'), tx['meta'].get('bank_transfer'),
                        tx['meta'].get('tax_return')
                    ])

    def login(self):
        r = requests.post(self.LOGIN_URL, data={
            'segmento': 'panelPersona',
            'tipo_documento': '1',
            'nro_documento': self.username,
            'pass': self.password,
            'password': self.password,
            'id': 'login',
            'tipo_usuario': 'R',
        })

        tree = html.fromstring(r.content)
        data = {}
        for input_element in tree.xpath('//input'):
            data[input_element.name] = input_element.value

        r = requests.post(self.SECOND_LOGIN_URL, data=data)

        self.cookies = r.history[0].cookies

        accounts = json.loads(
            re.search(
                r'var mensajeUsuario = JSON.parse\(\'(.*?)\'',
                r.text.replace('\n', '')
            ).group(1))

        self.parse_accounts(accounts)

        logger.info('{} accounts found.'.format(len(self.accounts)))
        for account in self.accounts:
            logger.info('{} {} in {} - {} {:.2f}'.format(
                account['id'], account['type'], account['currency_id'],
                account['currency_display'], account['balance']))

        total_transactions = 0
        for account in self.accounts:
            account['transactions'] = sorted(
                self.account_detail(account),
                key=lambda x: x['date']
            )
            total_transactions += len(account['transactions'])

        logger.info('Downloaded {} transactions from {} accounts.'.format(
            total_transactions, len(self.accounts)))
        for account in self.accounts:
            logger.info('{} {} in {} - {} transactions'.format(
                account['id'], account['type'], account['currency_id'],
                len(account['transactions'])))
            logger.info(
                '{:10s} | {:24s} | {:30s} | {:8s} | {:8s} | {:8s}'.format(
                    'date', 'description', 'additional description', 'debit',
                    'credit', 'balance'))

            for tx in account['transactions']:
                debit = ''
                credit = ''
                amount = '{:.2f}'.format(tx['amount'])
                if tx['type'] == 'debit':
                    debit = amount
                elif tx['type'] == 'credit':
                    credit = amount

                logger.info(
                    '{:10s} | {:24s} | {:30s} | {:8s} | {:8s} | {:8s}'.format(
                        tx['date'].isoformat(), tx['description'],
                        tx['additional_description'],
                        debit,
                        credit,
                        '{:.2f}'.format(tx['balance'])))
