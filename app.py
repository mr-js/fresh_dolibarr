import requests
from requests.auth import HTTPBasicAuth
import logging
import codecs
import json
import os


logging.basicConfig(
    handlers=[
            logging.StreamHandler(),
            logging.FileHandler('fresh_dolibar.log', 'w', 'utf-8')
            ],
    format='%(asctime)s %(levelname)s %(message)s [%(funcName)s]',
    datefmt='%Y.%m.%d %H:%M:%S',
    level=logging.DEBUG
    )


class FreshDolibarr:
    demo = True
    demo_items_limit = 10
    db_dolib_key = 'y8ekqpBnZ7G781Jv0ZK2aXt2P1YOT5rs'
    db_fresh_key = '6vH4RYbZvNTkE75M2WsviF1E80y7jks8'
    log = logging.getLogger(__name__)


    def __init__(self, demo=False):
        self.log.info('STARTED')
        self.demo = demo
        if self.demo:
            self.log.warning(f'DEMO mode ON (network: offline, limit: {self.demo_items_limit} items max)')


    def db_demo_dump(self):
        return None
        items = ['db_dolib_contacts', 'db_dolib_thirdparties', 'db_fresh_contacts', 'db_fresh_thirdparties']
        if not os.path.exists('data'):
            os.mkdir('data')
        for item in items:
            with codecs.open(os.path.join('data', item + '.json'), 'w', 'utf-8') as f:
                json.dump(eval(f'self.{item}'), f, indent=4, ensure_ascii=False)


    def db_dolib_scan(self):
        if self.demo:
            try:
                self.log.warning('DEMO')
                items = ['db_dolib_contacts', 'db_dolib_thirdparties']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.debug('contacts OK')
                self.log.debug('thirdparties scan OK')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.db_dolib_thirdparties = {}                
                self.log.error(str(e))
        else:
            try:
                url = 'https://crm.himopt71.ru/api/index.php/contacts'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.db_dolib_key
                }            
                response = requests.get(url, headers=headers)
                self.db_dolib_contacts = response.json()
                self.log.debug('contacts OK')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.log.error(str(e))
            try:
                url = 'https://crm.himopt71.ru/api/index.php/thirdparties'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.db_dolib_key
                }
                response = requests.get(url, headers=headers)
                self.db_dolib_thirdparties = response.json()
                self.log.debug('thirdparties scan OK')
            except Exception as e:
                self.db_dolib_thirdparties = {}
                self.log.error(str(e))


    def db_dolib_analize(self):
        self.db_dolib_result = {}
        self.db_dolib_statistics = {'processed': 0, 'added': 0, 'passed': 0}
        for thirdparty in self.db_dolib_thirdparties:
            self.db_dolib_statistics.update({'processed': self.db_dolib_statistics.get('processed') + 1})
            id = thirdparty.get('id', '')
            self.log.debug(f'#{id} analizing...')
            thirdparty_potential_contacts = list(filter(lambda x: x.get('id') == id or x.get('socid') == id, self.db_dolib_contacts))
            if len(thirdparty_potential_contacts) > 0:
                self.log.debug(f'#{id} added')
                contacts = thirdparty_potential_contacts[0]
                result = {'company': thirdparty.get('name', ''), 'name': contacts.get('firstname', '') + ' ' + contacts.get('lastname', '')}
                self.db_dolib_result.update({self.db_dolib_statistics.get('processed') : result})
                self.db_dolib_statistics.update({'added': self.db_dolib_statistics.get('added') + 1})
            else:
                self.log.debug(f'#{id} passed')
                self.db_dolib_statistics.update({'passed': self.db_dolib_statistics.get('passed') + 1})
            if self.demo and self.db_dolib_statistics.get('processed') >= self.demo_items_limit:
                self.log.warning('DEMO Limit')
                break
        self.log.debug(f'{self.db_dolib_result=}')
        self.log.info(f'{self.db_dolib_statistics=}')
        

    def db_dolib_add(self, item):
        if self.demo:        
            self.log.warning(f'DEMO {item} write emulated')
        else:
            try:
                url = 'https://crm.himopt71.ru/api/index.php/contacts'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.db_dolib_key
                }
                params = {
                    'firstname': item.get('name').split(' ')[0],
                    'lastname': item.get('name').split(' ')[1],
                }
                response = requests.post(url, headers=headers)
                contact_record_id = int(response.text)
                self.log.debug(f'contact #{contact_record_id} added OK')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.log.error(str(e))            
            try:
                url = 'https://crm.himopt71.ru/api/index.php/thirdparties'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.db_dolib_key
                }
                params = {
                    'name': item.get('company'),
                    'socid': contact_record_id
                }
                response = requests.post(url, headers=headers, params=params)
                thirdparty_record_id = int(response.text)
                self.log.debug(f'thirdparty #{thirdparty_record_id} added OK')
            except Exception as e:
                self.db_dolib_thirdparties = {}
                self.log.error(str(e))


    def db_fresh_scan(self):
        if self.demo:
            try:
                self.log.warning('DEMO')
                items = ['db_fresh_contacts', 'db_fresh_thirdparties']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.debug('contacts OK')
                self.log.debug('thirdparties OK')
            except Exception as e:
                self.db_fresh_contacts = {}
                self.db_fresh_thirdparties = {}                
                self.log.error(str(e))
        else:        
            try:
                url = 'https://1cfresh.com/a/ea/2761709/ru/odata/standard.odata/Catalog_КонтактныеЛица'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json'
                }
                user, password = 'odata.user', self.db_fresh_key
                response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                self.db_fresh_contacts = response.json()
                self.log.debug('contacts scan OK')
            except Exception as e:
                self.db_fresh_contacts = {}
                self.log.error(str(e))
            try:
                url = 'https://1cfresh.com/a/ea/2761709/ru/odata/standard.odata/Catalog_Контрагенты'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json'
                }
                user, password = 'odata.user', self.db_fresh_key
                response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                self.db_fresh_thirdparties = response.json()
                self.log.debug('thirdparties scan OK')
            except Exception as e:
                self.db_fresh_thirdparties = {}
                self.log.error(str(e))


    def db_fresh_analize(self):
        self.db_fresh_result = {}
        self.db_fresh_statistics = {'processed': 0, 'added': 0, 'passed': 0}
        for thirdparty in self.db_fresh_thirdparties.get('value'):
            self.db_fresh_statistics.update({'processed': self.db_fresh_statistics.get('processed') + 1})
            id, name, ref = thirdparty.get('Ref_Key', ''), thirdparty.get('НаименованиеПолное', ''), thirdparty.get('ОсновноеКонтактноеЛицо_Key', '')
            self.log.debug(f'#{id} analizing...')
            thirdparty_potential_contacts = list(filter(lambda x: x.get('ОбъектВладелец') == id, self.db_fresh_contacts.get('value')))
            if len(thirdparty_potential_contacts) > 0:
                self.log.debug(f'#{id} added')
                contacts = thirdparty_potential_contacts[0]
                result = {'company': thirdparty.get('НаименованиеПолное', ''), 'name': contacts.get('Имя', '') + ' ' + contacts.get('Фамилия', '')}
                self.db_fresh_result.update({self.db_fresh_statistics.get('processed') : result})
                self.db_fresh_statistics.update({'added': self.db_fresh_statistics.get('added') + 1})
            else:
                self.log.debug(f'#{id} passed')
                self.db_fresh_statistics.update({'passed': self.db_fresh_statistics.get('passed') + 1})
            if self.demo and self.db_fresh_statistics.get('processed') >= self.demo_items_limit:
                self.log.warning('DEMO Limit')
                break
        self.log.debug(f'{self.db_fresh_result=}')
        self.log.info(f'{self.db_fresh_statistics=}')


    def db_fresh_add(self, item):
        if self.demo:        
            self.log.warning(f'DEMO {item} write emulated')
        else:
            try:
                url = 'https://1cfresh.com/a/ea/2761709/ru/odata/standard.odata/Catalog_КонтактныеЛица'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json',
                    'Имя': item.get('name').split(' ')[0],
                    'Фамилия': item.get('name').split(' ')[1],                              
                }
                user, password = 'odata.user', self.db_fresh_key
                response = requests.post(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                contact_record_id = response.json().get('Ref_Key')
                self.log.debug(f'contact #{contact_record_id} added OK')                
                
            except Exception as e:
                self.db_fresh_contacts = {}
                self.log.error(str(e))
            try:
                url = 'https://1cfresh.com/a/ea/2761709/ru/odata/standard.odata/Catalog_Контрагенты'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json',
                    'НаименованиеПолное': item.get('company'),
                    'ОсновноеКонтактноеЛицо_Key': contact_record_id                 
                }
                user, password = 'odata.user', self.db_fresh_key
                response = requests.post(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                thirdparty_record_id = response.json().get('Ref_Key')
                self.log.debug(f'thirdparty #{thirdparty_record_id} added OK')
            except Exception as e:
                self.db_fresh_thirdparties = {}
                self.log.error(str(e))

    
    def db_all_sync(self):
        self.db_all_sync_statistics = {'processed': 0, 'added db_fresh => db_dolib': 0, 'added db_dolib => db_fresh': 0, 'passed': 0}
        if self.demo:
            self.log.warning('DEMO db_all_sync')
        try:
            self.log.info('sync fresh => dolib ...')
            for db_fresh_item_key, db_fresh_item_value in self.db_fresh_result.items():
                self.log.debug(f'''Company "{db_fresh_item_value.get('company')}" analizing...''')
                db_dolib_item_potential = list(filter(lambda x: x[1].get('company') == db_fresh_item_value.get('company'), self.db_dolib_result.items()))
                if len(db_dolib_item_potential) == 0:
                    self.db_dolib_add(db_fresh_item_value)
                    self.log.debug('added db_fresh => db_dolib (not found in db_dolib)')
                    self.db_all_sync_statistics.update({'added db_fresh => db_dolib': self.db_all_sync_statistics.get('added db_fresh => db_dolib') + 1})
                else:
                    self.log.debug('passed (found in db_dolib)')
                    self.db_all_sync_statistics.update({'passed': self.db_all_sync_statistics.get('passed') + 1})
                self.db_all_sync_statistics.update({'processed': self.db_all_sync_statistics.get('processed') + 1})
            self.log.info('sync dolib => fresh ...')
            for db_dolib_item_key, db_dolib_item_value in self.db_dolib_result.items():
                self.log.debug(f'''Company "{db_dolib_item_value.get('company')}" analizing...''')
                db_fresh_item_potential = list(filter(lambda x: x[1].get('company') == db_dolib_item_value.get('company'), self.db_fresh_result.items()))
                if len(db_fresh_item_potential) == 0:
                    self.db_fresh_add(db_dolib_item_value)
                    self.log.debug('added db_dolib => db_fresh (not found in db_fresh)')
                    self.db_all_sync_statistics.update({'added db_dolib => db_fresh': self.db_all_sync_statistics.get('added db_dolib => db_fresh') + 1})
                else:
                    self.log.debug('passed (found in db_fresh)')
                    self.db_all_sync_statistics.update({'passed': self.db_all_sync_statistics.get('passed') + 1})
                self.db_all_sync_statistics.update({'processed': self.db_all_sync_statistics.get('processed') + 1})
        except Exception as e:
            self.log.error(str(e))
        self.log.info(self.db_all_sync_statistics)


if __name__ == '__main__':
    fs = FreshDolibarr(demo=True)
    fs.db_dolib_scan()
    fs.db_dolib_analize()
    fs.db_fresh_scan()
    fs.db_fresh_analize()
    fs.db_all_sync()
    