import requests
from requests.auth import HTTPBasicAuth
import logging
import codecs
import json
import os
from secrets import token_hex
import time
import tomllib
import keyring


class FreshDolibarr:
    def __init__(self):
        logging.basicConfig(
            handlers=[
                    logging.StreamHandler(),
                    logging.FileHandler('fresh_dolibar.log', 'w', 'utf-8')
                    ],
            format='%(asctime)s %(levelname)s %(message)s [%(funcName)s]',
            datefmt='%Y.%m.%d %H:%M:%S',
            level=logging.INFO
            )    
        self.log = logging.getLogger(__name__)
        try:
            with codecs.open('fresh_dolibarr.toml', 'rb') as f:
                data = tomllib.load(f)
                for k, v in data.items():
                    exec(f'self.{k} = v')
                self.log.setLevel(logging.DEBUG) if self.log_details else self.log.setLevel(logging.INFO)
        except Exception as e:
            self.log.error(f'CONFIG: {str(e)}')    
        self.log.info('STARTED')
        self.log.info(f'{self.sync_dir_fresh2dolib=}, {self.sync_dir_dolib2fresh=}')
        self.log.info(f'{self.log_details=}, {self.demo_limit=}, {self.demo_input=}, {self.demo_output=}')
        self.sync_key_dolib = keyring.get_password('fresh_dolibar', 'sync_key_dolib')
        self.sync_key_fresh = keyring.get_password('fresh_dolibar', 'sync_key_fresh')
 

    def db_all_dumps(self):
        items = ['db_dolib_contacts', 'db_dolib_thirdparties', 'db_fresh_contacts', 'db_fresh_thirdparties']
        if not self.demo_input and any(filter(lambda x: not os.path.exists(os.path.join('data', x + '.json')), items)):
            if not os.path.exists('data'):
                os.mkdir('data')
            for item in items:
                with codecs.open(os.path.join('data', item + '.json'), 'w', 'utf-8') as f:
                    json.dump(eval(f'self.{item}'), f, indent=4, ensure_ascii=False)
            self.log.warning(f'Updated dumps for: {items}')
        else:
            self.log.warning(f'Updating dumps for: {items} passed')


    def db_dolib_scan(self):
        if self.demo_input:
            try:
                items = ['db_dolib_contacts', 'db_dolib_thirdparties']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.info('DEMO contacts scan OK')
                self.log.info('DEMO thirdparties scan OK')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.db_dolib_thirdparties = {}                
                self.log.error(str(e).replace('\n', ' '))
        else:
            try:
                url = f'{self.sync_url_dolib}/contacts'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.sync_key_dolib
                }            
                response = requests.get(url, headers=headers)
                self.log.debug(f'{response.text=}')
                self.db_dolib_contacts = response.json()
                self.log.info('contacts scan OK')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.log.error(str(e).replace('\n', ' '))
            try:
                url = f'{self.sync_url_dolib}/thirdparties'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.sync_key_dolib
                }
                response = requests.get(url, headers=headers)
                self.db_dolib_thirdparties = response.json()
                self.log.info('thirdparties scan OK')
            except Exception as e:
                self.db_dolib_thirdparties = {}
                self.log.error(str(e).replace('\n', ' '))


    def db_fresh_scan(self):
        if self.demo_input:
            try:
                items = ['db_fresh_contacts', 'db_fresh_thirdparties']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.info('DEMO contacts scan OK')
                self.log.info('DEMO thirdparties scan OK')
            except Exception as e:
                self.db_fresh_contacts = {}
                self.db_fresh_thirdparties = {}          
                self.log.error(str(e).replace('\n', ' '))
        else:        
            try:
                url = f'{self.sync_url_fresh}/Catalog_КонтактныеЛица'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json'
                }
                user, password = 'odata.user', self.sync_key_fresh
                response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                self.db_fresh_contacts = response.json()
                self.log.info('contacts scan OK')
            except Exception as e:
                self.db_fresh_contacts = {}
                self.log.error(str(e).replace('\n', ' '))
            try:
                url = f'{self.sync_url_fresh}/Catalog_Контрагенты'
                headers = {
                    'Content-Type': 'application/json'
                }
                params = {
                    '$format': 'json'
                }
                user, password = 'odata.user', self.sync_key_fresh
                response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(user, password))
                self.db_fresh_thirdparties = response.json()
                self.log.info('thirdparties scan OK')
            except Exception as e:
                self.db_fresh_thirdparties = {}
                self.log.error(str(e).replace('\n', ' '))


    def db_dolib_analize(self):
        self.db_dolib_result = {}
        self.db_dolib_analize_statistics = {'processed': 0, 'added': 0, 'passed': 0}
        for thirdparty in self.db_dolib_thirdparties:
            self.db_dolib_analize_statistics.update({'processed': self.db_dolib_analize_statistics.get('processed') + 1})
            id = thirdparty.get('id', '')
            inn = thirdparty.get('idprof2', '')
            self.log.debug(f'company #{id} {thirdparty} analizing...')
            thirdparty_potential_contacts = list(filter(lambda x: x.get('id') == id or x.get('socid') == id, self.db_dolib_contacts))
            thirdparty_potential_conflicts = list(filter(lambda x: x.get('company_id') == inn, self.db_dolib_result.values()))
            thirdparty_potential_exists = list(filter(lambda x: x.get('idprof2') == inn, self.db_dolib_thirdparties))
            self.log.debug(f'{len(thirdparty_potential_contacts)=}, {len(thirdparty_potential_conflicts)=}, {len(thirdparty_potential_exists)=}')
            if thirdparty.get('idprof2', '') and len(thirdparty_potential_contacts) > 0:
                self.log.debug(f'company #{id} {thirdparty} added')
                contacts = thirdparty_potential_contacts[0]
                result = {
                    'company_id': token_hex(4),
                    'company_type': '2' if thirdparty.get('fournisseur', '') == '1' else ('1' if thirdparty.get('client', '') == '1' else '0'),
                    'company_name': thirdparty.get('name', ''),
                    'company_ogrn': thirdparty.get('idprof1', ''),
                    'company_inn': thirdparty.get('idprof2', ''),
                    'company_kpp': thirdparty.get('idprof3', ''),
                    'company_okpo': thirdparty.get('idprof4', ''),
                    'company_country': thirdparty.get('country_code', ''),
                    'company_town': thirdparty.get('town', ''),
                    'company_address': thirdparty.get('address', ''),
                    'company_zip': thirdparty.get('zip', ''),
                    'company_phone': thirdparty.get('phone_1', ''),
                    'company_email': thirdparty.get('email_1', ''),
                    'contact_lastname': contacts.get('lastname', ''),
                    'contact_firstname': contacts.get('firstname', ''),
                    'contact_post': contacts.get('poste', ''),
                    'contact_email': contacts.get('email', ''),
                    'contact_mobile': contacts.get('phone_mobile', ''),
                    'contact_phone': contacts.get('phone_pro', ''),
                }
                self.db_dolib_result.update({self.db_dolib_analize_statistics.get('processed') : result})
                self.db_dolib_analize_statistics.update({'added': self.db_dolib_analize_statistics.get('added') + 1})
                self.log.debug(f'{result=}')
            else:
                self.log.debug(f'company #{id} {thirdparty} passed')
                self.db_dolib_analize_statistics.update({'passed': self.db_dolib_analize_statistics.get('passed') + 1})
            if self.demo_limit > 0 and self.db_dolib_analize_statistics.get('processed') >= self.demo_limit:
                self.log.warning(f'DEMO Limit: {self.demo_limit} items max')
                break
        self.log.debug(f'{self.db_dolib_result=}')
        self.log.info(f'{self.db_dolib_analize_statistics=}')


    def db_fresh_analize(self):
        self.db_fresh_result = {}
        self.db_fresh_analize_statistics = {'processed': 0, 'added': 0, 'passed': 0}
        for thirdparty in self.db_fresh_thirdparties.get('value'):
            self.db_fresh_analize_statistics.update({'processed': self.db_fresh_analize_statistics.get('processed') + 1})
            id, name, ref = thirdparty.get('Ref_Key', ''), thirdparty.get('НаименованиеПолное', ''), thirdparty.get('ОсновноеКонтактноеЛицо_Key', '')
            inn = thirdparty.get('ИНН', '')
            self.log.debug(f'company #{id} {thirdparty} analizing...')
            thirdparty_potential_contacts = list(filter(lambda x: x.get('ОбъектВладелец') == id, self.db_fresh_contacts.get('value')))
            thirdparty_potential_conflicts = list(filter(lambda x: x.get('company_id') == inn, self.db_fresh_result.values()))
            thirdparty_potential_exists = list(filter(lambda x: x.get('ИНН') == inn, self.db_fresh_thirdparties.get('value')))
            self.log.debug(f'{len(thirdparty_potential_contacts)=}, {len(thirdparty_potential_conflicts)=}, {len(thirdparty_potential_exists)=}')            
            if thirdparty.get('ИНН', '') and len(thirdparty_potential_contacts) > 0:
                self.log.debug(f'company #{id} {thirdparty} added')
                contacts = thirdparty_potential_contacts[0]
                result = {
                    'company_id': token_hex(4),
                    'company_type': '2' if thirdparty.get('Parent_Key', '') == "7f5cb650-639b-11ef-8f80-fa163eb4f3b4" else '1',
                    'company_name': thirdparty.get('НаименованиеПолное', ''),
                    'company_ogrn': thirdparty.get('РегистрационныйНомер', ''),
                    'company_inn': thirdparty.get('ИНН', ''),
                    'company_kpp': thirdparty.get('КПП', ''),
                    'company_okpo': thirdparty.get('КодПоОКПО', ''),
                    'company_country': thirdparty.get('Страна', ''),
                    'company_town': thirdparty.get('Город,населённый пункт', ''),
                    'company_address': thirdparty.get('Адрес', ''),
                    'company_zip': thirdparty.get('Индекс', ''),
                    'company_phone': thirdparty.get('НомерТелефона', ''),
                    'company_email': thirdparty.get('АдресЭП', ''),
                    'contact_lastname': contacts.get('Фамилия', ''),
                    'contact_firstname': contacts.get('Имя', ''),
                    'contact_post': contacts.get('Должность', ''),
                    'contact_email': contacts.get('КонтактнаяИнформация')[0].get('АдресЭП', '') if contacts.get('КонтактнаяИнформация') else '',
                    'contact_mobile': contacts.get('КонтактнаяИнформация')[0].get('НомерТелефона', '') if contacts.get('КонтактнаяИнформация') else '',
                    'contact_phone': contacts.get('КонтактнаяИнформация')[0].get('НомерТелефона', '')  if contacts.get('КонтактнаяИнформация') else '',
                }
                self.db_fresh_result.update({self.db_fresh_analize_statistics.get('processed') : result})
                self.db_fresh_analize_statistics.update({'added': self.db_fresh_analize_statistics.get('added') + 1})
                self.log.debug(f'{result=}')
            else:
                self.log.debug(f'company #{id} {thirdparty} passed')
                self.db_fresh_analize_statistics.update({'passed': self.db_fresh_analize_statistics.get('passed') + 1})
            if self.demo_limit > 0 and self.db_fresh_analize_statistics.get('processed') >= self.demo_limit:
                self.log.warning(f'DEMO Limit: {self.demo_limit} items max')
                break
        self.log.debug(f'{self.db_fresh_result=}')
        self.log.info(f'{self.db_fresh_analize_statistics=}')


    def db_dolib_add(self, item):
        thirdparty_record_id = ''; contact_record_id = ''
        id = item.get('company_id', '')
        inn = item.get('company_inn', '')
        try:
            url = f'{self.sync_url_dolib}/thirdparties'
            headers = {
                'Content-Type': 'application/json',
                'DOLAPIKEY': self.sync_key_dolib
            }
            data = {
                'name': item.get('company_name', ''),
                'idprof1': item.get('company_ogrn', ''),
                'idprof2': item.get('company_inn', ''),
                'idprof3': item.get('company_kpp', ''),
                'idprof4': item.get('company_okpo', ''),
                'country_code': item.get('company_country', ''),
                'town': item.get('company_town', ''),
                'address': item.get('company_address', ''),
                'zip': item.get('company_zip', ''),
                'phone_1': item.get('company_phone', ''),
                'email_1': item.get('company_email', ''),        
                'client': '1' if (item.get('company_type', '') == '1' or item.get('company_type', '') == '0') else '0',
                'fournisseur': '1' if (item.get('company_type', '') == '2'  or item.get('company_type', '') == '0') else '0',
                'country_id': '19',
            }   
            if self.demo_output:
                thirdparty_record_id = '0'
                self.log.debug(f'DEMO thirdparty #{thirdparty_record_id} {data} added OK')
            else:
                response = requests.post(url, headers=headers, json=data)
                if response.status_code == 200 or response.status_code == 201:
                    thirdparty_record_id = response.text
                    self.log.info(f'thirdparty #{thirdparty_record_id} {data} added OK')                  
        except Exception as e:
            self.db_dolib_thirdparties = {}
            self.log.error(str(e).replace('\n', ' '))
        try:
            url = f'{self.sync_url_dolib}/contacts'
            headers = {
                'Content-Type': 'application/json',
                'DOLAPIKEY': self.sync_key_dolib
            }
            data = {
                'lastname': item.get('contact_lastname', ''),
                'firstname': item.get('contact_firstname', ''),
                'poste': item.get('contact_post', ''),
                'email': item.get('contact_email', ''),
                'phone_mobile': item.get('contact_mobile', ''),
                'phone_pro': item.get('contact_phone', ''),
                'socname': item.get('company_name', ''),   
                'socid': thirdparty_record_id,
                'fk_soc': thirdparty_record_id,
            }
            if self.demo_output:
                contact_record_id = '0'
                self.log.debug(f'DEMO contact #{contact_record_id} {data} added OK')
            else:
                response = requests.post(url, headers=headers, json=data)
                if response.status_code == 200 or response.status_code == 201:
                    contact_record_id = response.text
                    self.log.info(f'contact #{contact_record_id} {data} added OK')
                else:
                    raise Exception(response.text)
        except Exception as e:
            self.db_dolib_contacts = {}
            self.log.error(str(e).replace('\n', ' '))            
        return thirdparty_record_id, contact_record_id
        

    def db_fresh_add(self, item):
        thirdparty_record_id = ''; contact_record_id = ''
        id = item.get('company_id', '')
        inn = item.get('company_inn', '')
        try:
            url = f'{self.sync_url_fresh}/Catalog_КонтактныеЛица'
            headers = {
                'Content-Type': 'application/json'
            }
            params = {
                '$format': 'json'
            }                
            data = {
                'Фамилия': item.get('contact_lastname', ''),
                'Имя Отчество': item.get('contact_firstname', ''),
                'Должность': item.get('contact_post', ''),
                'Email': item.get('contact_email', ''),
                'Телефон мобильный': item.get('contact_mobile', ''),
                'Телефон рабочий': item.get('contact_phone', ''),                         
            }
            if self.demo_output:
                contact_record_id = '0'
                self.log.debug(f'DEMO contact #{contact_record_id} {data} added OK')
            else:            
                user, password = 'odata.user', self.sync_key_fresh
                response = requests.post(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
                if response.status_code == 200 or response.status_code == 201:
                    contact_record_id = response.json().get('Ref_Key')
                    self.log.info(f'contact #{contact_record_id} {data} added OK')
                else:
                    raise Exception(response.text)
        except Exception as e:
            self.db_fresh_contacts = {}
            self.log.error(str(e).replace('\n', ' '))
        try:
            url = f'{self.sync_url_fresh}/Catalog_Контрагенты'
            headers = {
                'Content-Type': 'application/json'
            }
            params = {
                '$format': 'json'
            }                
            data = {
                'НаименованиеПолное': item.get('company_name', ''),
                'Description': item.get('company_name', ''),
                'РегистрационныйНомер': item.get('company_ogrn', ''),
                'ИНН': item.get('company_inn', ''),
                'КПП': item.get('company_kpp', ''),
                'КодПоОКПО': item.get('company_okpo', ''),
                'Страна': item.get('company_country', ''),
                'Город,населённый пункт': item.get('company_town', ''),
                'Адрес': item.get('company_address', ''),
                'Индекс': item.get('company_zip', ''),
                'НомерТелефона': item.get('company_phone', ''),
                'Email': item.get('company_email', ''),
                'ОсновноеКонтактноеЛицо_Key': contact_record_id         
            }
            if self.demo_output:
                thirdparty_record_id = '0'
                self.log.debug(f'DEMO thirdparty #{thirdparty_record_id} {data} added OK')
            else:            
                user, password = 'odata.user', self.sync_key_fresh
                response = requests.post(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
                if response.status_code == 200 or response.status_code == 201:
                    thirdparty_record_id = response.json().get('Ref_Key')
                    self.log.info(f'thirdparty #{thirdparty_record_id} {data} added OK')
                else:
                    raise Exception(response.text)
        except Exception as e:
            self.db_fresh_thirdparties = {}
            self.log.error(str(e).replace('\n', ' '))
        return thirdparty_record_id, contact_record_id

    
    def db_all_sync(self):
        self.db_all_sync_statistics = {'processed': 0, 'added db_fresh => db_dolib': 0, 'added db_dolib => db_fresh': 0, 'passed': 0}
        if self.sync_dir_fresh2dolib:
            try:
                self.log.debug('sync fresh => dolib ...')
                for item in self.db_fresh_result.values():
                    id = item.get('company_id', '')
                    inn = item.get('company_inn', '')
                    self.log.debug(f'company #{id} {item} analizing...')                
                    item_potential = list(filter(lambda x: x[1].get('company_inn') == item.get('company_inn'), self.db_dolib_result.items()))
                    if len(item_potential) == 0:
                        self.db_dolib_add(item)
                        self.log.debug('added db_fresh => db_dolib')
                        self.db_all_sync_statistics.update({'added db_fresh => db_dolib': self.db_all_sync_statistics.get('added db_fresh => db_dolib') + 1})
                    else:
                        self.log.debug('passed (found in db_dolib)')
                        self.db_all_sync_statistics.update({'passed': self.db_all_sync_statistics.get('passed') + 1})
                    self.db_all_sync_statistics.update({'processed': self.db_all_sync_statistics.get('processed') + 1})
                    if not self.demo_output:
                        time.sleep(self.sync_requests_delay)
            except Exception as e:
                self.log.error(str(e).replace('\n', ' '))
        if self.sync_dir_dolib2fresh:                
            try:                                
                self.log.debug('sync dolib => fresh ...')
                for item in self.db_dolib_result.values():
                    id = item.get('company_id', '')
                    inn = item.get('company_inn', '')
                    self.log.debug(f'company #{id} {item} analizing...')
                    item_potential = list(filter(lambda x: x[1].get('company_inn') == item.get('company_inn'), self.db_fresh_result.items()))
                    if len(item_potential) == 0:
                        self.db_fresh_add(item)
                        self.log.debug('added db_dolib => db_fresh')
                        self.db_all_sync_statistics.update({'added db_dolib => db_fresh': self.db_all_sync_statistics.get('added db_dolib => db_fresh') + 1})
                    else:
                        self.log.debug('passed (found in db_fresh)')
                        self.db_all_sync_statistics.update({'passed': self.db_all_sync_statistics.get('passed') + 1})
                    self.db_all_sync_statistics.update({'processed': self.db_all_sync_statistics.get('processed') + 1})
                    if not self.demo_output:
                        time.sleep(self.sync_requests_delay)
            except Exception as e:
                self.log.error(str(e).replace('\n', ' '))
        self.log.info(f'{self.db_all_sync_statistics=}')


if __name__ == '__main__':
    fs = FreshDolibarr()
    fs.db_dolib_scan()
    fs.db_fresh_scan()
    fs.db_all_dumps()
    fs.db_dolib_analize()
    fs.db_fresh_analize()
    fs.db_all_sync()
    