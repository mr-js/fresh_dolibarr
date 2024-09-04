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
            format='%(asctime)s [%(funcName)s] %(levelname)s %(message)s',
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
        self.log.info(f'{self.log_details=}, {self.demo_limit=}, {self.demo_input=}, {self.demo_output=}')
        self.sync_key_dolib = keyring.get_password('fresh_dolibar', 'sync_key_dolib')
        self.sync_key_fresh = keyring.get_password('fresh_dolibar', 'sync_key_fresh')
        self.data = {}
        self.state = {}
 

    def db_all_dumps(self):
        items = ['db_dolib_contacts', 'db_dolib_companies', 'db_fresh_contacts', 'db_fresh_companies']
        if not self.demo_input and any(filter(lambda x: not os.path.exists(os.path.join('data', x + '.json')), items)):
            if not os.path.exists('data'):
                os.mkdir('data')
            for item in items:
                with codecs.open(os.path.join('data', item + '.json'), 'w', 'utf-8') as f:
                    json.dump(eval(f'self.{item}'), f, indent=4, ensure_ascii=False)
            self.log.warning(f'Updated dumps for: {items}')


    def db_dolib_scan(self):
        src = 'dolib'
        if self.demo_input:
            try:
                items = ['db_dolib_contacts', 'db_dolib_companies']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.info('contacts readed')
                self.log.info('companies readed')
            except Exception as e:
                self.db_dolib_contacts = {}
                self.db_dolib_companies = {}                
                self.log.error(str(e).replace('\n', ' '))
        else:
            try:
                url = f'{self.sync_url_dolib}/contacts'
                headers = {
                    'Content-Type': 'application/json',
                    'DOLAPIKEY': self.sync_key_dolib
                }            
                response = requests.get(url, headers=headers)
                # self.log.debug(f'{response.text=}')
                self.db_dolib_contacts = response.json()
                self.log.info('contacts readed')
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
                self.db_dolib_companies = response.json()
                self.log.info('companies readed')
            except Exception as e:
                self.db_dolib_companies = {}
                self.log.error(str(e).replace('\n', ' '))
        self.state['dolib'] = {'total': 0, 'extracted': 0, 'passed': 0}
        for company in self.db_dolib_companies:
            self.state['dolib'].update({'total': self.state['dolib'].get('total') + 1})
            id = token_hex(4)        
            company_internal_id = company.get('id', '')
            inn = company.get('idprof2', '')
            name = company.get('name', '')
            self.log.debug(f'company {name} {inn} "{src}" #{id} scanning...')
            contacts = next(iter(list(filter(lambda x: x.get('id') == company_internal_id or x.get('socid') == company_internal_id, self.db_dolib_contacts))), None)
            if company.get('idprof2', '') and contacts:
                result = {
                    'src': src,

                    'company_name': company.get('name', ''),
                    'company_inn': company.get('idprof2', ''),
                    'company_type': '2' if company.get('fournisseur', '') == '1' else ('1' if company.get('client', '') == '1' else '0'),                    
                    'company_ogrn': company.get('idprof1', ''),
                    'company_kpp': company.get('idprof3', ''),
                    'company_okpo': company.get('idprof4', ''),
                    'company_country': company.get('country_code', ''),
                    'company_town': company.get('town', ''),
                    'company_address': company.get('address', ''),
                    'company_zip': company.get('zip', ''),
                    'company_phone': company.get('phone_1', ''),
                    'company_email': company.get('email_1', ''),
                    'company_internal_id': company_internal_id,

                    'contact_lastname': contacts.get('lastname', ''),
                    'contact_firstname': contacts.get('firstname', ''),
                    'contact_post': contacts.get('poste', ''),
                    'contact_email': contacts.get('email', ''),
                    'contact_mobile': contacts.get('phone_mobile', ''),
                    'contact_phone': contacts.get('phone_pro', ''),
                    'contact_internal_id': contacts.get('id', ''),      
                }
                self.data.update({id : result})
                self.state['dolib'].update({'extracted': self.state['dolib'].get('extracted') + 1})
                self.log.debug(f'company {name} {inn} "{src}" extracted')
            else:
                self.state['dolib'].update({'passed': self.state['dolib'].get('passed') + 1})
                self.log.debug(f'company {name} {inn} "{src}" passed (no INN or Contacts)')
            if self.demo_limit > 0 and self.state['dolib'].get('total') >= self.demo_limit:
                self.log.warning(f'Limit: {self.demo_limit} items max')
                break
    

    def db_fresh_scan(self):
        src = 'fresh'
        if self.demo_input:
            try:
                items = ['db_fresh_contacts', 'db_fresh_companies']
                for item in items:
                    with codecs.open(os.path.join('data', item + '.json'), 'r', 'utf-8') as f:
                        exec(f'self.{item} = json.load(f)')
                self.log.info('contacts readed')
                self.log.info('companies readed')
            except Exception as e:
                self.db_fresh_contacts = {}
                self.db_fresh_companies = {}          
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
                self.log.info('contacts readed')
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
                self.db_fresh_companies = response.json()
                self.log.info('companies readed')
            except Exception as e:
                self.db_fresh_companies = {}
                self.log.error(str(e).replace('\n', ' '))
        self.state['fresh'] = {'total': 0, 'extracted': 0, 'passed': 0}
        for company in self.db_fresh_companies.get('value'):
            self.state['fresh'].update({'total': self.state['fresh'].get('total') + 1})
            id = token_hex(4)
            company_internal_id, name, ref = company.get('Ref_Key', ''), company.get('НаименованиеПолное', ''), company.get('ОсновноеКонтактноеЛицо_Key', '')
            inn = company.get('ИНН', '')
            name = company.get('НаименованиеПолное', '')
            self.log.debug(f'company {name} {inn} "{src}" #{id} scanning...')
            contacts = next(iter(list(filter(lambda x: x.get('ОбъектВладелец') == company_internal_id, self.db_fresh_contacts.get('value')))), None)  
            if company.get('ИНН', '') and contacts:
                result = {
                    'src': src,

                    'company_name': company.get('НаименованиеПолное', ''),
                    'company_inn': company.get('ИНН', ''),
                    'company_type': '2' if company.get('Parent_Key', '') == "7f5cb650-639b-11ef-8f80-fa163eb4f3b4" else '1',                                    
                    'company_ogrn': company.get('РегистрационныйНомер', ''),
                    'company_kpp': company.get('КПП', ''),
                    'company_okpo': company.get('КодПоОКПО', ''),
                    'company_country': company.get('Страна', ''),
                    'company_town': company.get('Город,населённый пункт', ''),
                    'company_address': company.get('Адрес', ''),
                    'company_zip': company.get('Индекс', ''),
                    'company_phone': company.get('НомерТелефона', ''),
                    'company_email': company.get('АдресЭП', ''),
                    'company_internal_id': company_internal_id,

                    'contact_lastname': contacts.get('Фамилия', ''),
                    'contact_firstname': contacts.get('Имя', ''),
                    'contact_post': contacts.get('Должность', ''),
                    'contact_email': contacts.get('КонтактнаяИнформация')[0].get('АдресЭП', '') if contacts.get('КонтактнаяИнформация') else '',
                    'contact_mobile': contacts.get('КонтактнаяИнформация')[0].get('НомерТелефона', '') if contacts.get('КонтактнаяИнформация') else '',
                    'contact_phone': contacts.get('КонтактнаяИнформация')[0].get('НомерТелефона', '')  if contacts.get('КонтактнаяИнформация') else '',
                    'contact_internal_id': contacts.get('КонтактнаяИнформация')[0].get('Ref_Key', ''),                     
                }
                self.log.debug(f'company {name} {inn} "{src}" extracted')
                self.data.update({id : result})
                self.state['fresh'].update({'extracted': self.state['fresh'].get('extracted') + 1})
            else:
                self.state['fresh'].update({'passed': self.state['fresh'].get('passed') + 1})
                self.log.debug(f'company {name} {inn} "{src}" passed (no INN or Contacts)')
            if self.demo_limit > 0 and self.state['fresh'].get('total') >= self.demo_limit:
                self.log.warning(f'demo limit: {self.demo_limit} items max')
                break


    def db_dolib_write(self, item, company_update='', contact_update=''):
        if self.demo_output:
            self.log.warning('DEMO')
            return '!', '!'
        company_record_id = ''; contact_record_id = ''
        id = item.get('id', '')
        inn = item.get('company_inn', '')
        name = item.get('company_name', '')
        try:
            url = f'{self.sync_url_dolib}/thirdparties' if not company_update else f'{self.sync_url_dolib}/thirdparties/{company_update}'         
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
            if company_update:
                response = requests.put(url, headers=headers, json=data)
            else:
                response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200 or response.status_code == 201:
                company_record_id = response.json().get('id')
                self.log.info(f'company writed ({company_record_id})')
            else:
                raise Exception(response.text)                                  
        except Exception as e:
            # self.db_dolib_companies = {}
            self.log.warning(str(e).replace('\n', ' '))
        try:
            url = f'{self.sync_url_dolib}/contacts' if not contact_update else f'{self.sync_url_dolib}/contacts/{contact_update}'
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
                'socid': company_record_id,
                'fk_soc': company_record_id,
            }
            response = requests.post(url, headers=headers, json=data)
            if contact_update:
                response = requests.put(url, headers=headers, json=data)
            else:
                response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200 or response.status_code == 201:
                contact_record_id = response.json()
                self.log.info(f'contact writed ({contact_record_id})')
            else:
                raise Exception(response.text)
        except Exception as e:
            # self.db_dolib_contacts = {}
            self.log.warning(str(e).replace('\n', ' '))            
        return company_record_id, contact_record_id
        

    def db_fresh_write(self, item, company_update='', contact_update=''):
        if self.demo_output:
            self.log.warning('DEMO')
            return '!', '!'        
        company_record_id = ''; contact_record_id = ''
        id = item.get('id', '')
        inn = item.get('company_inn', '')
        name = item.get('company_name', '')        
        try:
            url = f'{self.sync_url_fresh}/Catalog_КонтактныеЛица(guid\'{company_update}\')' if contact_update else f'{self.sync_url_fresh}/Catalog_КонтактныеЛица'
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
            user, password = 'odata.user', self.sync_key_fresh
            response = requests.post(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
            if contact_update:
                response = requests.put(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
            else:
                response = requests.post(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
            if response.status_code == 200 or response.status_code == 201:
                contact_record_id = response.json().get('Ref_Key')
                self.log.info(f'contact writed ({contact_record_id})')
            else:
                raise Exception(response.text)
        except Exception as e:
            # self.db_fresh_contacts = {}
            self.log.warning(str(e).replace('\n', ' '))
        try:
            url = f'{self.sync_url_fresh}/Catalog_Контрагенты(guid\'{company_update}\')' if company_update else f'{self.sync_url_fresh}/Catalog_Контрагенты'
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
            user, password = 'odata.user', self.sync_key_fresh
            if company_update:
                response = requests.put(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
            else:
                response = requests.post(url, headers=headers, params=params, json=data, auth=HTTPBasicAuth(user, password))
            if response.status_code == 200 or response.status_code == 201:
                company_record_id = response.json().get('Ref_Key')
                self.log.info(f'company writed ({contact_record_id})')
            else:
                raise Exception(response.text)
        except Exception as e:
            # self.db_fresh_companies = {}
            self.log.warning(str(e).replace('\n', ' '))
        return company_record_id, contact_record_id

    
    def db_all_sync(self):
        self.state['sync'] = {'total': 0, 'fresh => dolib': 0, 'dolib => fresh': 0, 'passed': 0}
        try:
            for id, item in self.data.items():
                src = item.get('src', '')
                dst = 'dolib' if src == 'fresh' else 'fresh'
                dir = f'{src} => {dst}'
                inn = item.get('company_inn', '')
                name = item.get('company_name', '')
                self.log.debug(f'company {name} {inn} "{src}" #{id} synchronising...')
                company_duplicates = list(filter(lambda x: self.data[x].get('src', '') != item.get('src', '') and self.data[x].get('company_inn', '') == item.get('company_inn') and x != id, self.data.keys()))
                if company_duplicates:
                    self.log.warning(f'dublicates found: {company_duplicates}')
                company_update = '' if len(company_duplicates) == 0 else self.data[company_duplicates[0]].get('company_internal_id', '')
                contact_duplicates = list(filter(lambda x: self.data[x].get('src', '') != item.get('src', '') and self.data[x].get('contact_internal_id', '') == item.get('contact_internal_id') and x != id, self.data.keys()))
                if contact_duplicates:
                    self.log.warning(f'dublicates found: {contact_duplicates}')                    
                contact_update = '' if len(contact_duplicates) == 0 else self.data[contact_duplicates[0]].get('contact_internal_id', '')
                company_record_id, contact_record_id = self.db_dolib_write(item, company_update, contact_update) if dst == 'dolib' else self.db_fresh_write(item, company_update, contact_update)
                if company_record_id and contact_record_id:
                    self.state['sync'].update({dir: self.state['sync'].get(dir) + 1})
                    self.log.debug(f'company {name} {inn} "{src}" #{id} {"UPDATED" if company_update else "CREATED"} in "{dst}": {company_record_id=}, {contact_record_id=}')            
                else:
                    self.state['sync'].update({'passed': self.state['sync'].get('passed') + 1})
                    self.log.error(f'company {name} {inn} "{src}" #{id} {"UPDATED" if company_update else "CREATED"} in "{dst}" FAILED')
                self.state['sync'].update({'total': self.state['sync'].get('total') + 1})
                if not self.demo_output:
                    time.sleep(self.sync_requests_delay)
        except Exception as e:
            self.log.error(str(e).replace('\n', ' '))
        self.log.info(self.state)


if __name__ == '__main__':
    fs = FreshDolibarr()
    fs.db_dolib_scan()
    fs.db_fresh_scan()
    fs.db_all_dumps()
    fs.db_all_sync()
    