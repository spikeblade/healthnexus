from datetime import datetime 
from models.base import Session, engine, Base, enviroment, variables
from models.hco_file import HCOFile
from models.hco_status import Status
from models.online_presence import OnlinePresence
from models.online_presence_source import OnlinePresenceSource
from models.hco_id import HCOid
from models.phone_link import PhoneLinks
from models.phone_enum import PhoneEnum
from models.phone_link_source import PhoneLinksSource
from sqlalchemy import and_, or_
from models.adresses import Adress
from models.adresses_enum import AdressEnum
from models.adresseslinks import AdressLink
from models.adress_link_source import AdressLinksSource
import pika
import time
import requests
import functools
import logging
import threading
#get varibles from json file
import json

queue = variables['enviroments'][enviroment]['queue']
user = variables['globals']['rabbit_user']
password = variables['globals']['rabbit_pass']
token = variables['globals']['token']
auth_id = variables['globals']['auth_id']
source_id = variables['enviroments'][enviroment]['source_id']
hco_pl_id = 0
today = datetime.now()
formated_date = today.strftime("%Y-%m-%d")
formated_date_time = today.strftime("%Y-%m-%d %H:%M:%S")
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

#function for get the adress normalization
def get_adress_form(street,last_line):
    url = 'https://us-street.api.smartystreets.com/street-address'
    fmt1 = 'street: {} last line: {} Url: {}'
    LOGGER.info('Call smartystreets api')
    LOGGER.info(fmt1.format(street,last_line,url))
    # defining a params json for the parameters to be sent to the API
    PARAMS = {'auth-id': auth_id,'auth-token': token,'street': street, 'lastline': last_line}
    # sending get request and saving the response as response object
    answer = requests.get(url=url, params=PARAMS)
    # extracting data in json format
    data = answer.json()
    LOGGER.info(data)
    return data

def ack_message(channel, delivery_tag):
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        fmt1 = 'channel: {} Delivery tag: {} Message body: {}'
        message = "Channel is already closed, so we can't ACK this message"
        LOGGER.exception(fmt1.format(channel,delivery_tag,message))
        sessionDb = Session() 
        newErrorStatus = Status(hco_pl_id,9,1,'channel close before finish process data',formated_date_time,formated_date_time)
        sessionDb.add(newErrorStatus)
        sessionDb.commit()
        sessionDb.close()

def do_work(connection, channel, delivery_tag, body):
    thread_id = threading.get_ident()
    fmt1 = 'Thread id: {} Delivery tag: {} Message body: {}'
    error1 = 'Spider: {} Type: {} Message error: {}'
    LOGGER.info(fmt1.format(thread_id, delivery_tag, body))
    broker_queue = json.loads(body) 
    spider = broker_queue['spider']
    hco_pl_id = broker_queue['job_id']
    spider_data = broker_queue['responses']
    try:
        if spider == 'FullHtmlSpider':
            LOGGER.info('Indexing html files request')
            sessionDb = Session()
            if spider_data[0]['status'] == 'success':
                #create new index
                newIndex = HCOFile(hco_pl_id,spider_data[0]['data']['path'],broker_queue['number_of_items'],formated_date_time,formated_date_time)
                #create status for know where the procces is
                newStatus = Status(hco_pl_id,3,0,None,formated_date_time,formated_date_time)
                #add new data to the db session and save
                sessionDb.add(newIndex)
                sessionDb.add(newStatus)
            else:
                LOGGER.error(error1.format(spider,'Indexing','Error in crawler getting html files'))                
                #create new error status 
                newErrorStatus = Status(hco_pl_id,7,1,'Error in crawler getting html files',formated_date_time,formated_date_time)
                sessionDb.add(newErrorStatus)
            sessionDb.commit()
            sessionDb.close()
        elif spider == 'InfoOfflineSpider':
            LOGGER.info('Saving crawler data to db')
            if spider_data[0]['status'] == 'success':
                #create presence
                faceurls = spider_data[0]['data']['status_fb']
                twiterurls = spider_data[0]['data']['status_tw']
                instargramUrls = spider_data[0]['data']['status_instagram']
                youtubeUrls = spider_data[0]['data']['status_youtube']
                phones = spider_data[0]['data']['status_phones']['found_phones']
                linkinurls = spider_data[0]['data']['status_linkedin']
                adresses_list = spider_data[0]['data']['possible_address']
                sessionDb = Session()
                LOGGER.info('Process facebook urls')
                for url in faceurls:
                    existsPresence = sessionDb.query(OnlinePresence,OnlinePresenceSource) \
                    .join(OnlinePresenceSource, OnlinePresence.ID==OnlinePresenceSource.HCO_ONLINE_PRESENCE_ID) \
                    .filter(and_(OnlinePresence.HCO_PL_ID==hco_pl_id,OnlinePresence.ONLINE_PRESENCE_URL==url[0],OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD=='05')).all()
                    if len(existsPresence) == 0: 
                        LOGGER.info('Saving facebook new online presence')
                        newPresence = OnlinePresence(hco_pl_id,2,url[0],"05",formated_date,formated_date)
                        newSource = OnlinePresenceSource(newPresence,'1337',formated_date,formated_date)
                        sessionDb.add(newPresence)
                        sessionDb.add(newSource) 
                    else:
                        LOGGER.info('Updating facebook online presence source')
                        setattr(existsPresence[0].OnlinePresenceSource, 'LAST_SEEN_DT', formated_date) 
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session() 
                LOGGER.info('Process twiter urls')
                for url in twiterurls:
                    existsPresence = sessionDb.query(OnlinePresence,OnlinePresenceSource) \
                    .join(OnlinePresenceSource, OnlinePresence.ID==OnlinePresenceSource.HCO_ONLINE_PRESENCE_ID) \
                    .filter(and_(OnlinePresence.HCO_PL_ID==hco_pl_id,OnlinePresence.ONLINE_PRESENCE_URL==url[0],OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD=='07')).all()
                    if len(existsPresence) == 0: 
                        LOGGER.info('Saving twiter new online presence')
                        newPresence = OnlinePresence(hco_pl_id,2,url[0],"07",formated_date,formated_date)
                        newSource = OnlinePresenceSource(newPresence,'1337',formated_date,formated_date)
                        sessionDb.add(newPresence)
                        sessionDb.add(newSource)
                    else:
                        LOGGER.info('Updating twiter online presence source')
                        setattr(existsPresence[0].OnlinePresenceSource, 'LAST_SEEN_DT', formated_date) 
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session() 
                LOGGER.info('Process instagram urls')
                for url in instargramUrls:
                    existsPresence = sessionDb.query(OnlinePresence,OnlinePresenceSource) \
                    .join(OnlinePresenceSource, OnlinePresence.ID==OnlinePresenceSource.HCO_ONLINE_PRESENCE_ID) \
                    .filter(and_(OnlinePresence.HCO_PL_ID==hco_pl_id,OnlinePresence.ONLINE_PRESENCE_URL==url[0],OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD=='06')).all()
                    if len(existsPresence) == 0:
                        LOGGER.info('Saving instagram new online presence')  
                        newPresence = OnlinePresence(hco_pl_id,2,url[0],"06",formated_date,formated_date)
                        newSource = OnlinePresenceSource(newPresence,'1337',formated_date,formated_date)
                        sessionDb.add(newPresence)
                        sessionDb.add(newSource)
                    else:
                        LOGGER.info('Updating instagram online presence source')
                        setattr(existsPresence[0].OnlinePresenceSource, 'LAST_SEEN_DT', formated_date) 
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session() 
                LOGGER.info('Process youtube urls')
                for url in youtubeUrls:
                    existsPresence = sessionDb.query(OnlinePresence,OnlinePresenceSource) \
                    .join(OnlinePresenceSource, OnlinePresence.ID==OnlinePresenceSource.HCO_ONLINE_PRESENCE_ID) \
                    .filter(and_(OnlinePresence.HCO_PL_ID==hco_pl_id,OnlinePresence.ONLINE_PRESENCE_URL==url[0],OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD=='08')).all()
                    if len(existsPresence) == 0: 
                        LOGGER.info('Saving youtube new online presence')  
                        newPresence = OnlinePresence(hco_pl_id,2,url[0],"08",formated_date,formated_date)
                        newSource = OnlinePresenceSource(newPresence,'1337',formated_date,formated_date)
                        sessionDb.add(newPresence)
                        sessionDb.add(newSource)
                    else:
                        LOGGER.info('Updating youtube online presence source')
                        setattr(existsPresence[0].OnlinePresenceSource, 'LAST_SEEN_DT', formated_date)
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session() 
                LOGGER.info('Process linkin urls')
                for url in linkinurls:
                    existsPresence = sessionDb.query(OnlinePresence,OnlinePresenceSource) \
                    .join(OnlinePresenceSource, OnlinePresence.ID==OnlinePresenceSource.HCO_ONLINE_PRESENCE_ID) \
                    .filter(and_(OnlinePresence.HCO_PL_ID==hco_pl_id,OnlinePresence.ONLINE_PRESENCE_URL==url[0],OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD=='04')).all()
                    if len(existsPresence) == 0: 
                        LOGGER.info('Saving linkin new online presence')  
                        newPresence = OnlinePresence(hco_pl_id,2,url[0],"04",formated_date,formated_date)
                        newSource = OnlinePresenceSource(newPresence,'1337',formated_date,formated_date)
                        sessionDb.add(newPresence)
                        sessionDb.add(newSource)   
                    else:
                        LOGGER.info('Updating linkin online presence source')
                        setattr(existsPresence[0].OnlinePresenceSource, 'LAST_SEEN_DT', formated_date) 
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session()  
                LOGGER.info('Process phones finded by crawler')
                for phone in phones:
                    LOGGER.info('Formating phone')
                    phoneNumberRes = str(int("".join(filter(str.isdigit, phone[0]))))
                    existsNumber = sessionDb.query(PhoneEnum,PhoneLinksSource) \
                    .join(PhoneLinks, PhoneEnum.PHONE_PL_ID==PhoneLinks.PHONE_PL_ID) \
                    .join(PhoneLinksSource, PhoneLinks.ID==PhoneLinksSource.PHONE_LINK_ID) \
                    .filter(and_(PhoneLinks.HCO_PL_ID==hco_pl_id,PhoneEnum.PHONE_NUMBER==phoneNumberRes)).all()
                    if len(existsNumber) == 0: 
                        LOGGER.info('Saving new phone')  
                        newPhoneEnum = PhoneEnum(phoneNumberRes,1,1,formated_date,formated_date)
                        LOGGER.info('New phone enum created')  
                        newPhoneLink = PhoneLinks(newPhoneEnum,hco_pl_id,1,formated_date,formated_date)
                        LOGGER.info('New phone link created')  
                        newPhoneLinkSource = PhoneLinksSource(newPhoneLink,'1337',formated_date,formated_date)
                        sessionDb.add(newPhoneEnum)
                        sessionDb.add(newPhoneLink)
                        sessionDb.add(newPhoneLinkSource)
                    else:
                        LOGGER.info('Updating phone source')
                        setattr(existsNumber[0].PhoneLinksSource, 'LAST_SEEN_DT', formated_date)  
                        #create status for know where the procces is
                    sessionDb.commit()
                sessionDb.close()
                sessionDb = Session() 
                LOGGER.info('Process addresses finded by crawler')
                for adress in adresses_list:
                    street = adress['address']
                    last_line = adress['address_last_line']
                    adress_data = get_adress_form(street,last_line)
                    adress_data = adress_data[0]
                    temp_adress = "{}{}{}{}{}".format(adress_data.get('delivery_line_1',''), adress_data.get('delivery_line_2',''),adress_data.get('components', {}).get('city_name',''),
                    adress_data.get('components', {}).get('state_abbreviation',''),adress_data.get('components', {}).get('zipcode','')).upper()
                    existsAdress = sessionDb.query(AdressEnum) \
                    .filter(AdressEnum.ADDR_KEY_STAND==temp_adress).all()
                    if len(existsAdress) == 0:
                        LOGGER.info('Creating new location enum')
                        tempDpvNotes = adress_data.get('analysis', {}).get('dpv_footnotes','') 
                        tempvacant = adress_data.get('analysis', {}).get('dpv_vacant','') 
                        dpvStat = ''
                        if tempDpvNotes == '' :
                            dpvStat = 'UNKNOWN'
                        elif tempDpvNotes == 'A1' :
                            dpvStat = 'INACT_DPV'
                        elif tempvacant == '' and tempDpvNotes == 'AA' : 
                             dpvStat = 'ACT_DPV'
                        elif tempvacant == 'Y' and tempDpvNotes == 'AA' :  
                            dpvStat = 'INACT_VACANT'
                        elif tempDpvNotes == 'AA' :
                            dpvStat = 'ACT_NOTVACANT'
                        else :
                            dpvStat = 'UNKNOWN'
                        newAdressEnum = AdressEnum(temp_adress,'A',dpvStat,formated_date_time,formated_date_time)
                        sessionDb.add(newAdressEnum)
                        ids_list = sessionDb.query(HCOid) \
                        .filter(and_(HCOid.HCO_PL_ID == hco_pl_id,HCOid.ID_TYPE_CD.in_(['CMS_POS','NPI']))).all()
                        for row in ids_list:
                            if row.ID_TYPE_CD == 'CMS_POS' :
                               row.ID_TYPE_CD == 'POS' 
                            LOGGER.info('Creating new location Link') 
                            newAdressLink = AdressLink(newAdressEnum,row.ID_VALUE,row.ENTITY_CD,row.ID_TYPE_CD,formated_date_time,formated_date_time,formated_date_time)
                            newAdressLinkSource = AdressLinksSource(newAdressLink,'1337',formated_date_time,formated_date_time)
                            sessionDb.add(newAdressLink)
                            sessionDb.add(newAdressLinkSource)
                        LOGGER.info('Inserting data in LOC_DEMO_ALL') 
                        footNotes = adress_data.get('analysis', {}).get('footnotes',None) 
                        newAdress = Adress(newAdressEnum,adress_data['delivery_line_1'],adress_data['last_line'],adress_data['components']['zipcode'],temp_adress,adress_data['components']['city_name'],
                        adress_data['components']['state_abbreviation'],adress_data['components']['plus4_code'],adress_data['metadata']['zip_type'],adress_data['metadata']['record_type'],
                        adress_data['delivery_point_barcode'],adress_data['components']['delivery_point'],adress_data['components']['delivery_point_check_digit'],adress_data['analysis']['dpv_match_code'],
                        tempDpvNotes,adress_data['analysis']['dpv_cmra'],tempvacant,adress_data['analysis']['active'],footNotes,
                        adress_data['metadata']['rdi'],adress_data['metadata']['elot_sequence'],adress_data['metadata']['elot_sort'],adress_data['metadata']['carrier_route'],adress_data['metadata']['congressional_district'],
                        adress_data['metadata']['county_fips'],adress_data['metadata']['county_name'],adress_data['components']['primary_number'],adress_data['components']['street_name'],adress_data['components']['street_suffix'],
                        adress_data['components']['default_city_name'],adress_data['metadata']['latitude'],adress_data['metadata']['longitude'],adress_data['metadata']['precision'],adress_data['metadata']['time_zone'],
                        adress_data['metadata']['utc_offset'],adress_data['metadata']['dst'],formated_date_time,formated_date_time,formated_date_time)
                        sessionDb.add(newAdress)
                    else:
                        LOGGER.info('Updating LAST_UPDATE_DT for locations')
                        setattr(existsAdress[0], 'LAST_UPDATE_DT', formated_date)  
                        #create status for know where the procces is
                    sessionDb.commit()
                LOGGER.info('hco verified')
                newStatus = Status(hco_pl_id,6,0,None,formated_date_time,formated_date_time)
                sessionDb.add(newStatus)
                sessionDb.commit()
                sessionDb.close()
            else:
                LOGGER.error(error1.format(spider,'Crawler saving data','Error in crawler'))      
                sessionDb = Session() 
                #create new error status
                newErrorStatus = Status(hco_pl_id,7,1,'Error in crawler',formated_date_time,formated_date_time)
                sessionDb.add(newErrorStatus)
                sessionDb.commit()
                sessionDb.close()
        elif spider == 'LocationsSpider': 
            LOGGER.info('Saving relationships in bd')
            if spider_data[0]['status'] == 'success':
                #create rlationship 
                childs = spider_data[0]['data']
                for child in childs:
                    childName = child['name']
                    childAdreess = child['address']
                    childPhones = child['phones']
            else:
                LOGGER.error(error1.format(spider,'Crawler saving data','Error in crawler'))      
                sessionDb = Session() 
                #create new error status
                newErrorStatus = Status(hco_pl_id,7,1,'Error in crawler',formated_date_time,formated_date_time)
                sessionDb.add(newErrorStatus)
                sessionDb.commit()
                sessionDb.close()   
    except Exception as err:
        LOGGER.exception(error1.format(spider,'Crawler saving data',err))  
        sessionDb = Session() 
        newErrorStatus = Status(hco_pl_id,9,1,str(err),formated_date_time,formated_date_time)
        sessionDb.add(newErrorStatus)
        sessionDb.commit()
        sessionDb.close()
    LOGGER.info('Done')
    cb = functools.partial(ack_message, channel, delivery_tag)
    connection.add_callback_threadsafe(cb)

def on_message(channel, method_frame, header_frame, body, args):
    (connection, threads) = args
    delivery_tag = method_frame.delivery_tag
    t = threading.Thread(target=do_work, args=(connection, channel, delivery_tag, body))
    t.start()
    threads.append(t)

credentials = pika.PlainCredentials(username=user, password=password)
parameters =  pika.ConnectionParameters(host='52.5.96.13', credentials=credentials, heartbeat=1000,
                                       blocked_connection_timeout=600)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(exchange="answer_manager", exchange_type="direct", passive=False, durable=True, auto_delete=False)
channel.queue_declare(queue=queue, durable=True)
channel.queue_bind(queue=queue, exchange="answer_manager", routing_key="standard_key")
channel.basic_qos(prefetch_count=10)

threads = []
on_message_callback = functools.partial(on_message, args=(connection, threads))
channel.basic_consume(queue,on_message_callback)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Wait for all to complete
for thread in threads:
    thread.join()

connection.close()



