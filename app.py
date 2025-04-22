from datetime import datetime 
from models.base import Session, engine, Base, enviroment, variables
from sqlalchemy import and_, or_
from models.hco_status import Status
from models.online_presence import OnlinePresence
from models.hco_id import HCOid
from models.hco_name import HCOName
import pika
import time

queue = variables['enviroments'][enviroment]['queue']
user = variables['globals']['rabbit_user']
password = variables['globals']['rabbit_pass']
credentials = pika.PlainCredentials(username=user, password=password)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='52.5.96.13',credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True)
#init db
sessionDb = Session()
#Get the online precences ready for get htmls
web_urls = sessionDb.query(HCOid,HCOName,OnlinePresence) \
    .join(HCOName, HCOid.HCO_PL_ID==HCOName.HCO_PL_ID) \
    .join(OnlinePresence, HCOid.HCO_PL_ID==OnlinePresence.HCO_PL_ID) \
    .filter(and_(HCOid.ID_TYPE_CD.in_(['CMS_POS']),HCOName.HCO_NAME_TYPE_CD.in_(['CDBA']),OnlinePresence.ONLINE_PRESENCE_URL_TYPE_CD == '02'))\
    .all()
#go through results for create html broker messages and change status
print(len(web_urls))
for row in web_urls:
    #Verifies still not gets htmls or have and error 
    exists = sessionDb.query(Status)\
    .filter(and_(Status.HCO_PL_ID == row.HCOid.HCO_PL_ID\
    ,Status.ID_STATUS == 4))\
    .count()
    print(exists)
    if exists == 0 :
        #creating message in the rabbitMQ queue 
        message= '{"task":"getHtml",'f'"id":"{row.HCOid.HCO_PL_ID}","name":"{row.HCOName.HCO_NAME}","url":"{row.OnlinePresence.ONLINE_PRESENCE_URL}"''}'
        print(message) 
        channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
        #change status 
        today = datetime.now()
        formated_date_time = today.strftime("%Y-%m-%d %H:%M:%S")
        newStatus = Status(row.HCOid.HCO_PL_ID,4,0,'Getting the html',formated_date_time,formated_date_time)
        sessionDb.add(newStatus)
        sessionDb.commit()
    else :
        message = '{"task":"getHtml",'f'"id":"{row.HCOid.HCO_PL_ID}","name":"{row.HCOName.HCO_NAME}","message":"All ready in the data base"''}'
        print(message) 
        channel.basic_publish(
            exchange='',
            routing_key='beforeDoneTask',
            body=message,
            properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
print('DONE')
sessionDb.close()
exit()
    

