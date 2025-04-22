import logging
from config.config import configDbDev, configDbProd
import pymysql
import uuid
from util.util import send_file




def lambda_handler(event, context):
    conn = None
    env = event.get("environment", None)
    provider = event.get("providerGroup", None)
    provider_version = event.get("providerGroupVersion", None)
    project = event.get("project", None)
    permissions = event.get("permissions", None)
    email = event.get("email", None)
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.warning("Info : the process init for event {}".format(event))
        ref = None
        if env== 'development':
            ref = configDbDev()
        else:
            ref = configDbProd()
        conn = pymysql.connect(host=ref['host'], user=ref['user'], passwd=ref['password'], db=ref['database'], port=int(ref['port']), connect_timeout=10)
        cursor = conn.cursor()
        query = "select pl_id from lnk_providergroups where  providergroup_id = "+str(provider)+" and providergroup_versions_id = "+str(provider_version)
        logger.info("Query for get pl ids %s",query)
        cursor.execute(query)
        ids_file = '\n'.join([str(elem[0]) for elem in cursor])
        logger.info("Query executed and file formed")
        provider_txt = str(provider) + str(provider_version)
        logger.info("Sending file to athena %s",provider_txt)
        send_file("athena-data-ondemand-test", ids_file, 'providers/'+provider_txt+'/providers.csv')
        tables = project['project_tables']
        for table in tables:
            is_valid_table = False
            for permission in permissions:
                if table['table_name'] == permission['name']:
                    is_valid_table = True
            if is_valid_table:
                field_on = table['table_name'][0:3] + '_PL_ID'
                logger.info('Extraction for table %s', table['table_name'])
                message = '{"query":"select * from masterfile_' + table['table_name'] + ' join providers'+provider_txt+' on masterfile_' + table['table_name']+'.' + field_on + '= providers'+provider_txt+'.id","database":"'+ref['athena_db']+'",'\
                            '"provider_name":" providers' + provider_txt + '",'\
                            '"provider_folder":"' + provider_txt + '",'\
                            '"bucket":"'+ref['bucket']+str(project['id'])+'-'+project['name']+'-'+project['process_id']+'","path":"' + \
                            email+'/'+table['table_name']+'"}'
                logger.info('Message out %s', message)
                f_name = str(table['id'])+str(uuid.uuid1())+'.json'
                logger.info('Sending file to S3 for process %s', f_name)
                send_file("data-extraction-messages", message, f_name)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
    finally:
        if conn != None:
           conn.close()
 
