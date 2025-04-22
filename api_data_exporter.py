import json
import sys
from functools import reduce, partial
from purplelab.storage import storage_service
from purplelab.utils.aws import s3_client
from purplelab.utils.file_utils import file_utils
from purplelab.utils.crypto import crypto_utils
from purplelab.utils.mysql_utils import mysql_client, mysql_csv_exporter
from purplelab.utils.google_cloud import logging
from purplelab.utils.property import properties
from purplelab.utils.functools import functional

logging.setup_logger()
LOG = logging.get_logger()


def export_api_data(export_settings_file):
    settings = _get_export_settings(export_settings_file)
    con = mysql_client.get_connection(settings["db_connection"])
    task_id = settings["id"]
    try:
        LOG.info("export api data, begin to export data for task %s", task_id)
        _update_task_state(task_id, "RUNNING", con)
        query = settings["query"]
        params = settings["params"]
        calculate_header = settings.get("calculate_header", False)
        columns_to_replace = settings.get("columns_to_replace", [])
        get_header_1 = partial(_get_header_1, columns_to_replace)
        get_header_1 = functional.identity() if not calculate_header else get_header_1
        output = s3_client.get_temp_file()
        remove_detail_column_name = partial(
            _remove_detail_column_name, columns_to_replace)
        LOG.info(
            "export api data, begin to  export data for task %s in path %s", task_id, output)
        query_file = mysql_csv_exporter.export_query_to_csv(
            query, params, con, remove_detail_column_name, get_header_1)
        s3_client.upload_file(query_file, output)
        _set_task_state_finish(task_id, output, con)
        LOG.info(
            "export api data, finish to  export data for task %s in path %s", task_id, output)
    except Exception as ex:
        error_message = "task id %s(%s) fail to export query" % (
            task_id, export_settings_file)
        LOG.critical(error_message, exc_info=ex)
        _update_task_state(task_id, "FAIL", con)
        raise ex
    finally:
        con.close()


def _get_header_1(columns_to_replace, column):
    columns = filter(lambda replace: len(
        column.split(replace)) > 1, columns_to_replace)
    columns = list(columns)
    return "" if not columns else columns[0].replace("_", "").upper()


def _remove_detail_column_name(columns_to_replace, column):
    all_replaces = ["detail_", "_detail", "_master", "master_"]
    all_replaces.extend(columns_to_replace)
    return reduce(lambda new_column, column_name: new_column.replace(column_name, ""),
                  all_replaces, column)


def _update_task_state(task_id, state, con):
    query = """update exports set state='{}',
     output=''
      where export_id='{}'""".format(state, task_id)
    mysql_client.execute_statement(query, con)


def _set_task_state_finish(task_id, output, con):
    query = """update exports set state='{}',
     output='{}',finished_at=now()
      where export_id='{}'""".format("DONE", output, task_id)
    mysql_client.execute_statement(query, con)


def _get_export_settings(export_settings_remote_file):
    settings_file = storage_service.download_file(
        export_settings_remote_file)
    encryption_key = properties.get_property_from_context(
        "crypto", "encryption_key")
    temp = crypto_utils.decrypt_file(encryption_key, settings_file)
    settings = json.loads(file_utils.get_file_content(temp))
    file_utils.delete_file(settings_file)
    file_utils.delete_file(temp)
    return settings


if __name__ == "__main__":
    export_api_data(sys.argv[1])
