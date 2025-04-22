from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import datetime
import json
import argparse

MYSQL_WORKFLOW = 'purplelab.etl.mysql_etl.run_mysql_etl'
DEFAULT_QUEUE = "purplelab-admin-tools-workers"


def create_etl(etl_name, category_name, etl_query_dir, db):
    category_id = get_or_create_category_id(category_name, db)
    etl = {"name": etl_name, "description": etl_name, "type": get_object_id(category_id),
           "deleted": False,
           "template": "N/A", "documentation": "", "createdAt": get_date_time_now(),
           "workflow": MYSQL_WORKFLOW,
           "updatedAt": get_date_time_now()}
    etl["queue"] = DEFAULT_QUEUE
    etl_id = save_etl(etl, db)
    save_etls_params(etl_id, etl_query_dir, db)


def save_etls_params(etl_id, etl_query_dir, db):
    parameters = get_default_params_by_etl_query_dir(etl_query_dir)
    pre_load_queries = get_queries_from_directory(
        os.path.join(etl_query_dir, 'pre_load'))
    post_load_queries = get_queries_from_directory(
        os.path.join(etl_query_dir, 'post_load'))
    big_queries = get_queries_from_directory(
        os.path.join(etl_query_dir, 'big_queries'))
    mysql_queries = get_queries_from_directory(
        os.path.join(etl_query_dir, 'mysql_queries'))
    if 'begin' in parameters:
        save_parameter(etl_id, "begin", "string", parameters["begin"], db)
    if 'end' in parameters:
        save_parameter(etl_id, "end", "string", parameters["end"], db)
    save_etl_param(etl_id, "backups", parameters["backups"], db)
    save_etl_param(etl_id, "pre_load_queries", pre_load_queries, db)
    save_etl_param(etl_id, "post_load_queries", post_load_queries, db)
    save_etl_param(etl_id, "big_queries", big_queries, db)
    save_etl_param(etl_id, "mysql_queries", mysql_queries, db)
    save_parameter(etl_id, "load_table", "readonly",
                   parameters["load_table"], db)
    if len(mysql_queries) > 0:
        save_parameter(etl_id, "connection_source",
                       "environment", "development", db)
        save_parameter(etl_id, "connection_target",
                       "environment", "development", db)
    else:
        save_parameter(etl_id, "connection_target",
                       "environment", "development", db)


def save_parameter(etl_id, parameter_name, parameter_type, parameter_val, db):
    etl_param = {"name": parameter_name, "type": parameter_type,
                 "default": parameter_val,
                 "etl": get_object_id(etl_id),
                 "createdAt": get_date_time_now()}
    db.etlparam.insert_one(etl_param)


def get_queries_from_directory(etl_query_dir):
    if not os.path.exists(etl_query_dir):
        return list()
    queries_files = filter(lambda file_name: file_name.endswith(
        ".sql"), os.listdir(etl_query_dir))
    queries_files_sorted = sorted(queries_files, key=get_query_file_key)
    queries_files_path = map(lambda query_file: os.path.join(
        etl_query_dir, query_file), queries_files_sorted)
    return list(map(read_file, queries_files_path))


def get_query_file_key(query_file):
    return int(query_file.split("__")[0])


def save_etl_param(etl_id, param_name, param_value, db):
    etl_param = {"name": param_name, "type": "object", "default":
                 json.dumps(param_value), "etl": get_object_id(etl_id),
                 "createdAt": get_date_time_now()}
    db.etlparam.insert_one(etl_param)


def get_default_params_by_etl_query_dir(etl_query_dir):
    paremeters_file = os.path.join(etl_query_dir, "parameters.json")
    if not os.path.exists(paremeters_file):
        return {"backups": [], "temps_tables": []}
    return json.loads(read_file(paremeters_file))


def read_file(file_path):
    fd = open(file_path, "r")
    content = fd.read()
    fd.close()
    return content


def save_etl(etl, db):
    return db.etl.insert_one(etl).inserted_id


def get_or_create_category_id(category_name, db):
    category = get_category(category_name, db)
    if not category:
        return create_category(category_name, db)
    return category["_id"]


def create_category(category_name, db):
    category = {"name": category_name, "createdAt": get_date_time_now(
    ), "updatedAt": get_date_time_now()}
    return db.etltype.insert_one(category).inserted_id


def delete_etl(etl_name, db):
    etl = get_etl(etl_name, db)
    if not etl:
        return etl
    etl_id = etl["_id"]
    delete_etl_params(etl_id, db)
    db.etl.remove({"_id": get_object_id(etl_id)})


def delete_etl_params(etl_id, db):
    db.etlparam.delete_many({"etl": get_object_id(etl_id)})


def get_etl(etl_name, db):
    return db.etl.find_one({"name": etl_name})


def get_date_time_now():
    return datetime.datetime.utcnow()


def get_category(category, db):
    etl_types = db.etltype
    query = {"name": category}
    return etl_types.find_one(query)


def get_db(client_path, purplelab_db):
    client = MongoClient(client_path)
    return client[purplelab_db]


def get_object_id(id_str):
    return ObjectId(id_str)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("etl", help="name of the etl")
    parser.add_argument(
        "--dir", help="folder of the etl that contain queries and parameters.json", default=".")
    parser.add_argument("-a", "--action",
                        help="action to do add or delete are the only valid",
                        type=str, choices=["add", "delete"])
    parser.add_argument("--database", default="purplelab",
                        help="database to connection")
    parser.add_argument("-t", "--type", default="mysql",
                        help="type of the etl")
    parser.add_argument("--host", default="35.202.237.183",
                        help="host to connect")
    parser.add_argument("-p", "--port", default="27017",
                        help="port to connect")
    parser.add_argument("-u", "--user", default="api",
                        help="database user")
    parser.add_argument("--password", default="cVFf5cX8LyvbA8db",
                        help="database password")
    parser.add_argument("--auth", default="admin",
                        help="authentication database")
    args = parser.parse_args()
    action = args.action
    purplelab_db = args.database
    user = args.user
    password = args.password
    auth_database = args.auth
    client_path = "mongodb://%s:%s/" % (args.host, args.port)
    if len(user.strip()) != 0:
        client_path = "mongodb://%s:%s@%s:%s/?authSource=%s" % (
            user, password, args.host, args.port, auth_database)

    db = get_db(client_path, purplelab_db)
    etl_query_dir = args.dir
    category_name = args.type
    etl_name = args.etl
    if action == "add":
        delete_etl(etl_name, db)
        create_etl(etl_name, category_name, etl_query_dir, db)
    else:
        delete_etl(etl_name, db)


if __name__ == "__main__":
    main()
