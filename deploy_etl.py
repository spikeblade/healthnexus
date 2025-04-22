from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import datetime
import json
import argparse

DEFAULT_QUEUE = "purplelab-admin-tools-workers"
MTMDM_QUEUE = "mtmdm-admin-tools-workers"
MTMDM_LONG_RUNNING_QUEUE = "mtmdm-admin-tools-workers-long-running-task"


def create_etl(category_name, etl_file, queries_dir, is_name_query, recursive_query, queue, db):
    category_id = get_or_create_category_id(category_name, db)
    etl_content = get_etl_with_params(etl_file)
    etl = etl_content["etl"]
    etl_params = etl_content["etl_params"]
    queries = get_queries_from_directory(
        queries_dir, is_name_query, recursive_query)
    etl["type"] = get_object_id(category_id)
    etl["createdAt"] = get_date_time_now()
    etl["updatedAt"] = get_date_time_now()
    etl["queue"] = queue
    delete_etl(etl["name"], db)
    etl_id = save_etl(etl, db)
    save_etls_params(etl_id, etl_params, db)
    if queries:
        queries_param = {
            "name": "queries", "type": "object", "default": queries
        }
        save_etl_param(etl_id, queries_param, db)


def save_etls_params(etl_id, etl_params, db):
    return list(map(lambda param: save_etl_param(etl_id, param, db), etl_params))


def save_etl_param(etl_id, etl_param, db):
    if etl_param["type"] in ["object", "choice"]:
        etl_param["default"] = json.dumps(etl_param["default"])
    etl_param["createdAt"] = get_date_time_now()
    etl_param["etl"] = get_object_id(etl_id)
    db.etlparam.insert_one(etl_param)


def get_etl_with_params(file_path):
    content = read_file(file_path)
    return json.loads(content)


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


def get_queries_from_directory(etl_query_dir, is_name_query, is_recursive):
    if not is_recursive:
        return _get_queries_from_directory(etl_query_dir, is_name_query)
    return _get_queries_from_directory_recursive(etl_query_dir, is_name_query)


def _get_queries_from_directory_recursive(etl_query_dir, is_name_query):
    etl_dirs = map(lambda file_path: os.path.join(
        etl_query_dir, file_path), os.listdir(etl_query_dir))
    etl_dirs = filter(os.path.isdir, etl_dirs)
    etl_dirs_map = map(lambda etl_dir: (os.path.basename(
        etl_dir), _get_queries_from_directory_recursive(etl_dir, is_name_query)), etl_dirs)
    etl_queries = dict(etl_dirs_map)
    etl_queries.update(_get_queries_from_directory(
        etl_query_dir, is_name_query))
    return etl_queries


def _get_queries_from_directory(etl_query_dir, is_name_query):
    queries_files = filter(lambda file_name: file_name.endswith(
        ".sql"), os.listdir(etl_query_dir))
    if not is_name_query:
        queries_files = sorted(queries_files, key=get_query_file_key)
    queries_files_path = map(lambda query_file: os.path.join(
        etl_query_dir, query_file), queries_files)
    queries_files_path = list(queries_files_path)
    queries_name = list(map(lambda file_name: file_name.replace(
        ".sql", ""), map(os.path.basename, queries_files_path)))
    queries = list(map(read_file, queries_files_path))
    if is_name_query:
        return dict(zip(queries_name, queries))
    return queries


def get_query_file_key(query_file):
    return int(query_file.split("__")[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file", help="etl json file")
    parser.add_argument(
        "--queries", help="etl queries", default=".")
    parser.add_argument(
        "--name_query", help="sort query files or use dict instead with the file name like key",
        default=False,
        action="store_true")
    parser.add_argument(
        "--recursive_query", help="add queries recursive from directory",
        default=False,
        action="store_true")
    parser.add_argument(
        "-m", "--mtmdm", help="set etl to execute to mtmdm workers",
        default=False,
        action="store_true")
    parser.add_argument(
        "-l", "--longrunning", help="set etl to execute to mtmdm long running workers",
        default=False,
        action="store_true")
    parser.add_argument("-a", "--action",
                        help="action to do add or delete are the only valid",
                        type=str, choices=["add", "delete"])
    parser.add_argument("--database", default="purplelab",
                        help="database to connection")
    parser.add_argument("-t", "--type", default="bigquery",
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
    etl_query_file = args.file
    is_name_query = args.name_query
    recursive_query = args.recursive_query
    queries_dir = args.queries
    category_name = args.type
    queue = DEFAULT_QUEUE
    if args.mtmdm:
        queue = MTMDM_QUEUE
    elif args.longrunning:
        queue = MTMDM_LONG_RUNNING_QUEUE
    if action == "add":
        create_etl(category_name, etl_query_file,
                   queries_dir, is_name_query, recursive_query, queue, db)
    else:
        etl_content = get_etl_with_params(etl_query_file)
        delete_etl(etl_content["etl"]["name"], db)


if __name__ == "__main__":
    main()
