
import os
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String
from app.config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, coalesce
from app.utils.functions import *
import uuid
from dotenv import load_dotenv
load_dotenv()

def load_json_query():
    PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
    query_path = os.path.join(PARENT_DIR,"script", "json_query.py")
    print(query_path)
    namespace = {}
    with open(query_path) as f:
        exec(f.read(), namespace)
    return namespace.get("json_query", "")

def get_db_config():
    if TARGET_DB == "POSTGRES":
        return {
            "url": POSTGRES_URL,
            "engine_url": POSTGRES_CON_URL,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "package": "org.postgresql:postgresql:42.7.2",
            "main_table": POSTGRES_TABLE,
            "history_table": POSTGRES_HISTORY_TABLE,
            "pk": POSTGRESS_TABLE_PKID,
            "conn_params": POSTGRESS_CON
        }
    elif TARGET_DB == "MYSQL":
        return {
            "url": f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            "engine_url": f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver",
            "package": "mysql:mysql-connector-java:8.0.33",
            "main_table": MYSQL_TABLE,
            "history_table": MYSQL_HISTORY_TABLE,
            "pk": MYSQL_TABLE_PKID,
            "conn_params": {
                "host": MYSQL_HOST,
                "port": MYSQL_PORT,
                "user": MYSQL_USER,
                "password": MYSQL_PASSWORD,
                "database": MYSQL_DATABASE,
            }
        }

def create_spark_session():
    db_conf = get_db_config()
    return (
        SparkSession.builder \
            .appName("KafkaToDB") \
            .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,{db_conf['package']}") \
            .master("local[*]") \
            .getOrCreate()
    )

def create_table_if_not_exists(df, table_name, pk_field, engine, include_version=False):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        return

    from sqlalchemy import String, Integer
    db_type = TARGET_DB.upper()
    columns = []

    for field in df.schema.fields:
        # add better type mapping here if needed
        if db_type == "MYSQL":
            columns.append(Column(field.name, String(255)))
        else:
            columns.append(Column(field.name, String))

    if include_version:
        if db_type == "MYSQL":
            columns.append(Column("version", String(10)))
        else:
            columns.append(Column("version", String))

    Table(table_name, metadata, *columns).create(bind=engine)

def read_table(spark, db_conf, table_name):
    return spark.read.format("jdbc") \
        .option("url", db_conf["url"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .option("driver", db_conf["driver"]) \
        .load()

def quote_identifier(identifier: str, db_type: str) -> str:
    if db_type.upper() == "POSTGRES":
        return f'"{identifier}"'
    elif db_type.upper() == "MSSQL":
        return f'[{identifier}]'
    elif db_type.upper() == "MYSQL":
        return f'`{identifier}`'
    else:
        raise ValueError(f"Unsupported database type for quoting: {db_type}")

def write_df(df, db_conf, table_name, mode="append"):
    df.write.format("jdbc") \
        .option("url", db_conf["url"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .option("driver", db_conf["driver"]) \
        .mode(mode) \
        .save()

def writestream_non_empty_batches(batch_df, batch_id):
    print(f"\n🚀 Processing batch ID: {batch_id}")

    spark = batch_df.sparkSession
    db_conf = get_db_config()

    value_df = batch_df.selectExpr("CAST(value AS STRING) as value")
    if value_df.rdd.isEmpty():
        print(f"⚠️ Skipping empty batch (id: {batch_id})")
        return

    value_df.createOrReplaceTempView("messageView")
    query_string = load_json_query()
    result_df = spark.sql(query_string)

    if result_df.rdd.isEmpty():
        print(f"⚠️ Extracted no records from JSON in batch {batch_id}.")
        return

    engine = create_engine(db_conf["engine_url"])

    if db_conf["main_table"] not in inspect(engine).get_table_names():
        print(f"✅ Creating main table: {db_conf['main_table']}")
    else:
        print(f"✅ Table {db_conf['main_table']} already exists.")

    if db_conf["history_table"] not in inspect(engine).get_table_names():
        print(f"✅ Creating history table: {db_conf['history_table']}")
    else:
        print(f"✅ Table {db_conf['history_table']} already exists.")

    create_table_if_not_exists(result_df, db_conf["main_table"], db_conf["pk"], engine)
    create_table_if_not_exists(result_df, db_conf["history_table"], db_conf["pk"], engine, include_version=True)

    try:
        existing_df = read_table(spark, db_conf, db_conf["main_table"])
        existing_ids = existing_df.select(db_conf["pk"]).distinct().rdd.flatMap(lambda x: x).collect()
        existing_ids_broadcast = spark.sparkContext.broadcast(existing_ids)

        new_records = result_df.filter(~col(db_conf["pk"]).isin(existing_ids_broadcast.value))
        update_records = result_df.filter(col(db_conf["pk"]).isin(existing_ids_broadcast.value))

        print(f"🆕 New records count: {new_records.count()}")
        print(f"♻️ Update records count: {update_records.count()}")

        if not new_records.rdd.isEmpty():
            write_df(new_records, db_conf, db_conf["main_table"], mode="append")
            print(f"📥 Inserted {new_records.count()} new records into {db_conf['main_table']}.")

        if not update_records.rdd.isEmpty():
            current_df = existing_df
            join_cond = current_df[db_conf["pk"]] == update_records[db_conf["pk"]]
            joined_df = current_df.alias("curr").join(update_records.alias("new"), join_cond, "inner")

            data_columns = [c for c in current_df.columns if c != db_conf["pk"]]
            change_conditions = [col(f"curr.{c}") != col(f"new.{c}") for c in data_columns]
            combined_change_condition = change_conditions[0]
            for cond in change_conditions[1:]:
                combined_change_condition |= cond

            changed_records = joined_df.filter(combined_change_condition).select("curr.*")

            print(f"📦 Changed record count: {changed_records.count()}")

            if not changed_records.rdd.isEmpty():
                history_df = read_table(spark, db_conf, db_conf["history_table"])
                max_versions_df = history_df.groupBy(db_conf["pk"]) \
                    .agg(spark_max("version").alias("max_version"))

                versioned_old = changed_records.join(max_versions_df, db_conf["pk"], how="left") \
                    .withColumn("version", coalesce(col("max_version").cast("int"), lit(0)) + 1) \
                    .drop("max_version")

                versioned_old = versioned_old.dropDuplicates()
                write_df(versioned_old, db_conf, db_conf["history_table"], mode="append")
                print(f"📦 Archived {versioned_old.count()} changed records to history table.")

                changed_ids = changed_records.select(db_conf["pk"]).distinct().rdd.map(lambda row: row[0]).collect()
                ids_str = ", ".join([f"'{x}'" for x in changed_ids])

                quoted_table = quote_identifier(db_conf["main_table"], TARGET_DB)
                quoted_pk = quote_identifier(db_conf["pk"], TARGET_DB)
                delete_query = f'DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({ids_str})'

                if TARGET_DB == "POSTGRES":
                    import psycopg2
                    conn = psycopg2.connect(**db_conf["conn_params"])
                elif TARGET_DB == "MSSQL":
                    import pyodbc
                    conn = pyodbc.connect(
                        f"DRIVER={db_conf['conn_params']['driver']};"
                        f"SERVER={db_conf['conn_params']['server']};"
                        f"DATABASE={db_conf['conn_params']['database']};"
                        f"UID={db_conf['conn_params']['uid']};"
                        f"PWD={db_conf['conn_params']['pwd']}"
                    )
                elif TARGET_DB == "MYSQL":
                    import mysql.connector
                    conn = mysql.connector.connect(**db_conf["conn_params"])

                cursor = conn.cursor()
                cursor.execute(delete_query)
                conn.commit()
                cursor.close()
                conn.close()
                print(f"🧹 Deleted old records from {db_conf['main_table']} for update.")

                update_records_filtered = update_records.filter(col(db_conf["pk"]).isin(changed_ids))
                write_df(update_records_filtered, db_conf, db_conf["main_table"], mode="append")
                print(f"🔄 Updated {update_records_filtered.count()} records in {db_conf['main_table']}.")

    except Exception as e:
        print(f"❌ Error in batch {batch_id}: {e}")
