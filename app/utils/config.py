from dotenv import load_dotenv
load_dotenv()
import uuid
import os
import json
import uuid
from dotenv import load_dotenv
load_dotenv()

# Set target DB: "POSTGRES", "MSSQL", or "MYSQL"
TARGET_DB                           = os.environ['TARGET_DB']

# KAFKA CONFIGS
KAFKA_BOOTSTRAP_SERVERS             = os.environ['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_TOPIC                         = os.environ['KAFKA_TOPIC']
CHECKPOINT_LOC                      = f"{os.environ['CHECKPOINT_LOC']}_{uuid.uuid4()}"

# POSTGRES CONFIG
POSTGRES_USER                       = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD                   = os.environ['POSTGRES_PASSWORD']
POSTGRES_DB                         = os.environ['POSTGRES_DB']
POSTGRES_HOST                       = os.environ['POSTGRES_HOST']
POSTGRES_PORT                       = os.environ['POSTGRES_PORT']
POSTGRES_TABLE                      = os.environ['POSTGRES_TABLE']
POSTGRES_HISTORY_TABLE              = os.environ['POSTGRES_HISTORY_TABLE']
# Parse primary key - can be single string or JSON list
try:
    POSTGRESS_TABLE_PKID = json.loads(os.environ['POSTGRESS_TABLE_PKID'])
except json.JSONDecodeError:
    POSTGRESS_TABLE_PKID = os.environ['POSTGRESS_TABLE_PKID'].strip('"\'')
POSTGRES_DRIVER                     = os.environ['POSTGRES_DRIVER']

POSTGRES_URL                        = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_CON_URL                    = (f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
                                       f"/{POSTGRES_DB}")

POSTGRES_AUTH                       = {"user": POSTGRES_USER,"password": POSTGRES_PASSWORD}

POSTGRESS_CON                       = {"dbname": POSTGRES_DB,"host": POSTGRES_HOST,"port": POSTGRES_PORT,
                                        **POSTGRES_AUTH
                                        }

POSTGRESS_SPARK_CONF                = {"url": POSTGRES_URL,"dbtable": POSTGRES_TABLE,"driver": POSTGRES_DRIVER,
                                        **POSTGRES_AUTH
                                        }

# MYSQL CONFIG
MYSQL_USER = "mysql"
MYSQL_PASSWORD = "mysql*0123"
MYSQL_DATABASE = "mydb"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_TABLE = "bank_json"
MYSQL_HISTORY_TABLE = "bank_json_history"
MYSQL_TABLE_PKID = "MsgId"

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_CON_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

MYSQL_CON = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DATABASE,
}

MYSQL_SPARK_CONF = {
    "url": MYSQL_URL,
    "dbtable": MYSQL_TABLE,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": MYSQL_DRIVER
}
