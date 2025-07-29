from pyspark.sql.types import *

def spark_to_postgres_type(spark_type):

    mapping = {
        StringType(): "TEXT",
        IntegerType(): "INTEGER",
        LongType(): "BIGINT",
        DoubleType(): "DOUBLE PRECISION",
        FloatType(): "REAL",
        BooleanType(): "BOOLEAN",
        TimestampType(): "TIMESTAMP",
        DateType(): "DATE",
    }
    return mapping.get(spark_type, "TEXT")
