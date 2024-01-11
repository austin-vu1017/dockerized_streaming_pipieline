import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    session.execute(
    """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute(
    """
    CREATE TABLE IF NOT EXISTS spark_streams.crypto_market (
        id UUID PRIMARY KEY NOT NULL,
        rank VARCHAR(3),
        symbol VARCHAR(5) NOT NULL,
        supply INT,
        maxSupply INT,
        marketCapUsd TEXT,
        volumeUsd24Hr TEXT,
        priceUsd TEXT,
        changePercent24Hr TEXT,
        vwap24Hr TEXT,
        datetime DATETIME NOT NULL
    );
    """)

    print("Table created successfully!")

def insert_date(session, **kwargs):
    print("inserting data...")

    c_id = kwargs.get('id')
    rank = kwargs.get('rank')
    symbol = kwargs.get('symbol')
    supply = kwargs.get('supply')
    maxSupply = kwargs.get('maxSupply')
    marketCapUsd = kwargs.get('marketCapUsd')
    volumeUsd24Hr = kwargs.get('volumeUsd24Hr')
    priceUsd = kwargs.get('priceUsd')
    changePercent24Hr = kwargs.get('changePercent24Hr')
    vwap24Hr = kwargs.get('vwap24Hr')
    data_datetime = kwargs.get('datetime')

    try:
        session.execute(
        """
            INSERT INTO spark_streams.created_users(c_id, rank, symbol, supply, maxSupply, 
                marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr, data_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (c_id, rank, symbol, supply, maxSupply,
              marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr, data_datetime))
        logging.info(f"Data inserted for {c_id} {symbol}")

    except Exception as e:
        logging.error(f"Can't insert data due to {e}")


def create_spark_conn() -> SparkSession.builder:
    session_conn = None
    
    try:
        session_conn = SparkSession.builder \
            .appName() \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.41",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
            
        session_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection established successfully.')
    except Exception as e:
        logging.error(f"Can't establish Spark connection due to exception {e}")
    
    return session_conn

def create_cassandra_conn():
    cass_session = None
    
    try:
        cluster = Cluster(['localhost'])
        cass_session = cluster.connect()
        
        return cass_session
    except Exception as e:
        logging.error(f"Can't establish Cassandra connection due to exception {e}")
        
        return None
        
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("rank", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("supply", StringType(), False),
        StructField("maxSupply", StringType(), False),
        StructField("marketCapUsd", StringType(), False),
        StructField("volumeUsd24Hr", StringType(), False),
        StructField("priceUsd", StringType(), False),
        StructField("changePercent24Hr", StringType(), False),
        StructField("vwap24Hr", StringType(), False),
        StructField("data_datetime", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def main():
    spark_conn = create_spark_conn()
    
    if spark_conn is not None:
        session = create_cassandra_conn()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is currently starting...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
            

if __name__ == '__main__':
    main()
