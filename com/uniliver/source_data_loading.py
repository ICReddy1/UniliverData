from pyspark.sql import SparkSession, functions
import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import current_date

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)
    src_list = app_conf["source_data_list"]
    for src in src_list:
        src_conf = app_conf[src]
        if src_list == "SB":
            print("\nReading SB data from MySQL DB  ..")
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": src_conf["mysql_conf"]["dbtable"],
                          "numPartitions": "2",
                          "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }
            txn_df = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load()\
                .withColumn("ins_dt",functions.current_date())
            txn_df.show()

            print("\nWriting  data to S3  using SparkSession.write.format(),")

            txn_df \
                .write \
                .partitionBy("ins_dt") \
                .mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", "~") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/SB")

        elif src == "OL":
            print("\nReading OL data from FTP  ..")
            src_conf = app_conf[src]
            txn_df2 = spark.read \
                .format("com.springml.spark.sftp") \
                .option("host", app_secret["sftp_conf"]["hostname"]) \
                .option("port", app_secret["sftp_conf"]["port"]) \
                .option("username", app_secret["sftp_conf"]["username"]) \
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .option("fileType", "csv") \
                .option("delimiter", "|") \
                .load(src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")\
                .withColumn("ins_dt",functions.current_date())

            txn_df2.show(5, False)

            print("\n writing sft data to S3  using SparkSession.write.format(),")
            txn_df2.write \
                .partitionBy("ins_dt") \
                .mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", "|") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/OL")

            print("\n writing SFT done,")

# spark-submit --packages "mysql:mysql-connector-java:8.0.11,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/uniliver/source_data_loading.py
