from airflow.sdk.bases.hook import BaseHook
from pyspark.context import SparkContext

def apply_s3_config(sc: SparkContext):
    """
    Retrieve Airflow AWS connection credentials then 
    inject them to SparkContext (sc)
    """
    # Use Airflow aws_default connection
    aws_conn = BaseHook.get_connection("aws_default")
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    
    # Inject Airflow credentials to Hadoop Spark and set configurations
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.maximum", "100")