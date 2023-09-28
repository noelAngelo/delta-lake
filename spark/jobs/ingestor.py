from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql import DataFrame
import logging
import sys


class Transformer:

    @staticmethod
    def add_audit_file_name(df):
        return df.withColumn('audit_src_file_name', input_file_name())

    @staticmethod
    def add_audit_ingest_timestamp(df):
        return df.withColumn('audit_tgt_load_timestamp', current_timestamp())

    @staticmethod
    def full_transform(df):
        logging.info('Ingestor asked Transformer to process a delta table...')
        df = (df
              .transform(Transformer.add_audit_ingest_timestamp)
              .transform(Transformer.add_audit_ingest_timestamp))
        df.printSchema()
        logging.info('Transformer successfully processed a delta table...')
        return df


class Ingestor:
    """Class to ingest files from an S3 directory into a Delta Lake"""

    def __init__(self,
                 ss: SparkSession,
                 source: str,
                 path: str,
                 name: str,
                 target: str,
                 checkpoints: str,
                 schema: str):
        logging.info("Running Ingestor with settings ...")
        self.ss = ss
        self.source_bucket = source
        self.source_name = name
        self.source_path = path
        self.target_bucket = target
        self.checkpoints = checkpoints
        self.target_schema = schema
        logging.info(f"Ingestor for {self.source_name} has been initialised successfully ...")

    def _read_from_s3(self) -> DataFrame:
        """Incrementally ingest data from an S3 URI. (does not load the data, see `write_to_sink` )"""
        logging.info(f"Reading files from {self.source_bucket}")
        dataframe = (spark.readStream
                     .format("json")
                     .load(f"s3a://{self.source_bucket}/{self.source_path}/*"))
        logging.info(f"Found {len(dataframe.columns)} columns")
        dataframe.printSchema()
        return dataframe

    def _process_microbatch(self, df: DataFrame, batch_id):
        """Processes the writing into delta lake through micro-batches"""

        # Write to Delta Lake
        (df.write.format("delta")
        .mode("append")
        .saveAsTable(
            f"{self.source_name}"))

    def start(self):
        """Writes the ingested data into delta lake"""
        # Read incoming data from S3
        dataframe_stream = self._read_from_s3()

        # Transform incoming data from MinIO
        transformed_stream = Transformer.full_transform(dataframe_stream)

        logging.info(f"Writing files into {self.target_bucket}")
        write_stream = (transformed_stream.writeStream
                        .format("delta")
                        .option("checkpointLocation", f's3a://{self.checkpoints}/')
                        .foreachBatch(lambda mdf, batch_id: self._process_microbatch(mdf, batch_id)))

        query = write_stream.trigger(once=True).start()
        query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        format=(
            "[%(asctime)s.%(msecs)03d] [%(threadName)s] [%(levelname)s] "
            "~~~ %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # collect from args
    source_bucket, source_name, source_path = sys.argv[1], sys.argv[2], sys.argv[3]
    target_bucket, target_schema, checkpoint_location = sys.argv[4], sys.argv[5], sys.argv[6]

    # start a Spark session
    spark = SparkSession \
        .builder \
        .appName(f'ingest-{source_name}') \
        .getOrCreate()
    spark.conf.set("spark.sql.streaming.schemaInference", True)

    # start Ingestor
    ingestor = Ingestor(spark,
                        source=source_bucket,
                        name=source_name,
                        path=source_path,
                        target=target_bucket,
                        checkpoints=checkpoint_location,
                        schema=target_schema)
    ingestor.start()
    spark.stop()
