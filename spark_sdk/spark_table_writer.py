from pyspark.sql import DataFrame, SparkSession
from typing import List, Optional


class SparkTableWriter:
    def __init__(
        self,
        spark: SparkSession,
        database: str,
        table: str,
        format: str = "delta",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        path: Optional[str] = None,
        use_catalog: bool = False,
        overwrite_schema: bool = True,
        description: Optional[str] = None,
    ):
        self.spark = spark
        self.database = database
        self.table = table
        self.full_table_name = f"{database}.{table}"
        self.format = format.lower()
        self.mode = mode
        self.partition_by = partition_by or []
        self.path = path
        self.use_catalog = use_catalog
        self.overwrite_schema = overwrite_schema
        self.description = description

        self._validate_format()
        self._create_database_if_needed()

    def _validate_format(self):
        if self.format not in ["delta", "parquet", "csv"]:
            raise ValueError(f"Unsupported format: {self.format}. Only 'delta' and 'parquet' are supported.")

    def _create_database_if_needed(self):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def write(self, df: DataFrame):
        """
        Writes the DataFrame to the configured table and/or location
        """
        writer = (
            df.write
            .format(self.format)
            .mode(self.mode)
        )

        if self.overwrite_schema:
            writer = writer.option("overwriteSchema", "true")

        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        try:
            if self.path:
                if not self.path.strip():
                    raise ValueError("Provided path is empty or invalid.")

                print(f"Writing data to path: {self.path}")

                writer = writer.option("path", self.path)
                writer.saveAsTable(self.full_table_name)
            else:
                writer.saveAsTable(self.full_table_name)

            if self.description:
                self.spark.sql(f"COMMENT ON TABLE {self.full_table_name} IS '{self.description}'")

            print(f"Successfully wrote to table: {self.full_table_name}")
        except Exception as e:
            print(f" Failed to write table {self.full_table_name}: {str(e)}")
            raise

    def drop_table_if_exists(self):
        """
        Drop table before writing (optional utility)
        """
        if self.table_exists():
            print(f" Dropping existing table: {self.full_table_name}")
            self.spark.sql(f"DROP TABLE {self.full_table_name}")

    def table_exists(self) -> bool:
        """
        Check if table exists
        """
        return self.full_table_name in [
            f"{tbl.database}.{tbl.name}"
            for tbl in self.spark.catalog.listTables(self.database)
        ]
