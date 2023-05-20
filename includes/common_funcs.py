# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def insert_by_partition(df, database, table, partition_col):
    """
    Insert a Dataframe into a SQL Table by a partition. The function will overwrite the partition instead of the whole table.
    """

    # The partition column must be the last column for insertInto to work. Ref: https://towardsdatascience.com/understanding-the-spark-insertinto-function-1870175c3ee9.
    df_cols = df.columns
    partition_col_index = df_cols.index(partition_col)
    df_cols[partition_col_index], df_cols[-1] = df_cols[-1], df_cols[partition_col_index]
    df = df.select(df_cols)
    
    table_instance = f"{database}.{table}"
    if spark._jsparkSession.catalog().tableExists(table_instance):
        df.write.mode("overwrite").insertInto(table_instance)
    else:
        df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(table_instance)
