import findspark
findspark.init()

from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
from rpolygonpoint.utils.utils import logging


spark_conf = (
    SparkConf()
        .set("spark.executor.cores", 1)
        .set("spark.executor.instances", 4)
        .set("spark.dynamicAllocation.minExecutors", 0)
        .set("spark.dynamicAllocation.maxExecutors", 4)
        .set("spark.dynamicAllocation.enabled", False)
        .set("spark.sql.shuffle.partitions", 4)
        .set("spark.driver.memory", "8g")
)


spark = (
    SparkSession
        .builder
        .config(conf=spark_conf)
        .appName("random-polygon-point")
        .getOrCreate()
)


_storage_level_ = StorageLevel.DISK_ONLY


def copy_df(df):
    """
    Copy spark DataFrame
    """
    
    dfc = df.toDF(*df.columns)
    
    return dfc


def Expr(*expr_list) -> list:
    """
    List of spark exprs
    """

    lst_expr = [expr(e) for e in expr_list]

    return lst_expr


def write_persist(df, path=None, storage_level=StorageLevel.DISK_ONLY, 
                  write_mode="overwrite", alias="", partition=None) -> DataFrame:
    """
    Write or persist spark DataFrame
    """

    if path is None:
        
        df = df.persist(storage_level)
        n = df.count()
            
        msg = "Persist DataFrame {2} with {1:,} rows: StorageLevel {0}!"
        msg = msg.format(storage_level, n, alias)
        logging(name="write_persist", message=msg)
        
        return df
    
    else:
        
        if partition is not None:
            
            df = df.repartition(partition)
        
        df.write.parquet(path, mode=write_mode)

        msg = "Write DataFrame to {0}: WriteMode {1}!"
        msg = msg.format(path, write_mode)
        logging(name="write_persist", message=msg)

        return None


def unpersist(df, alias="") -> None:
    """
    Unpersist spark DataFRame
    """

    try:

        df.unpersist()
        msg = "Unpersist DataFrame %s!" % alias
        logging(name="unpersist", message=msg)

    except:

        None
