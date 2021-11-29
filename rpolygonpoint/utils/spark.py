from pyspark import StorageLevel, DataFrame
from pyspark.sql.functions import expr
from rpolygonpoint.utils import logging


def Expr(*expr_list) -> list:
    """
    List of spark exprs
    """

    lst_expr = [expr(e) for e in expr_list]

    return lst_expr


def write_persist(df, path=None, storage_level=StorageLevel.DISK_ONLY, 
                  write_mode="overwrite", alias="") -> DataFrame:
    """
    Write or persist spark DataFrame
    """

    if path is None:
        
        df = df.persist(storage_level)
        n = df.count()
            
        msg = "Persist DataFrame {2} with {1:,} rows: StorageLevel {0}!"
        msg.format(storage_level, n, alias)
        logging(name="write_persist", message=msg)
        
        return df
    
    else:
        
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
