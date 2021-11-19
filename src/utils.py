from pyspark import StorageLevel
from pyspark.sql import DataFrame
import datetime
import pytz


def str_sys_datetime() -> str:
    
    str_dt = datetime.datetime.now()\
        .strftime("%Y-%m-%d %I:%M:%S.%f %p")
    
    return str_dt


def Expr(*expr_list) -> list:
    
    lst_expr = [expr(e) for e in expr_list]
    
    return lst_expr


def write_persist(df, path=None, storage_level=StorageLevel.DISK_ONLY, write_mode="overwrite", 
                  alias="", message=True) -> DataFrame:
    
    if path is None:
        
        df = df.persist(storage_level)
        n = df.count()
        
        if message:
            
            msg = ">>> Persist DataFrame<{2}> with {1:,} rows: StorageLevel <{0}>!"
            print(msg.format(storage_level, n, alias))
        
        return df
    
    else:
        
        df.write.parquet(path, mode=write_mode)
        
        if message:
            print(">>> Write DataFrame to {0}: WriteMode <{1}>!".format(path, write_mode))
        
        return None


def unpersist(df, alias=""):
    
    try:
        df.unpersist()
        
        print("    ...unpersist DataFrame<%s>!" % alias)
        
    except:

        None


def to_list(x):

    _list = x.copy() if type(x) is list else [x]

    return _list
