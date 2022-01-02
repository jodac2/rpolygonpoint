from rpolygonpoint.utils.utils import logging, to_list
from rpolygonpoint.utils.spark import write_persist, unpersist
from rpolygonpoint.utils.functions import get_delimiter_rectangle
from pyspark.sql.functions import expr


def spark_seed(seed):
    """
    Retunr '' if seed is None
    """
    
    __seed = "" if seed is None else seed
    
    return __seed


def gen_rnorm(df, param=[0., 1.], name="rnom", digit=None, seed=None):
    """
    Generate random number N(mu, sigma)
    """
    
    expr_rand = "randn({2}) * {1} + {0}".format(*param, spark_seed(seed))
    
    if digit is not None:

        expr_rand = "round({0}, {1})".format(expr_rand, digit)
    
    expr_rand += " as " + name
    
    df_rand = df.selectExpr(
            "*", 
            expr_rand
        )

    return df_rand


def print_range(param, name, digit=1, s=3):
    """
    Print messa with range
    """
    
    __p = {"1": 0.6827, "2": 0.9545, "3": 0.9973}
   
    lower = param[0] - s * param[1]
    upper = param[0] + s * param[1]

    msg = "Values between {0:.{2}f} and {1:.{2}f} with probability ".format(lower, upper, digit)
    msg += str(__p[str(s)])
    
    logging(name=name, message=msg)


def expr_scale(name, from_range, to_range, digit=None):
    """
               (b-a)(x - min)
    f(x) = --------------  + a
              max - min
    """
    
    expr_str = "({3} - {2})/({1} - {0})*({4} - {0}) + {2}"
    expr_str = expr_str.format(*from_range, *to_range, name)
    
    if digit is not None:
        expr_str = "round({0}, {1})".format(expr_str, digit) 
    
    expr_str += " as " + name + "_scale"
    
    return expr_str


def rescale(df, name, scale=[0., 1.], digit=None):
    """
    Rescale to range scale
    """
    
    from_range = [*df.selectExpr("min(%s)" % name, "max(%s)" % name).first()]
    expr_str = expr_scale(name, from_range, scale, digit)
    
    return df.selectExpr("*", expr_str)


def expr_rand_u2(coord_x, coord_y, prefix="rand", seed=None):
    """
    Expresions to get random u2
    """

    expr_rand = "{0} + ({1} - {0}) * rand({3}) as {2}"
    
    rand_x = expr_rand.format(*coord_x, prefix + "_x", spark_seed(seed))
    
    seed_y = seed if seed is None else seed + 1
    
    rand_y = expr_rand.format(*coord_y, prefix + "_y", spark_seed(seed_y))
    
    return rand_x, rand_y


def get_rand_u2(df_cells, size="size", polygon_id="polygon_id", coords=["coord_x", "coord_y"], prefix="coord", path=None, seed=None):
    """
    Random number in mesh cell
    """
    
    _polygon_id = to_list(polygon_id)

    df_cells2 = get_delimiter_rectangle(
        df_polygon=df_cells, 
        polygon_id=_polygon_id + ["cell_id", size],
        coords=coords
    )
    
    rand_u2 = expr_rand_u2(
        coord_x=["min_" + coords[0], "max_" + coords[0]], 
        coord_y=["min_" + coords[1], "max_" + coords[1]], 
        prefix=prefix, 
        seed=seed
    )
    
    df_rand_u2 = df_cells2\
        .selectExpr(
            "*","explode(sequence(1, %s)) as _index_" % size
        ).selectExpr(
            *_polygon_id, 
            "cell_id", 
            "_index_",
            *rand_u2
        )
    
    df_rand_u2 = write_persist(
        df=df_rand_u2,
        alias="Cell-RandomPoint",
        path=path
    )
    
    unpersist(df_cells)
    
    return df_rand_u2


def get_rand2_u2(df_aceptation_rate, size, polygon_id="polygon_id", coords=["coord_x", "coord_y"], prefix="coord", path=None, seed=None):
    """
    Random number in mesh cell
    """
    
    _polygon_id = to_list(polygon_id)

    df_cell_size = df_aceptation_rate\
        .groupBy(
            polygon_id
        ).agg(
            expr("mean(aceptation_rate) as rate"),
            expr("count(1) as cell_number")
        ).selectExpr(
            "*", 
            "{0} / rate as rsize".format(size)
        ).selectExpr(
            *_polygon_id,
            "ceil(rsize/cell_number + log(10, rsize/cell_number + 1)) as cell_size"
        )
    
    rand_u2 = expr_rand_u2(
        coord_x=["min_" + coords[0], "max_" + coords[0]], 
        coord_y=["min_" + coords[1], "max_" + coords[1]], 
        prefix=prefix, 
        seed=seed
    )
    
    df_rand_u2 = df_aceptation_rate\
        .join(
            df_cell_size, polygon_id, "left"
        ).selectExpr(
            "*","explode(sequence(1, cell_size)) as _index_"
        ).selectExpr(
            *_polygon_id, 
            "cell_id", 
            "_index_",
            *rand_u2
        )
    
    df_rand_u2 = write_persist(
        df=df_rand_u2,
        alias="Cell-RandomPoint",
        path=path
    )
    
    return df_rand_u2
