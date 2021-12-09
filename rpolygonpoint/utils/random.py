from rpolygonpoint.utils.utils import logging


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

