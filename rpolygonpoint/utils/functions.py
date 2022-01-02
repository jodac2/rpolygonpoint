from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from rpolygonpoint.utils.utils import logging, to_list
from rpolygonpoint.utils.spark import spark, copy_df, write_persist, Expr, unpersist, _storage_level_


def get_polygon_point_sample(df_polygon, polygon_id="polygon_id", path=None, 
                             prop=0.1, limit=3, seed=None) -> DataFrame:
    """
    Generate random sample of polygon points
    ------
    
    Params
    ------
    df_polygon: spark DataFrame with polygon points
    polygon_id: string with column name of polygon ID
    path: string with path where spark DataFrame will be saved with the generated sample, 
        if it is None it persists
    prop: value in the range (0, 1) that indicates the desired proportion
    limit: minimum number of polygon points to be included in the sample
    seed: seed to use in the selection of the points
    
    Return
    ------
    df_point_sample: spark DataFrame with sample of polygon point
    """
    
    df_polygon = copy_df(df_polygon)

    _polygon_id = to_list(polygon_id)

    df_point_number = df_polygon\
        .groupBy(
            _polygon_id
        ).agg(expr(
            "count(1) as _number_"
        ))
    
    _seed = "" if seed is None else seed
    
    df_point_sample = df_polygon\
        .join(
            df_point_number, _polygon_id, "inner"
        ).selectExpr(
            "*", 
            "rand(%s) as _random_" % _seed
        ).selectExpr(
            "*", 
            "row_number() over (partition by %s order by _random_ desc) as _rank_" % ",".join(_polygon_id)
        ).filter(
            "_rank_/_number_ <= {0} or _rank_ <= {1}".format(prop, limit)
        ).drop(
            "_number_", "_random_", "_rank_"
        )
    
    df_point_sample = write_persist(
        df=df_point_sample, 
        path=path, 
        storage_level=_storage_level_,
        alias="PolygonPointSample"
    )
    
    return df_point_sample


def get_delimiter_rectangle(df_polygon, polygon_id="polygon_id", coords=["coord_x", "coord_y"], path=None, partition=1) -> DataFrame:
    """
    Polygon delimiter rectangle
    ------
        Get points of the rectangle that contains the polygon. 
        Additionally, the centroid of the polygon.
    
    Params
    ------
    df_polygon: spark DataFrame with polygon points
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    path: string with path where spark DataFrame will be saved with
        polygon delimiter rectangle and polygon centroid, if it is None it persists
    
    Return
    ------
    df_delimiter_rectangle: spark DataFrame with polygon delimiter rectangle
    """

    df_polygon = copy_df(df_polygon)

    _polygon_id = to_list(polygon_id)
    
    dict_funs = {"min": "min", "max": "max", "avg": "mean"}
    
    expr_str = [
        "{1}({0}) as {2}_{0}".format(ci, fi, ai)
        for ci in coords
            for ai, fi in dict_funs.items()
    ]
    
    df_delimiter_rectangle = df_polygon\
        .groupBy(
            _polygon_id
        ).agg(
            *Expr(*expr_str)
        )
    
    df_delimiter_rectangle = write_persist(
        df=df_delimiter_rectangle, 
        path=path, 
        storage_level=_storage_level_,
        alias="PolygonDelimietRectangle",
        partition=partition
    )
    
    return df_delimiter_rectangle


def get_polygon_side(df_polygon, polygon_id="polygon_id", coords=["coord_x", "coord_y"], 
                     point_seq="point_seq", path=None, order=False, partition=None) -> DataFrame:
    """
    Get polygon sides points
    ------
    
    Params
    ------
    df_polygon: spark DataFrame with polygon points
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    point_seq: string with column name of polygon point sequence
    path: string with path where spark DataFrame will be saved with
        polygon sides points, if it is None it persists
    order: logical value that indicates whether point_seq should be general. 
        Applies if a sample polygon point was generated
    
    Return
    ------
    df_polygon_side: spark DataFrame with polygon sides points
        the columns `end_(coords)` contain the end of the polygon side
    """
    
    df_polygon = copy_df(df_polygon)

    _polygon_id = to_list(polygon_id)

    if order:
        
        df_polygon = df_polygon\
            .selectExpr(
                "*", 
                "row_number() over (partition by {0} order by {1}) as _seq1_".format(",".join(_polygon_id), point_seq)
            )
    
    else:
        
        df_polygon = df_polygon\
            .selectExpr("*", "%s as _seq1_" % point_seq)
    
    # Ultimo punto del poligono como punto inicial
    df_polygon0 = df_polygon\
        .selectExpr(
            "*", 
            "row_number() over (partition by %s order by _seq1_ desc) - 1 as _seq_" % ",".join(_polygon_id)
        ).filter(
            "_seq_ = 0"
        ).drop("_seq1_")
    
    df_polygon2 = write_persist(
        df=df_polygon0.union(df_polygon), 
        storage_level=_storage_level_,
        alias="EndPolygonPoint"
    )
    
    logging(name="get_polygon_side", message="side end point")

    # Formar lados del poligono
    _condition = ["t0.{0} = t1.{0}".format(ci) for ci in _polygon_id]
    _condition = " and ".join(_condition)
    _condition += " and t0._seq_ + 1 = t1._seq_"

    df_polygon_sides = df_polygon2.alias("t0")\
        .join(
            df_polygon2.alias("t1"), 
            expr(_condition), 
            "inner"
        ).selectExpr(
            "t0.*", 
            "t1.{0} as end_{0}".format(coords[0]), 
            "t1.{0} as end_{0}".format(coords[1]), 
            "t1.{0} as end_{0}".format(point_seq)
        ).drop("_seq_")
    
    df_polygon_sides = write_persist(
        df=df_polygon_sides, 
        path=path, 
        storage_level=_storage_level_,
        alias="PolygonSidesPoint",
        partition=partition
    )
    
    unpersist(df_polygon2, "EndPolygonPoint")
    
    logging(name="get_polygon_side", message="polygon sides")

    return df_polygon_sides


def get_polygon_mesh2(df_rectangle, polygon_id="polygon_id", coords=["coord_x", "coord_y"], 
                     size=1, explode=True, prefix=None, path=None, partition=None) -> DataFrame:
    """
    Generar malla
    ------
        Generated polygon mesh

    Params
    ------
    df_container_rectangle: spark DataFrame with container rectangle
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    size: resolution of polygon mesh
    explode: if False mesh is retunr in array
    prefix: string with prefix
    path: string with path where spark DataFrame will be saved with
        container rectangles, if it is None it persists
    
    Return
    ------
    df_polygon_mesh: spark DataFrame with poygon mesh    
    """

    df_rectangle = copy_df(df_rectangle)

    _prefix = "" if prefix is None else prefix + "_"

    # Ocurren cosas raras cuando size=1, revisar
    _polygon_id = to_list(polygon_id)

    df_polygon_mesh = df_rectangle\
        .selectExpr(
            *_polygon_id, 
            "min_%s as min_x" % coords[0],
            "min_%s as min_y" % coords[1],
            "(max_{0} - min_{0})/{1} as size_x".format(coords[0], size),
            "(max_{0} - min_{0})/{1} as size_y".format(coords[1], size)
        ).selectExpr(
            "*",
            "explode(sequence(0, {0} - 1)) as x".format(size)
        ).selectExpr(
            "*", 
            "explode(sequence(0, {0} - 1)) as y".format(size)
        ).selectExpr(
            *_polygon_id,   
            "concat(array(array(x * size_x + min_x, y * size_y  + min_y), array(size_x, size_y))) as {0}cell_id".format(_prefix),
            """
            array(
                array(1, x * size_x + min_x, y * size_y  + min_y), 
                array(2, (x + 1) * size_x + min_x, y * size_y  + min_y), 
                array(3, (x + 1) * size_x + min_x, (y + 1) * size_y  + min_y), 
                array(4, x * size_x + min_x, (y + 1) * size_y  + min_y)
            ) as cell_vertex
            """
        )

    if explode:

        df_polygon_mesh = df_polygon_mesh\
            .selectExpr(
                *_polygon_id, 
                _prefix + "cell_id", 
                "explode(cell_vertex) as cell_vertex"
            ).selectExpr(
                *_polygon_id, 
                _prefix + "cell_id", 
                "cast(cell_vertex[0] as Integer) as point_seq", 
                "cell_vertex[1] as {0}".format(coords[0]), 
                "cell_vertex[2] as {0}".format(coords[1])
            )

    df_polygon_mesh = write_persist(
        df=df_polygon_mesh, 
        path=path,
        storage_level=_storage_level_,
        alias="PolygonMesh",
        partition=partition
    )

    logging(name="get_polygon_mesh2", message="polygon mesh")

    return df_polygon_mesh


def get_container_rectangle(df_point, df_delimiter_rectangle, polygon_id="polygon_id", add_cols=[],
                            coords=["coord_x", "coord_y"], path=None, partition=None, prefix=None) -> DataFrame:
    """
    Container rectangle    
    ------
        Identify container rectangles
    
    Params
    ------
    df_point: spark DataFrame with points to identify container rectangle
    df_delimiter_rectangle: spark DataFrame with delimiter rectangle
        generated by ´get_delimiter_rectangle´
    polygon_id: string with column name of polygon ID
    path: string with path where spark DataFrame will be saved with
        container rectangles, if it is None it persists
    coords: tupla with columns of polygon coordinates
    point_id: string with column name of point ID
    
    Return
    ------
    df_container_rectangle: spark DataFrame with container rectangle
    """

    df_point = copy_df(df_point)
    df_delimiter_rectangle = copy_df(df_delimiter_rectangle)
    
    is_inside = "({0} between min_{0} and max_{0}) and ({1} between min_{1} and max_{1})"
    is_inside = is_inside.format(*coords)
    
    _polygon_id = to_list(polygon_id)
    _prefix = "" if prefix is None else prefix + "_"

    cols = [pi for pi in _polygon_id if pi in df_point.columns]
    by_polygon = len(cols) == len(_polygon_id)

    if by_polygon:
        
        # Se especifica poligonos a valirdar
        df_container_rectangle = df_point.alias("t0")\
            .join(
                df_delimiter_rectangle.alias("t1"), 
                _polygon_id, 
                "inner"
            )

        _cols = ["t0.*"]
    
    else:

        # No se especifica polygonos a validar
        df_container_rectangle = df_point.alias("t0")\
            .crossJoin(
                df_delimiter_rectangle.alias("t1")
            )

        _cols = ["t0.*"] + ["t1.{0} as {1}{0}".format(ci, _prefix) for ci in _polygon_id]
    
    _add_cols = ["t1.{0} as {1}{0}".format(ci, _prefix) for ci in add_cols]

    df_container_rectangle = df_container_rectangle\
        .selectExpr(
            "*", 
            """
            case 
                when %s then 1
                else 0
            end as is_inside
            """ % is_inside
        ).filter(
            "is_inside = 1"
        ).selectExpr(
            _cols + _add_cols
        )
    
    df_container_rectangle = write_persist(
        df=df_container_rectangle, 
        path=path,
        storage_level=_storage_level_,
        alias="ContainerRectangle",
        partition=partition
    )
    
    logging(name="get_container_rectangle", message="container rectangle")

    return df_container_rectangle


def get_container_polygon(df_point, df_polygon_side, polygon_id="polygon_id", 
                          point_id="point_id", coords=["coord_x", "coord_y"], 
                          path=None, partition=None) -> DataFrame:
    """
    Container rectangle    
    ------
        Identify container polygon
        Ref: https://wrf.ecse.rpi.edu/Research/Short_Notes/pnpoly.html
    
    Params
    ------
    df_point: spark DataFrame with points to identify container rectangle
    df_polygon_side: spark DataFrame with polygon sides
        generated by ´get_polygon_side´
    polygon_id: string with column name of polygon ID
    point_id: string with column name of point ID
    path: string with path where spark DataFrame will be saved with
        container rectangles, if it is None it persists
    coords: tupla with columns of polygon coordinates
    
    Return
    ------
    df_container_polygon: spark DataFrame with container polygon
    """
    
    df_point = copy_df(df_point)
    df_polygon_side = copy_df(df_polygon_side)

    condition1 = "(t1.{0} > t0.{0}) != (t1.end_{0} > t0.{0})".format(coords[1])
    condition2 = "t0.{0} < (t1.end_{0} - t1.{0}) * (t0.{1} - t1.{1})/(t1.end_{1} - t1.{1}) + t1.{0}"
    condition2 = condition2.format(*coords)
    
    _polygon_id = to_list(polygon_id)
    _point_id = to_list(point_id)    
    
    cols = [pi for pi in _polygon_id if pi in df_point.columns]
    by_polygon = len(cols) == len(_polygon_id)

    if by_polygon:
        
        # Se especifica poligonos a valirdar
        df_container_polygon = df_point.alias("t0")\
            .join(
                df_polygon_side.alias("t1"), 
                _polygon_id, 
                "inner"
            )
    
    else:

        # No se especifica polygonos a validar
        df_container_polygon = df_point.alias("t0")\
            .crossJoin(
                df_polygon_side.alias("t1")
            )
    
    df_container_polygon = df_container_polygon\
        .filter(
            condition1
        ).filter(
            condition2
        ).groupBy(
            ["t0." + pi for pi in _point_id] + _polygon_id
        ).agg(*Expr(
            "cast(pow(-1, count(1) + 1) as Integer) as is_inside"
        )).filter(
            "is_inside = 1"
        ).drop("is_inside")
    
    by_cols = _point_id
    
    if by_polygon:
        by_cols += _polygon_id
    
    df_container_polygon = df_point\
        .join(
            df_container_polygon, by_cols, "inner"
        )
    
    df_container_polygon = write_persist(
        df=df_container_polygon, 
        path=path,
        storage_level=_storage_level_,
        alias="ContainerPolygon",
        partition=partition
    )
    
    logging(name="get_container_polygon", message="container polygon")

    return df_container_polygon


def get_cell_type(df_polygon_mesh, df_polygon_side, polygon_id="polygon_id", coords=["coord_x", "coord_y"], 
                  cell_id="cell_id", path=None, partition=None, outside=False, level=None) -> DataFrame:
    """
    Cell type
    ------
        Get cell type of mesh items
        1. Outside
        2. Inside
        3. Undecided

    Params
    ------
    df_polygon: spark DataFrame with polygon points
    df_polygon_side: spark DataFrame with polygon sides points
        the columns `end_(coords)` contain the end of the polygon side
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    cell_id: list with columns name of cell ID
    path: string with path where spark DataFrame will be saved with
        polygon sides points, if it is None it persists
    outside: logic value that indicate if cell type outside should be filtered
    
    Return
    ------
    df_cell_type: spark DataFrame with cell type
    """

    df_polygon_mesh = copy_df(df_polygon_mesh)
    df_polygon_side = copy_df(df_polygon_side)

    _polygon_id = to_list(polygon_id)
    _cell_id = to_list(cell_id)
    _point_seq = ["point_seq"]

    df_container_polygon = get_container_polygon(
        df_point=df_polygon_mesh, 
        df_polygon_side=df_polygon_side, 
        polygon_id=_polygon_id,
        point_id=_cell_id + _point_seq, 
        coords=coords
    )

    logging(name="get_cell_type", message="container polygon")

    df_cell_type1 = df_container_polygon\
        .groupBy(
            _polygon_id + _cell_id
        ).count(
        ).selectExpr(
            "*", 
            "case count when 4 then 'inside' else 'undecided' end as cell_type"
        ).drop("count")
    
    # Algunas de las celdas tipo outside pueden ser clasificadas de manera erronea
    # debido a que no tienen ninguno vertice dentro del poligono, pero si tiene
    # puntos del poligono, el siguiente codigo reclasifica a tipo undecided

    ############################################################################
    # OUTSIDE TO UNDECIDE
    ############################################################################
    
    # Identificar celdas tipo outside que contienen al menos un punto del poligono
    df_outside_pre = df_polygon_mesh\
        .join(
            df_cell_type1, 
            _polygon_id + _cell_id, 
            "left_anti"
        )
    
    df_delimiter_rectangle_out = get_delimiter_rectangle(
        df_polygon=df_outside_pre, 
        polygon_id=_polygon_id + _cell_id, 
        coords=coords
    )

    logging(name="get_cell_type", message="delimiter rectangle out")

    df_container_rectangle_out = get_container_rectangle(
        df_point=df_polygon_side, 
        df_delimiter_rectangle=df_delimiter_rectangle_out, 
        polygon_id=_polygon_id, 
        coords=coords,
        add_cols=["cell_id"]
    )

    logging(name="get_cell_type", message="container rectangle out")

    df_undecided_ret = df_container_rectangle_out\
        .select(
            _polygon_id + _cell_id
        ).distinct(
        ).selectExpr(
            "*", 
            "'undecided' as cell_type"
        )
    
    df_cell_type2 = df_outside_pre\
        .drop(
            "cell_type"
        ).join(
            df_undecided_ret, 
            _polygon_id + _cell_id, 
            "left"
        ).fillna(
            "outside", subset=["cell_type"]
        )
    ############################################################################
    # END OUTSIDE TO UNDECIDE
    ############################################################################
    
    df_cell_type = df_cell_type1\
        .union(
            df_cell_type2.select(df_cell_type1.columns)
        )

    df_cell_type = df_polygon_mesh\
        .join(
            df_cell_type, 
            _polygon_id + _cell_id, 
            "left"
        ).select(
            df_polygon_mesh.columns + ["cell_type"]
        )
    
    if not outside:

        df_cell_type = df_cell_type\
            .filter("cell_type != 'outside'")
    
    if level is not None:
        
        df_cell_type = df_cell_type\
                .selectExpr(
                    "*", 
                    "%s as cell_level" % level
                )
    
    df_cell_type = write_persist(
        df=df_cell_type, 
        path=path,
        storage_level=_storage_level_,
        alias="PolygonMeshCellType", 
        partition=partition
    )

    unpersist(df_container_polygon, "ContainerPolygon")
    unpersist(df_delimiter_rectangle_out, "DelimiterRectangleOutside")
    unpersist(df_container_rectangle_out, "ContainerRectangleOutside")

    logging(name="get_cell_type", message="mesh cell type")

    return df_cell_type


def get_polygon_area(df_polygon_side, polygon_id="polygon_id", coords=["coord_x", "coord_y"]) -> DataFrame:
    """
    Polygon area
    """
    
    df_polygon_area = df_polygon_side\
        .selectExpr(
            "*", 
            "{0}*end_{1} - {1} * end_{0} as area".format(*coords)
        ).groupBy(
            polygon_id
        ).agg(expr(
            "abs(sum(area)/2) as polygon_area"
        ))
    
    return df_polygon_area


def get_polygon_continue(df_polygon_mesh, df_polygon_area, level, polygon_id="polygon_id", coords=["coord_x", "coord_y"], prop=0.1) -> DataFrame:
    """
    Polygots to continue splir cells
    ------
    
    Params
    ------
    df_polygon_mesh: spark DataFrame with poygon mesh
    level: integer number level to evaluate
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    prop: earned rate of inside area to continue split cell mesh
    
    Return
    ------
    df_polygon_continue: spark DataFrame with polygons to continue split mesh cells
    """
    
    _polygon_id = to_list(polygon_id)
     
    df_cell_dr = get_delimiter_rectangle(
        df_polygon=df_polygon_mesh.filter("cell_type = 'inside'"), 
        polygon_id=_polygon_id + ["cell_id", "cell_level"], 
        coords=coords
    ).selectExpr(
        *_polygon_id, 
        "cell_level",
        "(max_{0} - min_{0}) * (max_{1} - min_{1}) as cell_area".format(*coords)
    )
    
    df_area = df_cell_dr\
        .groupBy(
            _polygon_id
        ).agg(expr(
            "sum(cell_area) as all_area_inside"
        ))
    
    df_area_last = df_cell_dr\
        .filter("cell_level = %s " % level)\
        .groupBy(
            _polygon_id
        ).agg(expr(
            "sum(cell_area) as area_inside"
        ))
    
    df_continue = df_polygon_area\
        .join(
            df_area, polygon_id, "left"
        ).join(
            df_area_last, polygon_id, "left"
        ).fillna(
            0
        ).selectExpr(
            "*",
            "polygon_area - all_area_inside as rest_area_undecided",
            "area_inside/polygon_area as prop_inside", 
            "(polygon_area - all_area_inside)/polygon_area as prop_undecided"
        )
    
    df_continue = df_continue.filter(
            "(prop_inside >= {0} and prop_undecided >= {0}) or prop_undecided = 1.0".format(prop)
        ).select(
            polygon_id
        ).distinct()
    
    df_polygon_continue = write_persist(
        df=df_continue,
        alias="ContinueSplitPolygonMesh-Level % s" % level
    )
    
    unpersist(df_cell_dr)
    
    return df_polygon_continue


def get_polygon_mesh(df_delimiter_rectangle, df_polygon_side, polygon_id="polygon_id", coords=["coord_x", "coord_y"], outside=False, 
                     split=2, level=None, path=None, partition=None, load=False, prop=0.1) -> DataFrame:
    
    """
    Generar malla
    ------
        Generated mesh cell type
    
    Params
    ------
    df_delimiter_rectangle: spark DataFrame with container rectangle
    df_polygonside: spark DataFrame with polygon sides
    polygon_id: string with column name of polygon ID
    coords: tupla with columns of polygon coordinates
    split: integer or list to split cells 
    level: integer to iterations number
    outside: logic value that indicate if cell type outside should be filtered
    path: string with path where spark DataFrame will be saved with
        container rectangles, if it is None it persists
    load: logical value that indecated if current polygon mesh must be loaded
    prop: earned rate of inside area to continue split cell mesh
    
    Return
    ------
    df_polygon_mesh: spark DataFrame with poygon mesh    
    """
    
    df_delimiter_rectangle = copy_df(df_delimiter_rectangle)
    df_polygon_side = copy_df(df_polygon_side)

    if type(split) is not list:
        
        _split = level * [split]
        
    else:
        
        _split = split + (level - len(split)) * split[-1:]
    
    if load is False:
        # get initial polygon mesh
        
        level = level - 1 
        
        df_polygon_mesh2 = get_polygon_mesh2(
            df_rectangle=df_delimiter_rectangle, 
            polygon_id=polygon_id,
            coords=coords,
            size=_split[0]
        )
        
        df_polygon_mesh = get_cell_type(
            df_polygon_mesh=df_polygon_mesh2, 
            df_polygon_side=df_polygon_side,
            polygon_id=polygon_id,
            coords=coords,
            path=path,
            outside=outside,
            level=1
        )
        
        unpersist(df_polygon_mesh2, "PolygonMesh")
        
        if  level > 1 and path is None:
            
            logging(name="get_polygon_mesh", message="Level > 0 but path is requested not null!")
            
            return df_polygon_mesh
    
    _polygon_id = to_list(polygon_id)
    
    last_level = spark.read.parquet(path)\
            .selectExpr("max(cell_level)").first()[0]
    
    df_polygon_area = get_polygon_area(
        df_polygon_side=df_polygon_side,
        polygon_id=polygon_id,
        coords=coords
    )

    df_polygon_area = write_persist(
        df=df_polygon_area,
        alias="PolygonArea"
    )

    for lvl in range(level):
        # Iterations for split mesh cells
        
        current_level = last_level + lvl + 1
        
        logging(name="get_polygon_mesh", message="Level {1} of {0}".format(last_level + level, current_level))
        
        # Load and persist current polygon mesh
        df_polygon_mesh = spark.read.parquet(path)
        
        # polygos to continue split mesh cells
        df_polygon_continue =  get_polygon_continue(
            df_polygon_mesh=df_polygon_mesh, 
            df_polygon_area=df_polygon_area,
            level=current_level - 1, 
            polygon_id=polygon_id, 
            coords=coords, 
            prop=prop
        )
        
        n_continue = df_polygon_continue.count()
        
        logging(name="get_polygon_mesh", message="Polygons to continue split mesh cells {1} - Level {0}".format(current_level, n_continue))
        
        if n_continue == 0:
            
            unpersist(df_polygon_continue)
            
            break
        
        df_polygon_mesh = write_persist(
            df=df_polygon_mesh,
            alias="PolygonMesh-Level % s" % current_level
        )
        
        # Split mesh cells
        df_cell_undecided = df_polygon_mesh\
            .filter(
                "cell_type = 'undecided'"
            ).join(
                df_polygon_continue, polygon_id, "left_semi"
            )
        
        df_cell_dr = get_delimiter_rectangle(
            df_polygon=df_cell_undecided, 
            polygon_id=_polygon_id + ["cell_id"], 
            coords=coords
        )
        
        df_cell_mesh2 = get_polygon_mesh2(
            df_rectangle=df_cell_dr, 
            polygon_id=_polygon_id + ["cell_id"],
            coords=coords,
            size=_split[lvl if load else lvl + 1],
            prefix="lvl"
        )
        
        df_cell_mesh2 = df_cell_mesh2\
            .drop("cell_id")\
            .withColumnRenamed("lvl_cell_id", "cell_id")
        
        df_cell_mesh = get_cell_type(
            df_polygon_mesh=df_cell_mesh2, 
            df_polygon_side=df_polygon_side,
            polygon_id=polygon_id,
            coords=coords,
            outside=outside,
            level=current_level
        )
        
        # Update polygon mesh
        df_undecided = df_polygon_mesh\
            .filter(
                "cell_type = 'undecided'"
            ).join(
                df_polygon_continue, polygon_id, "left_anti"
            )
        
        df_polygon_mesh = df_polygon_mesh\
            .filter(
                "cell_type != 'undecided'"
            ).union(
                df_undecided
            ).union(
                df_cell_mesh
            )
        
        write_persist(
            df=df_polygon_mesh, 
            path=path,
            partition=partition
        )
        
        unpersist(df_polygon_mesh)
        unpersist(df_cell_dr)
        unpersist(df_cell_mesh2)
        unpersist(df_polygon_continue)
        unpersist(df_polygon_area)
    
    df_polygon_mesh = spark.read.parquet(path)
    
    return df_polygon_mesh
