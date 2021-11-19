from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
from src.utils import Expr, write_persist, unpersist, to_list


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
        storage_level=StorageLevel.MEMORY_AND_DISK,
        alias="PolygonPointSample"
    )
    
    return df_point_sample


def get_delimiter_rectangle(df_polygon, polygon_id="polygon_id", coords=["coord_x", "coord_y"], path=None):
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

    _polygon_id = to_list(polygon_id)
    
    dict_funs = {"min": "min", "max": "max", "avg": "mean"}
    
    expr_str = [
        "{1}({0}) as {2}_{0}".format(ci, fi, ai)
        for ci in coords
            for ai, fi in dict_funs.items()
    ]
    
    df_delimiter_rectangle = df_polygon\
        .groupBy(_polygon_id)\
        .agg(*Expr(*expr_str))
    
    df_delimiter_rectangle = write_persist(
        df=df_delimiter_rectangle, 
        path=path, 
        storage_level=StorageLevel.MEMORY_AND_DISK,
        alias="PolygonDelimietRectangle"
    )
    
    return df_delimiter_rectangle


def get_polygon_side(df_polygon, polygon_id="polygon_id", coords=["coord_x", "coord_y"], 
                     point_seq="point_seq", path=None, order=False):
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
        path=path, 
        storage_level=StorageLevel.DISK_ONLY,
        alias="EndPolygonPoint"
    )
    
    # Formar lados del poligono
    df_polygon_sides = df_polygon2.alias("t0")\
        .join(
            df_polygon2.alias("t1"), 
            expr("t0._seq_ + 1 = t1._seq_"), 
            "inner"
        ).selectExpr(
            "t0.*", 
            "t1.{0} as end_{0}".format(coords[0]), 
            "t1.{0} as end_{0}".format(coords[1])
        ).drop("_seq_")
    
    df_polygon_sides = write_persist(
        df=df_polygon_sides, 
        path=path, 
        storage_level=StorageLevel.DISK_ONLY,
        alias="PolygonSidesPoint"
    )
    
    unpersist(df_polygon2, "EndPolygonPoint")
    
    return df_polygon_sides


def get_container_rectangle(df_point, df_delimiter_rectangle, polygon_id="polygon_id", 
                            coords=["coord_x", "coord_y"], path=None):
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
    
    is_inside = "({0} between min_{0} and max_{0}) and ({1} between min_{1} and max_{1})"
    is_inside = is_inside.format(*coords)
    
    _polygon_id = to_list(polygon_id)

    df_container_rectangle = df_point.alias("t0")\
        .crossJoin(
            df_delimiter_rectangle.alias("t1")
        ).selectExpr(
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
            "t0.*", 
            *["t1." + ci for ci in _polygon_id]
        )
    
    df_container_rectangle = write_persist(
        df=df_container_rectangle, 
        path=path,
        storage_level=StorageLevel.DISK_ONLY,
        alias="ContainerRectangle"
    )
      
    return df_container_rectangle


def get_container_polygon(df_point, df_polygon_side, polygon_id="polygon_id", 
                          point_id="point_id", coords=["coord_x", "coord_y"], 
                          path=None):
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
                _polygon_id, "inner"
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
        storage_level=StorageLevel.DISK_ONLY,
        alias="ContainerPolygon"
    )

    return df_container_polygon


def get_polygon_mesh(df_rectangle, polygon_id="polygon_id", coords=["coord_x", "coord_y"], size=1, explode=True, path=None):
    """
    Generar malla
    """

    # Ocurren cosas raras cuando size=1, revisar
    _polygon_id = to_list(polygon_id)

    df_polygon_mesh = df_rectangle\
        .selectExpr(
            *_polygon_id, 
            "array(floor(min_{0}/{1}), ceiling(max_{0}/{1}) - 1) as coord_x".format(coords[0], size),
            "array(floor(min_{0}/{1}), ceiling(max_{0}/{1}) - 1) as coord_y".format(coords[1], size)
        ).selectExpr(
            "*", 
            "explode(sequence(coord_x[0], coord_x[1])) as x"
        ).selectExpr(
            "*", 
            "explode(sequence(coord_y[0], coord_y[1])) as y"
        ).selectExpr(
            *_polygon_id, 
            "concat(lpad(x, length(coord_x[1]), '0'), ' ', lpad(y, length(coord_y[1]), '0')) as cell_id",  
            """
            array(
                array(1, x * {0}, y * {0}), 
                array(2, (x + 1) * {0}, y * {0}), 
                array(3, (x + 1) * {0}, (y + 1) * {0}), 
                array(4, x * {0}, (y + 1) * {0})
            ) as cell_vertex
            """.format(size)
        )

    if explode:

        df_polygon_mesh = df_polygon_mesh\
            .selectExpr(
                *_polygon_id, 
                "cell_id", 
                "explode(cell_vertex) as cell_vertex"
            ).selectExpr(
                *_polygon_id, 
                "cell_id", 
                "cast(cell_vertex[0] as Integer) as point_seq", 
                "cell_vertex[1] as " + coords[0], 
                "cell_vertex[2] as " + coords[1]
            )

    df_polygon_mesh = write_persist(
        df=df_polygon_mesh, 
        path=path,
        storage_level=StorageLevel.DISK_ONLY,
        alias="PolygonMesh"
    )

    return df_polygon_mesh


def get_cell_type(df_polygon_mesh, df_polygon_side, polygon_id="polygon_id", coords=["coord_x", "coord_y"], path=None):
    """
    Obtener tipo de celda de la malla
    """

    _polygon_id = to_list(polygon_id)

    df_container_polygon = get_container_polygon(
        df_point=df_polygon_mesh, 
        df_polygon_side=df_polygon_side, 
        polygon_id=_polygon_id,
        point_id=["cell_id", "point_seq"]
    )

    df_cell_type1 = df_container_polygon\
        .groupBy(
            [*_polygon_id, "cell_id"]
        ).count(
        ).selectExpr(
            "*", 
            "case count when 4 then 'inside' else 'undecided' end as cell_type"
        ).drop("count")
        
    df_cell_type = df_polygon_mesh\
        .join(
            df_cell_type1, [*_polygon_id, "cell_id"], "left"
        ).fillna(
            "outside", subset=["cell_type"]
        )

    df_cell_type = write_persist(
            df=df_cell_type, 
            path=path,
            storage_level=StorageLevel.DISK_ONLY,
            alias="PolygonMeshCellType"
        )

    unpersist(df_container_polygon, "ContainerPolygon")

    return df_cell_type
