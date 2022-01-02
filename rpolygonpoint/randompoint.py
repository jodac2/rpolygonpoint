from rpolygonpoint.containerpolygon import ContainerPolygon, as_data_frame
from rpolygonpoint.utils.utils import to_list
from rpolygonpoint.utils.random import get_rand_u2, get_rand2_u2
from pyspark.sql.functions import expr
from rpolygonpoint.utils.functions import get_delimiter_rectangle
from rpolygonpoint.utils.spark import write_persist, unpersist


class SetRandomPoint(ContainerPolygon):
    """
    Set methods for RandomPoint
    """

    # Nombre de tablas para preprocesor
    _tbl_aceptation_rate = "t_rpp_aceptation_rate"
    
    # Ruta de tablas para preprocesor
    _path_aceptation_rate = None

    _comp = False
    
    def __init__(self, df_polygon=None, psize=100, seed=None):
        
        super().__init__()
        
        self.df_polygon = df_polygon
        self.psize = psize
        self.seed = seed
    
    def set_psize(self, size):
        self.psize = size
    
    def set_seed(self, seed):
        self.seed = seed
    
    def set_path_data(self, path):
        self.path_data = path
        self._set_paths()
        self._set2_paths()
    
    def _set2_paths(self):
        """
        Update paths to preprocesor
        """

        if self.path_data is not None:
            
            self._path_aceptation_rate = self.path_data + self._tbl_aceptation_rate

        else:
            
            self._path_aceptation_rate = None
        

class AceptationRadomPoint(SetRandomPoint):
    """
    Aceptation method to RandomPoint
    """
    
    def __init__(self, df_polygon=None):
        
        super().__init__()
        
        self.df_polygon = df_polygon
    
    def load_aceptation_rate(self):
        """
        Load preprocesor
        """

        self.df_aceptation_rate = self._spark.read.parquet(self._path_aceptation_rate)
    
    def get_aceptation_rate(self):

        _polygon_id = to_list(self.polygon_id)

        # Generate random points in cells undecided to stimate aceptation rate
        df_cells_undecided = self.df_polygon_mesh\
            .filter(
                "cell_type = 'undecided'"
            ).selectExpr(
                "*", 
                "%s  as size" % self.psize
            )

        df_undecided_rand = get_rand_u2(
            df_cells=df_cells_undecided, 
            polygon_id=self.polygon_id, 
            coords=self.coords, 
            seed=self.seed
        ).withColumnRenamed(
            "cell_id", "cell2_id"
        )

        df_prop_undecided = self.get_container_polygon(
            df_point=df_undecided_rand, 
            point_id=["cell2_id", "_index_"]
        )

        # Aceptation rate to all polygon mesh cells
        df_aceptation0_rate = df_prop_undecided\
            .groupBy(
                *_polygon_id, 
                expr("cell2_id as cell_id")
            ).count(
            ).selectExpr(
                "*", 
                "count/%s as rate" % self.psize
            ).drop("count")

        df_cells_dr = get_delimiter_rectangle(
                df_polygon=self.df_polygon_mesh, 
                polygon_id=_polygon_id + ["cell_id", "cell_level", "cell_type"],
                coords=self.coords
            )
        
        df_cell_area = df_cells_dr\
            .selectExpr(
                "*", 
                "(max_coord_x - min_coord_x) * (max_coord_y - min_coord_y) as cell_area"
            )
        
        df_polygon_area = df_cell_area\
                .groupBy(
                    self.polygon_id
                ).agg(expr(
                    "sum(cell_area) as polygon_area"
                ))
        
        df_cells = df_cell_area\
                .join(
                    df_polygon_area, self.polygon_id, "left"
                ).selectExpr(
                    "*", 
                    "cell_area/polygon_area as sample_prop"
                ).drop(
                    "cell_area", "polygon_area"
                )


        df_aceptation_rate = df_cells\
            .join(
                df_aceptation0_rate, 
                _polygon_id + ["cell_id"],
                "left"
            ).selectExpr(
                "*",
                """
                case cell_type 
                    when 'inside' then 1 
                    else rate 
                end as aceptation_rate
                """
            ).fillna(
                0, subset="aceptation_rate"
            ).drop("rate")
        
        df_aceptation_rate = write_persist(
            df=df_aceptation_rate,
            path=self._path_aceptation_rate,
            alias="AceptationRate"
        )

        unpersist(df_cells_dr)

        self.df_aceptation_rate = as_data_frame(df_aceptation_rate, self._path_aceptation_rate)


class RadomPoint(AceptationRadomPoint):
    """
    Mais class to RandomPoint
    """
    
    def __init__(self, df_polygon=None):
        
        super().__init__()
        
        self.df_polygon = df_polygon
    
    def sample(self, size, path=None, partition=None):
        """
        Generate sample of polygon
        """

        _polygon_id = to_list(self.polygon_id)
        
        df_sample_pre = get_rand2_u2(
            df_aceptation_rate=self.df_aceptation_rate, 
            size=size, 
            polygon_id=self.polygon_id, 
            coords=self.coords,
            path=path, 
            seed=self.seed,
            comp=self._comp
        ).withColumnRenamed(
            "cell_id", "cell2_id"
        ).withColumnRenamed(
            "cell_type", "cell2_type"
        )

        df_sample_in = df_sample_pre\
            .filter("cell2_type = 'inside'")

        df_sample_un = self.get_container_polygon(
            df_point=df_sample_pre.filter("cell2_type = 'undecided'"), 
            point_id=["cell2_id", "cell2_type", "rand_id"]
        )

        df_sample = df_sample_in\
            .union(
                df_sample_un.select(df_sample_in.columns)
            )

        df_sample = write_persist(
            df = df_sample,
            path=path,
            alias="RandomPoint"
        )

        unpersist(df_sample_pre)
        unpersist(df_sample_un)

        return df_sample
