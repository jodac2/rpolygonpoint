from pyspark.sql.functions import expr
from rpolygonpoint.utils.spark import spark, write_persist
from rpolygonpoint.utils.utils import logging
from rpolygonpoint.utils.random import spark_seed, gen_rnorm, print_range
from rpolygonpoint.utils.plot import plotly_polygon

class SetRandomPolygon(object):
    """
    Set methods por RandomPolygon
    """
    
    def __init__(self):
        """
        Default params and seed
        """

        self._spark = spark
        self.vertex_param = [6, 1.]
        self.vertex_seed = None
        self.scale_param = [1., 0.01]
        self.scale_seed = None
        self.translate_param = [0., 1.]
        self.translate_seed = None
        self.angle_param = [0., 0.01]
        self.angle_seed = None
        self.radius_param = [1, 0.01]
        self.radius_seed = None
    
    def set_n(self, n):
        self.n = n
    
    def set_vertex_param(self, param):
        self.vertex_param = param
    
    def set_vertex_seed(self, seed):
        self.vertex_seed = seed
    
    def set_scale_param(self, param):
        self.scale_param = param
    
    def set_scale_param(self, seed):
        self.scale_param = seed
    
    def set_translate_param(self, param):
        self.translate_param = param
    
    def set_translate_seed(self, seed):
        self.translate_seed = seed
    
    def set_angle_param(self, param):
        self.angle_param = param
    
    def set_angle_seed(self, seed):
        self.angle_seed = seed
    
    def set_radius_param(self, param):
        self.radius_param = param
    
    def set_radius_seed(self, seed):
        self.radius_seed = seed


class RandomPolygon(SetRandomPolygon):
    """
    Generated Random Polygon
    ------
    
    Params
    ------
    n: number of polygons
    vertex_param: list with mu and s to vertex number
    vertex_seed: int to vertex seed
    scale_param: list with mu and s to radius scale
    scale_seed: int to radius seed
    translate_param: list with mu and s to translate vertex
    translate_seed: int to translate vertex
    angle_param: list with mu and s to angle amplitude
    angle_seed: int to angle amplitude
    radius_param: list with  mu and s to radius length
    radius_seed: int to angle amplitude

    Return
    ------
    df_polygon: spark DataFrame with random polygons
    """
    
    def __init__(self, n=1):
        
        super().__init__()
        self.n = n
    
    def generated(self):
        """
        Generated random polygons
        """
        
        self._polygon_id()
        self._vertex_number()
        self._radius_scale()
        self._translate_vertex()
        self._vertex_sequence()
        self._angle_amplitude()
        self._radius_length()
        self._polygon()
    
    def write_persist(self, path=None):
        
        df_polygon = write_persist(
            df=self.df_polygon, 
            path=path, 
            alias="RandomPolygon"
        )
        
        return df_polygon
    
    def plotly(self, size=[600, 600]):
        
        fig = plotly_polygon(self.df_polygon, size)
        
        return fig
        
    def _polygon_id(self):
        """
        Polygon id
        ------
            Generated columns with polygon id: 'polygon_<00n>'
        """
        
        _polygon_pad = len(str(self.n))
        
        df_polygon_id = self._spark.sql(
            "select explode(sequence(1, %s)) as polygon" % self.n
        ).selectExpr(
            "concat('polygon_', lpad(polygon, %s, '0')) as polygon_id" % _polygon_pad
        )
        
        self._df_polygon_id = df_polygon_id
    
    def _vertex_number(self):
        """
        Random vertex number
        ------
            Generated polygon vertex number ~ N(vertex_param)
        """
        
        print_range(self.vertex_param, "vertex_number")

        df_vertex_number = gen_rnorm(
            df=self._df_polygon_id, 
            param=self.vertex_param, 
            name="_vertex_", 
            digit=0, 
            seed=spark_seed(self.vertex_seed)
        ).selectExpr(
            "*", 
            "cast(greatest(3, _vertex_) as Integer) as vertex_number"
        ).drop("_vertex_")
        
        self._df_vertex_number = df_vertex_number
    
    def _radius_scale(self):
        """
        Random raius scale
        ------
        Generated raius scale, scale = abs(s), s ~ N(scale_param)
        """
        
        print_range(self.scale_param, "radius_scale")

        df_radius_scale = gen_rnorm(
            df=self._df_vertex_number, 
            param=self.scale_param, 
            name="_scale_", 
            digit=2, 
            seed=spark_seed(self.scale_seed)
        ).selectExpr(
            "*", 
            "abs(_scale_) as radius_scale"
        ).drop("_scale_")
        
        self._df_radius_scale = df_radius_scale
    
    def _translate_vertex(self):
        """
        Generated vertex translate ~ N(translate_param)
        """
        
        print_range(self.translate_param, "translate_coord")
        
        df_vertex_translate = gen_rnorm(
            df=self._df_radius_scale, 
            param=self.translate_param, 
            name="translate_x", 
            digit=2, 
            seed=spark_seed(self.translate_seed)
        )
        
        seed = self.translate_seed
        _seed = None if seed is None else seed + 1
        
        df_vertex_translate = gen_rnorm(
            df=df_vertex_translate, 
            param=self.translate_param, 
            name="translate_y", 
            digit=2, 
            seed=spark_seed(_seed)
        )
        
        self._df_vertex_translate = df_vertex_translate
    
    def _vertex_sequence(self):
        """
        Generated vertex sequence
        """
        
        df_config = self._df_vertex_translate\
            .selectExpr(
                 "*", 
                "explode(sequence(1, vertex_number)) as vertex_seq"
            )
        
        self._df_config = df_config
    
    def _angle_amplitude(self):
        """
        Generated angle amplitude
        """
        
        print_range(self.angle_param, "angle_amplitude")

        df_angle = gen_rnorm(
            df=self._df_config, 
            param=self.angle_param, 
            name="angle_delta", 
            digit=5, 
            seed=spark_seed(self.angle_seed)
        ).selectExpr(
            "*", 
            "round(2 * 3.141592 * vertex_seq / vertex_number + angle_delta, 4) as angle_amplitude"
        ).drop("angle_delta")
        
        self._df_angle = df_angle
    
    def _radius_length(self):
        """
        Generated radius lenght ~ N(radius_param)
        """
        
        print_range(self.radius_param, "radius_length")

        df_radius_length = gen_rnorm(
            df=self._df_angle, 
            param=self.radius_param, 
            name="radius_length", 
            digit=2, 
            seed=spark_seed(self.radius_seed)
        )
        
        self._df_radius_length = df_radius_length
    
    def _polygon(self):
        """
        Generated cartessian cooordinates of polygon vertex
        """
        
        df_polygon = self._df_radius_length\
            .selectExpr(
                "polygon_id",
                "vertex_seq as point_seq", 
                "round(radius_length * cos(angle_amplitude) + translate_x, 1) as coord_x", 
                "round(radius_length * sin(angle_amplitude) + translate_y, 1) as coord_y"
            )
        
        self.df_polygon = df_polygon
