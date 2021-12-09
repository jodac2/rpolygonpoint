from pyspark.sql import DataFrame
from rpolygonpoint.utils.spark import spark
from rpolygonpoint.utils.functions import  get_delimiter_rectangle
from rpolygonpoint.utils.functions import  get_polygon_side,
from rpolygonpoint.utils.functions import get_polygon_mesh

def is_data_frame(df, path):
    
    if type(df) is DataFrame:
       
        return df

    else:
       
        df = spark.read.parquet(path)
       
        return df


class SetContainerPolygon(object):
    """
    Set methods to class ContainerPolygon
    """

    _spark = spark
    
    # Nombre de tablas para preprocesor
    _tbl_delimiter_rectangle = "t_rpp_delimiter_rectangle"
    _tbl_polygon_side = "t_rpp_polygon_side"
    _tbl_polygon_mesh = "t_rpp_polygon_mesh"
    
    # Ruta de tablas para preprocesor
    _path_delimiter_rectangle = None
    _path_polygon_side = None
    _path_polygon_mesh = None
    
    def __init__(self, df_polygon=None):
        """
        Default values to parameters of class ContainerPolygon
        """

        self.df_polygon = df_polygon
        self.polygon_id = "polygon_id"
        self.coords = ["coord_x", "coord_y"]
        self.path_data = None
        self.point_seq = "point_seq"
        self.mesh_size = 1
        self.mesh_bsize = 2
        
        self.partition_delimiter_rectangle = 1
        self.partition_polygon_side = 1
        self.partition_polygon_mesh = 1
    
    def set_df_polygon(self, df):
        self.df_polygon = df
    
    def set_polygon_id(self, id):
        self.polygon_id = id
    
    def set_coords(self, coords):
        self.coords = coords
    
    def set_path_data(self, path):
        self.path_data = path
        self._set_paths()
    
    def set_point_seq(self, seq):
        self.point_seq = seq
    
    def set_mesh_size(self, size):
        self.mesh_size = size
    
    def set_mesh_bsize(self, size):
        self.mesh_bsize = size
    
    def _set_paths(self):
        """
        Update paths to preprocesor
        """

        if self.path_data is not None:
            
            self._path_delimiter_rectangle = self.path_data + self._tbl_delimiter_rectangle
            self._path_polygon_side = self.path_data + self._tbl_polygon_side
            self._path_polygon_mesh = self.ath_data + self._tbl_polygon_mesh
    
    def load_polygon_mesh(self):
        """
        Load preprocesor
        """

        self.df_delimiter_rectangle = self._spark.read.parquet(self._path_delimiter_rectangle)
        self.df_polygon_side = self._spark.read.parquet(self._path_polygon_side)
        self.df_polygon_mesh = self._spark.read.parquet(self._path_polygon_mesh)


class MeshContainerPolygon(SetContainerPolygon):
    """
    Method to get polygon mesh - cell type
    """
    
    def __init__(self):
        
        super().__init__()
    
    def get_polygon_mesh(self):
        """
        Polygon mesh - cell type
        """
    
        self._delimiter_reactangle()
        self._polygon_side()
        self._polygon_mesh()
    
    def _delimiter_reactangle(self):
        """
        Delimiter rectangle
        """

        df_delimiter_rectangle = get_delimiter_rectangle(
            df_polygon=self.df_polygon, 
            polygon_id=self.polygon_id, 
            coords=self.coords, 
            path=self._path_delimiter_rectangle,
            partition=self.partition_delimiter_rectangle
        )

        self.df_delimiter_rectangle = is_data_frame(df_delimiter_rectangle, self._path_delimiter_rectangle)
    
    def _polygon_side(self):
        """
        Polygon sides
        """
        
        df_polygon_side = get_polygon_side(
            df_polygon=self.df_polygon,
            polygon_id=self.self.polygon_id,
            coords=self.coords,
            point_seq=self.point_seq,
            path=self._path_polygon_side,
            partition=self.partition_polygon_side
        )

        self.df_polygon_side = is_data_frame(df_polygon_side, self._path_polygon_side)
    
    def _polygon_mesh(self):
        """
        Polygon mesh - cell type
        """
        
        df_polygon_mesh = get_polygon_mesh(
            df_delimiter_rectangle=self.df_delimiter_rectangle, 
            df_polygon_side=self.df_polygon_side, 
            polygon_id=self.polygon_id,
            coords=self.coords,
            size=self.mesh_size,
            bsize=self.mesh_bsize,
            path=self._path_polygon_mesh,
            partition=self.partition_polygon_mesh
        )

        self.df_polygon_mesh = is_data_frame(df_polygon_mesh, self._path_polygon_mesh)


class ContainerPolygon(MeshContainerPolygon):
    """
    Main class ContainerPolygon
    """
    
    def __init__(self, df_polygon=None):
        
        super().__init__()
        
        self.df_polygon = df_polygon
    
    def is_inside(self):

        """
        CODE
        """
