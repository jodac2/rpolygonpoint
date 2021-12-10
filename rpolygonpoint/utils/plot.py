import matplotlib
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def add_polygon(plt, polygons, color="#00A394", alpha=0.5, style="-", width=1):
    """
    Add polygont to plot
    """

    ax, fig = plt
    
    # Generated polygons
    patches = []

    for polygon in polygons:

        polygon = Polygon(xy=polygon, closed=True)
        patches.append(polygon)

    p = PatchCollection(patches, alpha=alpha)
    p.set_facecolor(color)
    p.set_edgecolor(color)
    p.set_linestyles(style)
    p.set_linewidth(width)
    
    # Add plygons
    ax.add_collection(p)
    
    return ax, fig
      

def get_axis_limit(x, tick):
    """
    Get axis lim
    """

    x_min = min(x)
    x_max = max(x)

    c_max =  1 if x_max % tick != 0 else 0
    c_min = -1 if x_min % tick != 0 else 0
    limits = [(x_min//tick + c_min) * tick, (x_max//tick + c_max) * tick]

    return limits


def add_point(plt, x, y, color="red", alpha=1, marker="o", size=1.5):
    """
    Add points to plot
    """

    ax, fig = plt
    
    ax.scatter(x, y, color=color, marker=marker, alpha=alpha, linewidths=size)
    
    return ax, fig


def plot_polygon(polygons, tick=1, color="#00A394", alpha=0.5, style="-", width=1, figsize=(5, 5)):
    """
    Plot polygons
    """

    polygons = polygons.copy()
    
    _title_color = "#00396C"
    _grid_color = "gray"
    
    fig = plt.figure(num="Polygons", figsize=figsize)
    ax = fig.add_subplot(111)
    ax.set_aspect("equal")
    
    # Labels format
    ax.set_title("Polygons", fontsize=20, fontweight="bold", color=_title_color)
    ax.set_xlabel("coord_x", fontsize=10, fontweight="bold", color=_title_color)
    ax.set_ylabel("coord_y", fontsize=10, fontweight="bold", color=_title_color)
    
    # Box format
    _spines = ["top", "left", "bottom", "right"]

    for s in _spines:

        # ax.spines[s].set_visible(False)
        ax.spines[s].set_color(_grid_color)
        ax.spines[s].set_linestyle(":")
        ax.spines[s].set_linewidth(1)
        ax.spines[s].set_alpha(0.4)
    
    # Axis limit
    x_lim, y_lim = np.apply_along_axis(lambda x: get_axis_limit(x, tick), 0, np.concatenate(polygons)).transpose()
    ax.set_xlim(x_lim)
    ax.set_ylim(y_lim)
    
    # Draw mesh
    ax.grid(which="major", axis="both", linestyle=":", color=_grid_color, linewidth=1, alpha=0.5)
    ax.set_xticks(np.arange(*x_lim, tick))
    ax.set_yticks(np.arange(*y_lim, tick))
    ax.tick_params(axis="both", which="major", labelsize=7, colors=_grid_color, labelcolor="black")
    
    # Draw polygons
    ax, fig = add_polygon((ax, fig), polygons, color=color, alpha=alpha, style=style, width=width)
    
    return ax, fig


def plotly_polygon(df_polygon, size=[600, 600], polygon_id="polygon_id", coords=["coord_x", "coord_y"], point_seq="point_seq"):
    """
    Plot polygons
    """
    
    lst_polygon = df_polygon\
        .groupBy(
            polygon_id
        ).agg(expr(
            "array_sort(collect_list(array({2}, {0}, {1}))) as point".format(*coords, point_seq)
        )).collect()
    
    fig = go.Figure()

    for polygon in lst_polygon:
        
        df_polygon = pd.DataFrame(polygon[1], columns=["point_seq", "coord_x", "coord_y"])
        
        fig.add_trace(go.Scatter(
            x=df_polygon["coord_x"], 
            y=df_polygon["coord_y"], 
            mode="lines", 
            name=polygon[0], 
            fill="toself", 
            line=dict(width=1, dash="dash")
        ))

    fig.update_layout(width=size[0], height=size[1], showlegend=True)

    return fig
