"""
Plot utilities for plotly libraries
"""
import plotly.graph_objects as go
import pandas as pd
import numpy as np

def plot_CDF(df, xtitle="", ytitle="", title="", clear_bound=True):
    """
    Plot the CDF for different class
    data should be a pandas dataframe, where each row is a set of data. 
    cut: Percent of CDF to show 1 means all
    """
    fig = go.Figure()
    max_v = float("-inf")
    min_v = float("inf")
    fig.update_layout(
        autosize=False,
        title={
            'text': title,
            'x':0.5,
            'yanchor': 'top'
        },
        xaxis_title=xtitle,
        yaxis_title=ytitle,
        width=800,
        height=600,
        font=dict(
            family="Time New Roman",
            size=16,
            color="#7f7f7f"
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        margin=go.layout.Margin(
            l=50,
            r=50,
            b=50,
            t=30,
            pad=4
        ),
    )
    for name, col in df.iteritems():
        sorted_col = col.sort_values()[col.notnull()]
        y = np.linspace(0, 1, len(sorted_col))
        min_v = min(min_v, sorted_col.iloc[0])
        max_v = max(max_v, sorted_col.iloc[-1])
        fig.add_trace(go.Scatter(x=sorted_col, y=y,  \
                                 mode='lines', name=name, line={'width': 3}))
    diff = (max_v - min_v) / 100
    if clear_bound: fig.update_xaxes(range=[min_v - diff, max_v + diff])
    fig.show()


def plot_bar(df, xtitle="", ytitle="", title="", idx='', stacked=False, unified=False):
    """
    idx: name of the key that plot has x index on
    unified: Only useful when stacked is on. Put all the stac into 100%
    """
    fig = go.Figure()
    if idx: df = df.set_index(idx)
    if stacked and unified:
        df_sum = df.sum(axis=1)
        df = df.div(df_sum, axis=0)
    for name, col in df.iteritems():
        fig.add_trace(go.Bar(name=name, x=df.index, y=col))
    fig.update_layout(
        autosize=False,
        title={
            'text': title,
            'x':0.5,
            'yanchor': 'top'
        },
        xaxis_title=xtitle,
        yaxis_title=ytitle,
        width=800,
        height=600,
        font=dict(
            family="Time New Roman",
            size=16,
            color="#7f7f7f"
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        margin=go.layout.Margin(
            l=50,
            r=50,
            b=50,
            t=30,
            pad=4
        ),
    )
    if stacked: fig.update_layout(barmode='stack')
    fig.show()