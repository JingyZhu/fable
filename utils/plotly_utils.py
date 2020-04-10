"""
Plot utilities for plotly libraries
"""
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from urllib.parse import urlparse
from collections import defaultdict
import re
import os
import json

class SiteTree:
    """
    Generate a tree from the given urls
    The intermediate node will be elimiated if there is only one child (leaf is exception)
    Use plotly to plot the tree
    """
    def generate_tree(self):
        tree = lambda: defaultdict(tree)
        url_tree = tree()
        # Insert urls into 
        for obj in self.urls:
            url, status, broken = obj['url'], obj['status'], obj.get('sic_broken')
            up = urlparse(url)
            subhost, path, query = up.netloc.split(':')[0], up.path, up.query
            if path == '': path += '/'
            directories = [subhost] + path.split('/')[1:]
            cur_node = url_tree
            for d in directories[:-1]:
                cur_node = cur_node[d]
            filename = directories[-1] + '?' + query if query else directories[-1]
            cur_node[filename] = [status, broken]
        # Delete intermediate nodes with only one child (link list)
        json.dump(url_tree, open('tmp/tree_pre.json', 'w+'))
        stack = [url_tree]
        while len(stack) > 0:
            cur_node = stack.pop()
            if isinstance(cur_node, list): continue
            keys = list(cur_node.keys())
            for k in keys:
                cur_k, v = k, cur_node[k]
                need_cut = False
                while not (len(v) > 1 or isinstance(v, list)):
                    sub_k, v = list(v.keys())[0], list(v.values())[0]
                    cur_k = '{}/{}'.format(cur_k, sub_k)
                    cur_node[cur_k] = v
                    need_cut = True
                if need_cut: del(cur_node[k])
                stack.append(cur_node[cur_k])
        json.dump(url_tree, open('tmp/tree.json', 'w+'))
        return url_tree

    def update_urls(self, urls):
        """urls should include status and broken metadata (refer to db_format-->url_status_implicit_broken)"""
        self.urls = urls
        self.tree = self.generate_tree()

    def __init__(self, urls):
        self.update_urls(urls)
    
    def plot_tree(self):
        # TODO imlement this function
        pass


def plot_CDF(df, xtitle="", ytitle="", title="", cut=1, clear_bound=True):
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
        length = len(col)
        sorted_col = col.sort_values()[col.notnull()]
        sorted_col = sorted_col[: int(length*cut)]
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

    
def plot_box(df, xtitle="", ytitle="", title=""):
    """
    Plot the boxplot for different class
    data should be a pandas dataframe, where each row is a set of data. 
    """
    fig = go.Figure()
    fig.update_layout(
        autosize=False,
        title={
            'text': title,
            'x':0.5,
            'yanchor': 'top'
        },
        xaxis_title=xtitle,
        yaxis_title=ytitle,
        width=1300,
        height=800,
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
        showlegend=False
    )
    for name, col in df.iteritems():
        length = len(col)
        col = col[col.notnull()]
        fig.add_trace(go.Box(y=col, name=name, boxpoints='all'))
    fig.show()