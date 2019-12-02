"""
Algorithm for performing a  directed uniform random walking
"""
import random
import numpy as np
from queue import Queue
import multiprocessing


class DURW():
    def __init__(self, sampler, w=0.15, B=10000):
        """
        Sampler is a function for gathering a new sample
        w: random jumping probability
        B: # sample steps 
        """
        # TODO Find how to get pool of w
        self.sample_list = []
        self.sample.random_jump_pool = []
        self.B = B
         
