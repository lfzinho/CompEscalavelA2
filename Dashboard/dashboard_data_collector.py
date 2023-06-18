import plotly.express as px
import plotly.graph_objects as go
import random

# ===== Constants =====
DEBUG = True

# ===== Data =====
hist_n_cars = []
hist_n_over_speed = []
hist_n_collisions_risk = []

# ===== Getters =====
def get_n_roads():
    if DEBUG:
        return random.randint(0, 20)
    else:
        return # TODO

def get_n_cars():
    if DEBUG:
        return random.randint(0, 100)
    else:
        return # TODO

def get_n_over_speed():
    if DEBUG:
        return random.randint(0, 20)
    else:
        return # TODO

def get_n_collisions_risk():
    if DEBUG:
        return random.randint(0, 10)
    else:
        return # TODO

# ===== Graphs =====