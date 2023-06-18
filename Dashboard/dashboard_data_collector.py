import plotly.express as px
import plotly.graph_objects as go
import random
import time
from datetime import datetime

# ===== Constants =====
DEBUG = True
HIST_LIMIT = 50

# ===== Data =====
hist_n_cars = []
hist_n_over_speed = []
hist_n_collisions_risk = []

# ===== Getters =====
def update_hist(hist: list, time: int, new_val: int):
    """
    Updates the given list with the new value, removing the oldest value if the list is too long.
    """
    if len(hist) >= HIST_LIMIT:
        hist.pop(0)
    hist.append((time, new_val))

def get_n_roads():
    result = 0
    delay = 0
    if DEBUG:
        result = random.randint(0, 20)
        now = time.time()
    else:
        result = ... # TODO
    return (time, now)

def get_n_cars():
    result = 0
    if DEBUG:
        result = random.randint(0, 200)
        now = time.time()
    else:
        result = ... # TODO
    update_hist(hist_n_cars, now, result)
    return (now, result)

def get_n_over_speed():
    result = 0
    if DEBUG:
        result = random.randint(0, 20)
        now = time.time()
    else:
        result = ... # TODO
    update_hist(hist_n_over_speed, now, result)
    return result

def get_n_collisions_risk():
    result = 0
    if DEBUG:
        result = random.randint(0, 20)
        now = time.time()
    else:
        result = ... # TODO
    update_hist(hist_n_collisions_risk, now, result)
    return result

# ===== Graphs =====

def get_general_graph():
    """
    Returns a graph with overview data.
    """
    fig = go.Figure()

    # Add traces
    fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_cars],
                                y=[x[1] for x in hist_n_cars],
                                name="Cars"))
    fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_over_speed],
                                y=[x[1] for x in hist_n_over_speed],
                                name="Over speed"))
    fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_collisions_risk],
                                y=[x[1] for x in hist_n_collisions_risk],
                                name="Collisions risk"))
    
    # Edit the layout
    fig.update_layout(title='General data',
                        xaxis_title='Time',
                        yaxis_title='Number of cars',
                        # Hover mode
                        hovermode="x",
                        # Range selector
                        xaxis=dict(
                            rangeselector=dict(
                                buttons=list([
                                    dict(count=1,
                                        label="1sec",
                                        step="second",
                                        stepmode="backward"),
                                    dict(count=1,
                                        label="1min",
                                        step="minute",
                                        stepmode="backward"),
                                    dict(count=5,
                                        label="5min",
                                        step="minute",
                                        stepmode="backward"),
                                    dict(step="all")
                                ])
                            ),
                            rangeslider=dict(
                                visible=True
                            ),
                            type="date",
                            # Tick format
                            tickformat="%H:%M:%S"
                        ),
                        # Background
                        paper_bgcolor='rgba(0,0,0,0)',
                        # Font
                        font=dict(
                            color='#1E1E1E'
                        )
                    )

    return fig

# ===== Main =====
if __name__ == "__main__":
    """
    For testing purposes.
    """
    for i in range(50):
        get_n_cars()
        get_n_over_speed()
        get_n_collisions_risk()
        time.sleep(0.001)
        print("generating data...", i, end="\r")
    
    fig = get_general_graph()
    fig.show()