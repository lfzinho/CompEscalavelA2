import plotly.express as px
import plotly.graph_objects as go
import redis
import random
import time
from datetime import datetime, timedelta

# ===== Constants =====
DEBUG = True
HIST_LIMIT = 2048

# ===== Data =====
hist_n_cars = []
hist_n_over_speed = []
hist_n_collisions_risk = []

# ===== Redis =====
r = redis.Redis(host='localhost', port=6379, db=0)

# ===== Getters =====
def update_hist(hist: list, time: int, new_val: int):
    """
    Updates the given list with the new value, removing the oldest value if the list is too long.
    """
    # If the last value is the same as the new one, don't update
    if len(hist) > 0 and hist[-1][0] == time:
        return
    # If the list is too long, remove the oldest value
    if len(hist) >= HIST_LIMIT:
        hist.pop(0)
    # Add the new value
    hist.append((time, new_val))

def get_n_roads():
    result = 0
    time_event = 0
    if DEBUG:
        result = random.randint(0, 20)
        time_event = time.time()
    else:
        result = r.get('n_roads')
        time_event = r.get('time_n_roads')
    return (time_event, result)

def get_n_cars():
    result = 0
    time_event = 0
    if DEBUG:
        result = random.randint(0, 200)
        time_event = time.time()
    else:
        result = r.get('n_cars')
        time_event = r.get('time_n_cars')
    update_hist(hist_n_cars, time_event, result)
    return (time_event, result)

def get_n_over_speed():
    result = 0
    time_event = 0
    if DEBUG:
        result = random.randint(0, 20)
        time_event = time.time()
    else:
        result = r.get('n_over_speed')
        time_event = r.get('time_n_over_speed')
    update_hist(hist_n_over_speed, time_event, result)
    return (time_event, result)

def get_n_collisions_risk():
    result = 0
    time_event = 0
    if DEBUG:
        result = random.randint(0, 20)
        time_event = time.time()
    else:
        result = r.get('n_collisions_risk')
        time_event = r.get('time_n_collisions_risk')
    update_hist(hist_n_collisions_risk, time_event, result)
    return (time_event, result)

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
    fig.update_layout(title='<b>📈 Dados Gerais</b>',
                        xaxis_title='Tempo',
                        yaxis_title='Número de ocorrências',
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
                            tickformat="%H:%M:%S",
                            # Range to five minutes
                            range = [datetime.now() - timedelta(seconds=10), datetime.now()]
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