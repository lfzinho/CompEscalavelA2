import plotly.express as px
import plotly.graph_objects as go
import redis 
import random 
import time 
from datetime import datetime, timedelta 
import pandas as pd 
from io import StringIO 
import string 

# ===== Constants =====
DEBUG = False
HIST_LIMIT = 1024

# ===== Data =====
hist_n_cars = []
hist_n_over_speed = []
hist_n_collisions_risk = []

# ===== Redis =====
r = redis.Redis(
    host='localhost',
    port=6379,
    db=1,
    decode_responses = True
)

# ===== Setters =====
def set_simulator_n_roads(n_roads: int):
    r.set('simulator_n_roads', n_roads)

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
        if time_event is not None:
            time_event = float(time_event)
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
        if time_event is not None:
            time_event =float(time_event)
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
        if time_event is not None:
            time_event =float(time_event)
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
        if time_event is not None:
            time_event = float(time_event)
    update_hist(hist_n_collisions_risk, time_event, result)
    return (time_event, result)

def get_list_over_speed():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        placas = ['ABC-1234', 'DEF-5678', 'GHI-9012', 'JKL-3456']
        velocidades = [random.randint(80, 120) for _ in range(len(placas))]
        riscos_colisao = [random.randint(0, 1) for _ in range(len(placas))]

        # Construindo a string de dados
        result = "Placa,Velocidade,Risco de colisão\n"
        for placa, velocidade, risco in zip(placas, velocidades, riscos_colisao):
            result += f"{placa},{velocidade},{risco}\n"

        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('list_over_speed')
        
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        result = result.head(5)
        time_event = r.get('time_list_over_speed')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

def get_list_collisions_risk():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        placas = ['ABC-1234', 'DEF-5678', 'GHI-9012', 'JKL-3456']
        velocidades = [random.randint(80, 120) for _ in range(len(placas))]

        # Construindo a string de dados
        result = "Placa,Velocidade\n"
        for placa, velocidade in zip(placas, velocidades):
            result += f"{placa},{velocidade}\n"

        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('list_collisions_risk')
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        result = result[result['colision_risk'] == 1]
        result = result.head(5)
        time_event = r.get('time_list_collisions_risk')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

def get_list_banned_cars():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        placas = ['ABC-1234', 'DEF-5678', 'GHI-9012', 'JKL-3456']

        # Construindo a string de dados
        result = "Placa\n"
        for placa in placas:
            result += f"{placa}\n"

        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('list_banned_cars')
        
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        result.head(5)
        time_event = r.get('time_list_banned_cars')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

def get_list_dangerous_cars():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        placas = ['ABC-1234', 'DEF-5678', 'GHI-9012', 'JKL-3456']

        # Construindo a string de dados
        result = "Placa\n"
        for placa in placas:
            result += f"{placa}\n"

        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('list_dangerous_cars')
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        result.head(5)
        time_event = r.get('time_list_dangerous_cars')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

def get_top_100():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        placas = ['ABC-1234', 'DEF-5678', 'GHI-9012', 'JKL-3456']
        # gerando 100 placas aleatórias
        placas += [f"{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}{random.randint(1000, 9999)}" for _ in range(100)]
        n_rodovias = [random.randint(1, 10) for _ in range(len(placas))]

        # Construindo a string de dados
        result = "Placa,Número de rodovias\n"
        for placa, n_rodovia in zip(placas, n_rodovias):
            result += f"{placa},{n_rodovia}\n"

        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('top_100')
        
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        time_event = r.get('time_top_100')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

def get_list_roads():
    result = ""
    time_event = 0
    if DEBUG:
        # Gerando dados aleatórios
        rodovias = ['BR-101', 'BR-116', 'BR-230', 'BR-232']
        mean_speeds = [random.randint(80, 120) for _ in range(len(rodovias))]
        mean_crossing = [random.randint(1, 10) for _ in range(len(rodovias))]
        accidents = [random.randint(1, 10) for _ in range(len(rodovias))]

        # Construindo a string de dados
        result = "Rodovia,Velocidade média dos carros,Tempo médio de travessia,Número de acidentes\n"
        for rodovia, mean_speed, mean_cross, accident in zip(rodovias, mean_speeds, mean_crossing, accidents):
            result += f"{rodovia},{mean_speed},{mean_cross},{accident}\n"

        result = pd.read_csv(StringIO(result))
        time_event = time.time()
    else:
        result = r.get('list_roads')
        
        if result is None: return (0, pd.DataFrame())
        result = pd.read_csv(StringIO(result))
        result = result.head(5)
        time_event = r.get('time_list_roads')
        if time_event is not None:
            time_event = float(time_event)
    return (time_event, result)

# ===== Graphs =====

def get_general_graph():
    """
    Returns a graph with overview data.
    """
    fig = go.Figure()

    # Add traces
    try:
        fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_cars],
                                    y=[x[1] for x in hist_n_cars],
                                    name="Cars"))
        fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_over_speed],
                                    y=[x[1] for x in hist_n_over_speed],
                                    name="Over speed"))
        fig.add_trace(go.Scatter(x=[datetime.fromtimestamp(x[0]) for x in hist_n_collisions_risk],
                                    y=[x[1] for x in hist_n_collisions_risk],
                                    name="Collisions risk"))
    except:
        pass
    
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
        get_list_over_speed()
        get_list_collisions_risk()
        get_list_banned_cars()
        get_list_dangerous_cars()
        get_top_100()
        get_list_roads()
        time.sleep(0.001)
        if DEBUG:
            print("generating data...", i, end="\r")
    
    fig = get_general_graph()
    fig.show()