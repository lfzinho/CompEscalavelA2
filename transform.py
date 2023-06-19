import redis

r = redis.Redis(host="localhost", port=6379, db=0, password="")

def extract_speed_and_acceleration(message):
    try:
        veiculo_placa = message[b'veiculo_placa']
        veiculo_posicao = message[b'veiculo_posicao']

        # Calculate speed and acceleration based on the position data
        # Replace the calculations with your actual logic
        speed = calculate_speed(veiculo_posicao)
        acceleration = calculate_acceleration(veiculo_posicao)

        return veiculo_placa, speed, acceleration
    except KeyError as _:
        print(f"KeyError: {str(_)}")
        return None, None, None

def calculate_speed(veiculo_posicao):
    # Replace with your actual speed calculation logic
    return 80  # Example: static speed of 80 km/h

def calculate_acceleration(veiculo_posicao):
    # Replace with your actual acceleration calculation logic
    return 5  # Example: constant acceleration of 5 m/s^2

while True:
    stream_messages = r.xread({"veiculo": '$'}, count=1, block=50000)
    for _, message in stream_messages:
        #print(message[0][1][b"veiculo_placa"])
        veiculo_placa, speed, acceleration = extract_speed_and_acceleration(message[0][1])
        if veiculo_placa is not None:
            print(f"Vehicle: {veiculo_placa}, Speed: {speed} km/h, Acceleration: {acceleration} m/s^2")
