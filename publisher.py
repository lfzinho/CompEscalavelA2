# Description: Produces fake data and sends it to Redis Stream
import time
import random
import redis

from faker import Faker
from faker_vehicle import VehicleProvider


fake = Faker()
fake.add_provider(VehicleProvider)

r = redis.Redis(host="localhost", port=6379, db=0, password="")

while True:
    # Contém a placa do veículo, a rodovia e a posição do veículo
    output = {
        "veiculo_placa": fake.license_plate(),
        "rodovia": fake.street_name(),
        "veiculo_posicao": str(fake.local_latlng(country_code="BR", coords_only=True)),
    }
    print(output)
    print(r.xadd("veiculo", output))
    time.sleep(random.randint(1, 10))
