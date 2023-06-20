# Description: Produces fake data and sends it to Redis Stream
import time
import random
import redis

from faker import Faker
from faker_vehicle import VehicleProvider

fake = Faker("pt_BR")
fake.add_provider(VehicleProvider)

r = redis.Redis(host="10.22.224.145", port=6380, db=0, password="1234")

while True:
    # Contém a placa do veículo, a rodovia e a posição do veículo
    output = {
        "veiculo_placa": fake.license_plate(),
        "rodovia": fake.street_name(),
        "veiculo_posicao": str(fake.local_latlng(country_code="BR", coords_only=True)),
    }
    print(output)
    print(r.xadd("veiculo", output))
    # add stream into a database
    time.sleep(random.randint(1, 10))
