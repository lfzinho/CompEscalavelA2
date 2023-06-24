class Publisher:

    def __init__(self):
        self.redis = redis.Redis(host="10.22.156.68", port=6381, db=0, password="1234")
    
    def send_message(self, message_name, message_content):
        r.xadd(message_name, message_content)




# Description: Produces fake data and sends it to Redis Stream
import time
import random
import redis

from faker import Faker
from faker_vehicle import VehicleProvider

fake = Faker("pt_BR")
fake.add_provider(VehicleProvider)

r = redis.Redis(host="10.22.156.68", port=6380, db=0, password="1234")

while True:
    # Contém a placa do veículo, a rodovia e a posição do veículo
    output = {
        "field": "brenao"
    }
    print(output)
    print(r.xadd("veiculo", output))
    # add stream into a database
    time.sleep(random.randint(1, 10))


