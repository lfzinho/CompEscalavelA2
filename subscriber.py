# Log-based CDC

# A ideia seria criar um subscriber que fica escutando o stream de veiculos
# e salva em um banco de dados (?)

import redis

r = redis.Redis(host="localhost", port=6379, db=0, password="")

while True:
    print(r.xread({'veiculo': "$"}, count=1, block=50000))
