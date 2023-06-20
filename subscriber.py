# Log-based CDC

# A ideia seria criar um subscriber que fica escutando o stream de veiculos
# e salva em um banco de dados (?)

import redis

r = redis.Redis(host="10.22.224.145", port=6380, db=0, password="1234")

while True:
    print(r.xread({"veiculo": '$'}, count=1, block=50000))
