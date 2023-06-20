import redis

# ===== Redis =====

# Banco Histórico
r1 = redis.Redis(host='localhost', port=6379, db=0)
# Banco Pontual
r2 = redis.Redis(host='localhost', port=6379, db=1)
# Banco de Carros
r3 = redis.Redis(host='localhost', port=6379, db=2)

# ===== Setting for r1 =====

r1.zadd('seconds:road', 'car', 'position')
# segundos: tempo em segundos do envio da posição do carro.
# zadd: salva os dados no banco em um objeto de lista ordenada.

# ===== Setting for r2 =====

r2.hset('road', 'car', 'position')

# ===== Setting for r3 =====

r3.hset('car', 'data', 'value')
