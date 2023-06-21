import redis
import pandas as pd
from io import StringIO

# Conectando ao servidor Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Chave para armazenar os dados
chave = 'meus_dados'

# Dados a serem adicionados
dados = """Placa,Velocidade,Risco de colisão
ABC-1234,80,0
DEF-5678,90,0
GHI-9012,100,0
JKL-3456,110,0"""

print(dados)

# Adicionando os dados ao Redis
redis_client.set(chave, dados)

# Recuperando os dados do Redis
dados_redis = redis_client.get(chave).decode('utf-8')
print(dados_redis)
# Criando o DataFrame
df = pd.read_csv(StringIO(dados_redis))

# Exibindo o DataFrame
print(df)
