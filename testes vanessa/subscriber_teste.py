import redis

r = redis.Redis(
    host='10.22.180.106',
    port=6381,
    password='1234',
    db=0
)

mobile = r.pubsub()

#usando .subscribe() para se inscrever no canal que quer ouvir as mensagens
mobile.subscribe('veiculo')

# .listen() retorna um gerador sobre o qual você pode iterar e ouvir mensagens do editor
for message in mobile.listen():
    print(message['data']) # dá pra fazer o que quiser com message

print("Fim do programa")