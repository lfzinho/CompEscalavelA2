import redis

r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    decode_responses=True
)

mobile = r.pubsub()

#usando .subscribe() para se inscrever no canal que quer ouvir as mensagens
mobile.subscribe('canal-1')

# .listen() retorna um gerador sobre o qual você pode iterar e ouvir mensagens do editor
for message in mobile.listen():
    print(message) # dá pra fazer o que quiser com message