import redis

# initializing the redis instance
r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    decode_responses=True  # isso é pra códigos binários
)

while True:
    message = input("Mensagem canal: ")

    r.publish("canal-1", message)

