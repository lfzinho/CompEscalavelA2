import redis


class Subscriber:

    def __init__(self):
        self.debug = True
        self.redis = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=0
        )
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe("veiculo")

    def listen_and_save(self):
        for message in self.pubsub.listen():
            if self.debug:
                print(message)
            self.redis.set("veiculo", message["data"])


s = Subscriber()
s.listen_and_save()