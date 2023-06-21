import redis


class Subscriber:

    def __init__(self):
        self.debug = False
        self.redis = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=1,
            decode_responses = True,
        )
        self.redis.flushdb()
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe("veiculo")

    def listen_and_save(self, n):
        for message in self.pubsub.listen():
            if message["type"] != "message":
                continue
            if self.debug:
                print(message)
            data = message['data'].split(",")
            key = f"{data[2]}:{data[0]}"
            value = f"{data[1]} {data[3]} {data[4]}"
            self.redis.set(key, value)
            n -= 1
            if n<=0:
                break


s = Subscriber()
s.listen_and_save(1000)