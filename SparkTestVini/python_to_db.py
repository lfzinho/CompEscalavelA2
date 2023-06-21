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
        bar = n
        while n>0:
            try:
                for message in self.pubsub.listen():
                    if message["type"] != "message":
                        continue
                    if self.debug:
                        print(message)
                    data = message['data'].split(",")
                    key = f"{data[0]} {data[2]}"
                    value = f"{data[1]} {data[3]} {data[4]}"
                    self.redis.set(key, value)
                    n -= 1
                    if (bar-n)%(n/100)<2:
                        print(f"{1-n/bar} complete...", end="\r")
                    if n<=0:
                        break
            except ConnectionError:
                #tart again when connection times out
                pass

s = Subscriber()
s.listen_and_save(10000)