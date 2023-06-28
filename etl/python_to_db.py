import redis


class Subscriber:

    def __init__(self):
        self.debug = True
        self.redis = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
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
                    value = f"{data[1]} {data[3]} {data[4]} {data[5]} {data[6]}"
                    self.redis.set(key, value)
                    n -= 1
                    if (bar-n)%(n+1/100)<2:
                        print(f"{1-n/bar} complete...", end="\r")
                    if n<=0:
                        break
            except ConnectionError:
                # start again when connection times out
                pass

s = Subscriber()
s.listen_and_save(10000)