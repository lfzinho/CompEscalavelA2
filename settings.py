




class Publisher:
    def __init__(self):
        self.redis = redis.Redis(host="10.22.160.187", port=6381, db=0, password="1234", decode_responses=True)
    def send_message(self, message_channel, message_content):
        self.redis.publish(message_channel, message_content)


