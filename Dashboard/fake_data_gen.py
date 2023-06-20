import redis
import random
import time

r = redis.Redis(host='localhost', port=6379, db=0)

while True:
    r.set('n_roads', random.randint(0, 20))
    r.set('time_n_roads', time.time() - 100)

    r.set('n_cars', random.randint(0, 200))
    r.set('time_n_cars', time.time() - 100)

    r.set('n_over_speed', random.randint(0, 20))
    r.set('time_n_over_speed', time.time() - 100)

    r.set('n_collisions_risk', random.randint(0, 20))
    r.set('time_n_collisions_risk', time.time() - 100)
    
    time.sleep(100)

