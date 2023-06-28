import multiprocessing
import threading
import random
import string
import time
import redis

class Publisher:
    def __init__(self):
        self.redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    def send_message(self, message_channel, message_content):
        self.redis.publish(message_channel, message_content)


class RoadNumberGetter:
    def __init__(self):
        self.redis = redis.Redis(
            host = "localhost",
            port = 6379,
            db = 3,
            decode_responses = True
        )
    def get_number_roads(self):
        value = self.redis.get("simulator_n_roads")
        if value is None:
            value = 1
        return value


WORLD_FILE = "Simulator/world.txt"

MODELS = {
    "beetle": {
        "SPEED_MIN" : 2,
        "SPEED_MAX" : 4,
        "ACCELERATION_MIN" : -1,
        "ACCELERATION_MAX" : 1,      
    },
    "voyage": {
        "SPEED_MIN" : 2,
        "SPEED_MAX" : 6,
        "ACCELERATION_MIN" : -2,
        "ACCELERATION_MAX" : 2,      
    },
    "nivus": {
        "SPEED_MIN" : 2,
        "SPEED_MAX" : 8,
        "ACCELERATION_MIN" : -3,
        "ACCELERATION_MAX" : 4,      
    },
}

def model_from_plate(plate):
    '''
    Returns the name of a car model given a plate
    '''

    # Turns the plate to an integer by summing the ascii value of each character
    n = sum([ ord(s) for s in plate ])

    # Gets an ordered list of the car model names
    keys = list(MODELS.keys())
    keys.sort()

    # Gets the desired index
    i = n%len(MODELS)

    # Returns the key at given index
    return keys[i]

class Car:
    def __init__(self, road: 'Road', lane: int, speed_min: int, speed_max:int, acceleration_min:int, acceleration_max:int, lane_change_prob, collision_risk: float, collision_countdown, plate_counter, publisher):
        # Road
        self.road = road

        # Car parameters
        self.plate = '' + str(random.randint(0,9)) + str(random.randint(0,9)) + str(random.randint(0,9))
        # self.plate = ''.join([random.choice(string.ascii_uppercase) for _ in range(3)]) + str(random.randint(0,9)) + random.choice(string.ascii_uppercase) + str(random.randint(0,9)) + str(random.randint(0,9))
        self.model = model_from_plate(self.plate)
        self.risk = collision_risk
        self.speed_min = speed_min
        self.speed_max = speed_max
        self.acceleration_min = acceleration_min
        self.acceleration_max = acceleration_max
        self.lane_change_prob = lane_change_prob

        # Car variables
        self._lane, self._length = self._pos = (lane, 0)
        self.speed = self.speed_min
        self.collided = False
        self.counter = collision_countdown
        self.cycle_flag = True

        self.plate_counter = plate_counter

        self.publisher = publisher
    
    @property
    def pos(self):
        '''
            Returns the position of the car.
        '''
        return self._pos

    @pos.setter
    def pos(self, new_value: tuple[int]):
        '''
            Sets the position attribute of the car and then
            update the car's position in it's road attribute.
            Also calls a lane and length update
        '''
        # Tells the road to move the car in it
        self.road.move(self._pos, new_value)

        # Saves the new position of the car
        self._pos = new_value
        # Saves in the other variables as well
        if self.lane != new_value[0]:
            self.lane = new_value[0]
        if self.length != new_value[1]:
            self.length = new_value[1]


    @property
    def lane(self):
        '''
            Returns the lane the car is on.
        '''
        return self._lane

    @lane.setter
    def lane(self, new_value):
        '''
            Updates the car's lane and calls a position update
        '''
        # Saves the new position of the car
        self._lane = new_value
        # Saves in self.pos as well
        if self.pos[0] != new_value:
            self.pos = new_value, self.length

    @property
    def length(self):
        '''
            Returns the length the car is on.
        '''
        return self._length

    @length.setter
    def length(self, new_value):
        '''
            Updates the car's length and calls a position update
        '''
        # Saves the new position of the car
        self._length = new_value
        # Saves in self.pos as well
        if self.pos[1] != new_value:
            self.pos = self.lane, new_value
    
    def cycle(self):
        while self.cycle_flag:
            self.accelerate()
            if not self.collided:
                self.decide_movement()
            else:
                self.deletion()
            self.send_pos()
            # time.sleep(1)

    def accelerate(self):
        '''
            Accelerates the car.

            Chooses a random acceleration, and adds it to the speed.
            Corrects the speed if it's below the minimum or above the maximum.
        '''

        # Chooses how much to accelerate
        acc = random.randint(self.acceleration_min, self.acceleration_max)

        # Accelerates
        self.speed += acc

        # If speed is below min or above max, correct it
        if self.speed < self.speed_min:
            self.speed = self.speed_min
        elif self.speed > self.speed_max:
            self.speed = self.speed_max
    
    def decide_movement(self):
        '''
            Decides which movement to do.

            This function checks if there is something on the way.
            If there's nothing, it moves foward.
            And if there's something, it tries to change lanes, which may cause a collision.

            If it changes lanes and it's way is still obstructed it deaccelerates.

            Afterwards, moves forward
            
            At the end, independently, changes lane arbitrarily
        '''

        # 'free' variable, will decide whether to move straight or do something else
        free = True
        force = random.random() < self.risk

        # Check if the path is empty from it's current position to it's current_position + speed
        free, length_new = self.road.path_is_empty(
            self.lane,
            self.length +1,
            self.length + self.speed
        )

        # Nothing was on the way or wanted to collide: move forward
        if free or force:
            self.length = length_new
            return
        # The following code only executes if the car didn't move already

        # Tries to change lanes, and if it fails, deaccelerates
        if not self.change_lane():
            self.speed += self.acceleration_min

        # Now, with new lane/speed (or not), gets maximum length traveled
        _, length_new = self.road.path_is_empty(self.lane, self.length +1, self.length + self.speed)
        self.length = length_new

        # Arbitrary lane change
        if random.random() < self.lane_change_prob:
            self.change_lane(force)
    
    def change_lane(self, force = False):
        '''
            Decides whether to change lanes, considering 
            availability of the lane and whether the driver is being careful
        '''

        available_lanes = []

        is_forward = 0 < self.lane < self.road.lanes_f - 1

        if is_forward:
            # If can diminish lane
            if (self.lane > 0) and (force or self.road.is_empty(self.lane-1, self.length)):
                available_lanes.append(self.lane-1)
            # If can increase lane
            if self.lane < (self.road.lanes_f - 1) and (force or self.road.is_empty(
                self.lane + 1,
                self.length)
            ):
                available_lanes.append(self.lane+1)
        else:
            # If can diminish lane
            if (self.lane > self.road.lanes_f) and (force or self.road.is_empty(self.lane-1, self.length)):
                available_lanes.append(self.lane-1)
            # If can increase lane
            if self.lane < (self.road.lanes_total - 1) and (force or self.road.is_empty(
                self.lane + 1,
                self.length)
            ):
                available_lanes.append(self.lane+1)

        # Returns if no lane is available
        if len(available_lanes) == 0:
            return False

        # Moves to a new lane
        self.lane = random.choice(available_lanes)
        return True


    def deletion(self):
        self.counter -= 1
        if self.counter <= 0:
            self.cycle_flag = False
            self.road.delete_car(self.pos)

    def send_pos(self):
        # message = {
        #     "plate" : self.plate,
        #     "pos" : f"{self.pos[0]}, {self.pos[1]}",
        #     "datetime" : time.time(),
        #     "road": self.road.name,
        # }
        message = f"{time.time()},{self.road.name},{self.plate},{self.pos[0]},{self.pos[1]}"
        self.publisher.send_message("veiculo", message)
        print(f"Road: {self.road.name} Plate: {self.plate} Pos:{self.pos}")
    
    def collide(self):
        self.collided = True
        self.counter = self.road.collision_countdown


class Road:
    
    def __init__(self, name, lanes_f, lanes_b, length, speed_limit, prob_of_new_car, prob_of_changing_lane, prob_of_collision, car_speed_min, car_speed_max, car_acc_min, car_acc_max, collision_fix_time, publisher):

        # Road parameters
        self.name = name
        self.lanes_f = lanes_f
        self.lanes_b = lanes_b
        self.lanes_total = lanes_f + lanes_b
        self.length = length
        self.speed_limit = speed_limit
        self.car_spawn_prob = prob_of_new_car
        self.road = [[None]*self.length for _ in range(self.lanes_total)]
        self.collision_countdown = collision_fix_time
        self.prob_of_changing_lane = prob_of_changing_lane
        self.prob_of_collision = prob_of_collision
        self.car_speed_min = car_speed_min
        self.car_speed_max = car_speed_max
        self.car_acc_min = car_acc_min
        self.car_acc_max = car_acc_max

        self.collision_total = 0

        self.car_min = 100
        self.car_max = 500
        self.car_counter = 0
        self.flag = True
        self.publisher = publisher

        # processes
        self.processes = {}

        self.debug_counter = 0
    
    def set_flag(self, value):
        self.flag = value

    def path_is_empty(self, lane, length_start, length_end) -> tuple[bool, int]:
        '''
            Returns whether a path is empty or not and the maximum length reached.
        '''
        # pylint: disable=invalid-name

        l = self.length - 1

        for l in range(length_start, length_end + 1):
            # Check whether the length is already out of the road
            if l >= self.length:
                return True, self.length - 1

            # Check whether length_n is empty
            if not self.is_empty(lane, l):
                return False, l

        return True, l

    def is_empty(self, lane: int, length:int) -> bool:
        '''
            Returns whether a position is empty or not.
        '''
        if length >= self.length:
            return True
        return self.road[lane][length] is None
    
    def move(self, position_old, position_new):
        '''
            Inside self.road, copies the car at position_old
            to position_new and then removes it from position_old
            There are four possibilities depending on position_new:
                position_new is beyond the end of the road: removes the car and returns
                position_new is empty: moves the car normally
                position_new contains a car: creates a collision
                position_new contains a collision: adds the car to the collision

            At the end the car will be removed from it's old position
        '''

        # Create some variables for readability
        lane_o, length_o = position_old
        lane_n, length_n = position_new
        object_o = self.road[lane_o][length_o]

        if length_n >= self.length - 1:
            self.delete_car([lane_o, length_o])
            return

        object_n = self.road[lane_n][length_n]
        # Check if new position is beyond the road

        # Check if new position is empty
        if self.is_empty(lane_n, length_n):
            # Creates a copy of the car in the new position
            self.road[lane_n][length_n] = object_o

        # Check if the position contains another car
        elif isinstance(object_n, Car):
            # Support variable
            # Note: it is important that the new position car comes first in the list
            # Because, at this moment, the old position car still has it's old position saved in it
            # The first car position is used when creating the 'Collision' object
            object_n.collide()
            object_o.collide()
            self.collision_total += 1

        # If didn't fall into any condition before, raise an error
        else:
            raise ValueError(f"Value at ({lane_n}, {length_n}) is {type(object_n)}." +
                " Expected None, Car or Collision")

        # Removes the occurence of the car in the old position
        self.road[lane_o][length_o] = None

    def process_print(self):
        print(f"""Road {self.name}
        Name: {self.name}
        Forward Lanes: {self.lanes_f}
        Backward Lanes: {self.lanes_b}
        Length: {self.length}
        Speed Limit: {self.speed_limit}
        Car Spawn Probability: {self.car_spawn_prob}
        Collision Countdown: {self.collision_countdown}
        Probability of Changing Lane: {self.prob_of_changing_lane}
        Probability of Collision: {self.prob_of_collision}
        Minimum Car Speed: {self.car_speed_min}
        Maximum Car Speed: {self.car_speed_max}
        Minimum Car Acceleration: {self.car_acc_min}
        Maximum Car Acceleration: {self.car_acc_max}
        Total Lanes: {self.lanes_total}
        ----""")

    def cycle(self):
        time.sleep(0.001)
        plate = 0
        while True:
        # for i in range(1):
            #mark
            for lane in range(self.lanes_total):
                if (random.random() < self.car_spawn_prob) and (self.road[lane][0] is None) and (self.car_counter < self.car_max) and (self.flag):
                    
                    car = Car(self, lane, self.car_speed_min, self.car_speed_max, self.car_acc_min, self.car_acc_max, self.prob_of_changing_lane, self.prob_of_collision, self.collision_countdown, plate, self.publisher)
                    # only creates the car if the car isn't already in the road
                    plate += 1
                    if not car.plate in self.processes:
                        # print(f"{self.name} Created car with plate {car.plate_counter}")
                        self.road[lane][0] = car
                        process = threading.Thread(target=car.cycle)
                        process.start()
                        self.car_counter += 1
                    break
    
    def delete_car(self, car_pos):
        if self.road[car_pos[0]][car_pos[1]] is not None:
            self.road[car_pos[0]][car_pos[1]].cycle_flag = False
            self.road[car_pos[0]][car_pos[1]] = None
            self.car_counter -= 1
        


class World:
    
    def __init__(self):
        self.roads: list[Road] = []
        self.processes = {}
        self.n_roads = 1
        self.created_roads = 0
        self.road_number_getter = RoadNumberGetter()
        self.publisher = Publisher()

        self.main()

    def create_road(self, attr):
        name = attr[0]
        lanes_f = int(attr[1])
        lanes_b = int(attr[2])
        length = int(attr[3])
        speed_limit = int(attr[4])
        prob_of_new_car = float(attr[5])/100
        prob_of_changing_lane = float(attr[6])/100
        prob_of_collision = float(attr[7])/100
        car_speed_min = int(attr[8])
        car_speed_max = int(attr[9])
        car_acc_min = int(attr[10])
        car_acc_max = int(attr[11])
        collision_fix_time = int(attr[12])

        # create road
        road = Road(name, lanes_f, lanes_b, length, speed_limit, prob_of_new_car, prob_of_changing_lane, prob_of_collision, car_speed_min, car_speed_max, car_acc_min, car_acc_max, collision_fix_time, self.publisher)
        
        self.roads.append(road)
        
    
    def create_all_roads(self):
        with open(WORLD_FILE, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for _ in range(self.created_roads, self.n_roads):
                l = lines[self.created_roads]
                # creates attr variable
                attr = l.split(' ')
                self.create_road(attr)
                self.created_roads += 1
    
    def create_processes(self):
        # while True:
        #     for road in self.roads:
        #         road.cycle()
        for road in self.roads:
            process = multiprocessing.Process(target=road.cycle)
            self.processes[road.name] =  process


    def start_processes(self):
        for road in self.roads:
            process = self.processes[road.name]
            print("ATE AQUI CHEGOU")
            process.start()
            print("ATE AQUI NAO")

    def join_processes(self):
        for road in self.roads:
            process = self.processes[road.name]
            process.join()

    def delete_roads(self):
        for r in self.roads:
            r.set_flag(False)

    def main(self):
        self.create_all_roads()
        self.create_processes()
        print(self.processes)
        self.start_processes()
        
        try:
            while True:
                new_n_roads = int(self.road_number_getter.get_number_roads())
                if new_n_roads<self.created_roads:
                    self.n_roads = new_n_roads
                    self.delete_roads()
                if new_n_roads>self.created_roads:
                    self.n_roads = new_n_roads
                    self.create_all_roads()
                    self.create_processes()
                    self.start_processes()
                time.sleep(5)
        except KeyboardInterrupt:
            self.delete_roads()
            self.join_processes()
            # for road in self.roads:
            #     self.processes[road.name].stop()



if __name__ == '__main__':
    w = World()