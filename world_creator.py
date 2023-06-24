import random

if __name__ == "__main__":
    roads_number = 10
    lanes_number_range = [1, 3]
    length_range = [100, 1000]
    max_speed_range = [12, 24]
    prob_of_new_car_range = [0.1, 0.3]
    prob_of_changing_lane_range = [0.1, 0.3]
    prob_of_collision_range = [0.01, 0.05]
    car_speed_range = [12, 24]
    car_acc_range = [-3, 3]
    collision_fix_time_range = [10, 30]


    with open("Simulator/world.txt", "w") as f:
        initial_road = 100
        for i in range(roads_number):
            f.write(f"BR-{initial_road + i} ")

            left_lanes_number = random.randint(lanes_number_range[0], lanes_number_range[1])
            right_lanes_number = random.randint(lanes_number_range[0], lanes_number_range[1])
            f.write(f"{left_lanes_number} {right_lanes_number}")

            length = random.randint(length_range[0], length_range[1])
            f.write(f" {length}")

            max_speed = random.randint(max_speed_range[0], max_speed_range[1])
            f.write(f" {str(max_speed).zfill(3)}")

            # Note: probabilities such as new_car, changinglane and collision must be multiplied by 100
            # That's because the file only stores integers
            prob_of_new_car = random.uniform(prob_of_new_car_range[0], prob_of_new_car_range[1])
            f.write(f" {str(int(prob_of_new_car*100)).zfill(2)}")

            prob_of_changing_lane = random.uniform(prob_of_changing_lane_range[0], prob_of_changing_lane_range[1])
            f.write(f" {str(int(prob_of_changing_lane*100)).zfill(2)}")

            prob_of_collision = random.uniform(prob_of_collision_range[0], prob_of_collision_range[1])
            f.write(f" {str(int(prob_of_collision*100)).zfill(2)}")

            car_speed_min = random.choice(range(car_speed_range[0]))
            car_speed_max = random.choice(range(car_speed_min+1, car_speed_range[1]))
            f.write(f" {str(car_speed_min).zfill(3)} {str(car_speed_max).zfill(3)}")

            car_acc_min = random.choice(range(car_acc_range[0], 0))
            car_acc_max = random.choice(range(1, car_acc_range[1]))
            f.write(f" {str(car_acc_min).zfill(3)} {str(car_acc_max).zfill(3)}")

            collision_fix_time = random.randint(collision_fix_time_range[0], collision_fix_time_range[1])
            f.write(f" {str(collision_fix_time).zfill(2)}")

            f.write("\n")
