# Log-based CDC

# A ideia seria criar um subscriber que fica escutando o stream de veiculos
# e salva em um banco de dados (?)

# HEDLER

import redis

r = redis.Redis(host="10.22.156.68", port=6380, db=0, password="1234")

def count_plate(plate):
    plt = plate[b'plate']
    num = r.get(plt)
    if num is None:
        r.set(plt, 1)
    else:
        r.set(plt, int(num) + 1)
    # Printing the number of times the same vehicle has been seen
    print(f"Vehicle {plt} has been seen {r.get(plt)} times")

while True:
    output = r.xread({"veiculo": '$'}, count=1, block=50000)
    # print(output)
    # print(f"Placa: {str(output[0][1][0][1][b'plate'])}", end="\t")
    # print(f"Rodovia: {str(output[0][1][0][1][b'road'])}", end="\t")
    # print(f"Posicao: {str(output[0][1][0][1][b'pos'])}", end="\t")
    # print(f"Datetime: {str(output[0][1][0][1][b'datetime'])}")

    for _, message in output:
        count_plate(message[0][1])
        print(f"Placa: {str(message[0][1][b'plate'])}", end="\t")
        print(f"Rodovia: {str(message[0][1][b'road'])}", end="\t")
        print(f"Posicao: {str(message[0][1][b'pos'])}", end="\t")
        print(f"Datetime: {str(message[0][1][b'datetime'])}")

