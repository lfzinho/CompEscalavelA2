FROM python:3.10-slim

WORKDIR /home/eborn/Documentos/CompEscalavelA2

EXPOSE 6379/tcp

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./Simulator/simulator.py" ]