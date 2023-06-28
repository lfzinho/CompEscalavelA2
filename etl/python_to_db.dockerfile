FROM apache/spark-py:latest

WORKDIR /home/eborn/Documentos/CompEscalavelA2

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./python_to_db.py" ]