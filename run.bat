python -m venv env
.\env\bin\activate
pip3 install pika --upgrade

start python consumer.py
start python producer.py