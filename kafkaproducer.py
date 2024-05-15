from kafka import KafkaProducer
import time
bootstrap_servers = 'localhost:9092'
# Kafka topic to produce to
topic = 'hinata'

#path="/var/log/apache2/access.log"
path=r"C:\logs\hinata_access_log_20240401-122710.log"

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Read the log file and produce each line to Kafka
with open(path, 'r') as file:
   for line in file:
       producer.send(topic, value=line.encode('utf-8'))
       print(line)
       time.sleep(1)
