Create Kafka Topic: 
.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stock-data

Zookeper: 
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Server: 
.\bin\windows\kafka-server-start.bat .\config\server.properties

Server-1: 
.\bin\windows\kafka-server-start.bat .\config\server-1.properties

Install requirements.txt: 
pip install -r requirements.txt

List Kafka Topics: 
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 

python send_stock_data.py 
streamlit run ask_for_data.py

