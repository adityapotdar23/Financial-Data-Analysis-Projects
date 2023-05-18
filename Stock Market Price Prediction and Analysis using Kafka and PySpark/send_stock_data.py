from kafka import KafkaProducer
from kafka import KafkaConsumer
import yfinance as yf
import json
import time
import pandas as pd
import sys

producer = KafkaProducer(bootstrap_servers=['localhost:9092']) 

consumer = KafkaConsumer('stock-analyzer',bootstrap_servers=['localhost:9093'], 
                         auto_offset_reset='latest')


for message in consumer:
    value_bytes = message.value
    value_str = value_bytes.decode(errors='ignore')
    # print(f"Received value: {value_str}") 
    
    stock_data = yf.download(tickers=value_str, period='1d', interval='1m')
    stock_data = stock_data.reset_index()
    stock_data.index = stock_data.index.astype(str)
    stock_data['Datetime'] = stock_data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # non_serializable_cols = [col for col in stock_data.columns if not stock_data[col].dtype in [int, float, bool]]
    # stock_data = stock_data.drop(columns=non_serializable_cols)
    data_dict = stock_data.to_dict(orient='records')
    json_payload = json.dumps(data_dict).encode()
    producer.send('stock-data', value=json_payload)
    producer.flush()






# stock_data = yf.download(tickers='AAPL', period='1d', interval='1m')
# stock_data = stock_data.reset_index()

# print(stock_data.head())
# # Convert the index to string format
# # stock_data.index = stock_data.index.astype(str)

# stock_data['Datetime'] = stock_data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

# # # Remove non-serializable columns
# # non_serializable_cols = [col for col in stock_data.columns if not stock_data[col].dtype in [int, float, bool]]
# # stock_data = stock_data.drop(columns=non_serializable_cols)

# # # Convert the DataFrame to a dictionary and send it to Kafka
# data_dict = stock_data.to_dict(orient='records')
# json_payload = json.dumps(data_dict).encode()
# print(json_payload)
# producer.send('stock-data', value=json_payload)
# producer.flush()
