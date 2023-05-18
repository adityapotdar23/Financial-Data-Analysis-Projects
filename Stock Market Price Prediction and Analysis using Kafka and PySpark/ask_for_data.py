from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from numpy import array 
from kafka import KafkaConsumer
from kafka import KafkaProducer 
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Flatten
from keras.layers.convolutional import Conv1D
from keras.layers.convolutional import MaxPooling1D
import plotly.graph_objects as go

def split_sequence(sequence,steps):
    X,y=[],[]
    sequence=list(sequence)
    for start in range(len(sequence)):
        end_index = start+steps
        if end_index>len(sequence)-1:
            break
        sequence_x,sequence_y = sequence[start:end_index],sequence[end_index]
        X.append(sequence_x)
        y.append(sequence_y)
    return(array(X),array(y))

producer = KafkaProducer(bootstrap_servers=['localhost:9093']) 

consumer = KafkaConsumer('stock-data',bootstrap_servers=['localhost:9092'], 
                         auto_offset_reset='latest')

spark = SparkSession.builder \
    .appName("Real-time Stock Data") \
    .getOrCreate()

st.title("Stocks App")
tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "BAJFINANCE.NS"] 
symbol = st.selectbox("Select a stock symbol", tickers)
# for  message in consumer:    
#     print(message)
# message = 'AAPL' 
producer.send('stock-analyzer', value=symbol.encode('utf-8'))

for message in consumer:
    value_bytes = message.value
    value_str = value_bytes.decode(errors='ignore')
    # print(f"Received value: {value_str}")  
    df = pd.DataFrame(eval(value_str))
    st.dataframe(df.tail(5)) 
    last_5 = df.tail(20)

    fig = go.Figure(data=go.Scatter(x=last_5['Datetime'], y=last_5['Close'], mode='lines'))
    st.plotly_chart(fig)
    # Prediction
    spark_df = spark.createDataFrame(df)
    raw_sequence = df["Close"][-300:] 
    steps=3
    X,y = split_sequence(raw_sequence,steps)
    features = 1
    X = X.reshape((X.shape[0],X.shape[1],features))
    model = Sequential()
    model.add(Conv1D(filters=64,kernel_size=2,activation="relu",input_shape=(steps,features)))
    model.add(MaxPooling1D(pool_size=2))
    model.add(Flatten())
    model.add(Dense(100,activation='relu'))
    model.add(Dense(1))
    model.compile(optimizer='adam',loss='mse')
    model.fit(X,y,epochs=1000,verbose=0)
    x_input = array(raw_sequence[-3:])
    x_input = x_input.reshape((1,steps,features))
    y_pred = model.predict(x_input,verbose=0)
    st.write("The predicted next 'Close value' is", y_pred[0][0])
    # st.text(y_pred[0][0])
    break