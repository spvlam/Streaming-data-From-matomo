import json, time
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime,timedelta
import time

class consumer:
    def __init__(self,bootstrap_server):
        self.bootstrap_servers=bootstrap_server
    # runtime Problem handle
    def getMessageRuntime(self,topic):
        """
        input : topic 
        output : parquet file of data
        """
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'auto_offset_reset': 'earliest',  #get all the message from starting time( kafka start)
            # if comand auto_offset_reset, the message will be send since starting this file
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'consumer_timeout_ms':10000, #set time for waiting consumer message
        }
        # create kafka consumer with topic as passing
        self.consumer = KafkaConsumer(topic, **consumer_config)
        # dataLog contain tracking data in array form
        dataLog=[]
        for message in self.consumer:
            dataLog.append(message.value)
        # get current day
        dayDate = (datetime.now()-timedelta(days=1)).strftime('%d')
        monthDate = (datetime.now()-timedelta(days=1)).strftime('%m')
        yearDate = (datetime.now()-timedelta(days=1)).strftime('%Y')
        # save parquet file 
        finalFileName = topic+'_'+dayDate+'_'+monthDate+'_'+yearDate+'.parquet'
        #convert array of object to data frame
        dataFrame = pd.DataFrame(dataLog)
        #convert data frame to arrow table
        table = pa.Table.from_pandas(dataFrame)
        # write to parquet file
        pq.write_table(table,finalFileName)
        self.consumer.close()
        # return consumer 
    def getBatchOfMessage(self,topic,interval,batch_size):
        consumer_config={
            'bootstrap_servers':self.bootstrap_servers,
            # 'auto_offset_reset': 'earliest',  #get all the message from starting time
            # 'value_deserializer':lambda x: json.loads(x.decode('utf-8')),
        }
        self.consumerBatch = KafkaConsumer(topic,**consumer_config)  
        messages = []  # List to store messages in the current batch
        last_batch_time = time.time()  # Time of the last batch
        for message in self.consumerBatch:
            messages.append(message.value.decode('utf-8'))
            if len(messages) >= batch_size or time.time() - last_batch_time >= interval:
                with open('log.json','a') as fi:
                    json.dump(messages,fi)
                    fi.write('\n')
                    print('done')
                messages=[]
                last_batch_time=time.time()
        self.consumerBatch.close()
    
    
if __name__=='__main__':
    # consumerTest = consumer(BOOTSTRAP_SERVERS2)
    # consumerTest.getMessageRuntime('tracking2')
    consumerTest1 = consumer('localhost:9092')
    consumerTest1.getMessageRuntime('mazii')
    # consumerTest1.getBatchOfMessage('mazii',500,5)
    
