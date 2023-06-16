import os 
from datetime import datetime,timedelta
class hadoopInteration:
    def __init__(self):
        self.dayDate = (datetime.now()-timedelta(days=1)).strftime('%d')
        self.monthDate = (datetime.now()-timedelta(days=1)).strftime('%m')
        self.yearDate = (datetime.now()-timedelta(days=1)).strftime('%Y')
        self.fileName = f'{self.dayDate}_{self.monthDate}_{self.yearDate}.parquet'
    def saveToHadoop(self,topic):
        current_path = os.getcwd()
        command = f'hadoop fs -put {current_path}/{topic}_{self.dayDate}_{self.monthDate}_{self.yearDate}.parquet /mazii'
        os.system(command)
    def save_Many(self,topics):
        """
        input : array of topic
        output : save array of object to hadoop
        """
        for topic in topics:
            self.saveToHadoop(topic)
    
    
# if __name__ =='__main__':
#     abc = hadoopInteration()
#     abc.saveToHadoop('mazii')

