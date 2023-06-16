import os 
class createNode:
    # topics means array of topic , ex : [mazii,hanji...]
    def __init__(self,topics) -> None:
        self.topic = topics
    def initilizeNode(self):
        createHadoopNode = f'hadoop fs -mkdir /{self.topic}'
        try:
            os.system(createHadoopNode)
        except Exception as err:
            print(err)