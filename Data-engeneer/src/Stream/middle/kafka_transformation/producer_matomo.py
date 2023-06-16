import json
from kafka import KafkaProducer
from ...getter.matomoGeter.getDataFromMatomo import MatomoLog
from .kafkaConfig import kafkaConfiguration
class BatchProducer(kafkaConfiguration):
    def __init__(self, bootstrap_server):
        super().__init__(bootstrap_server)
        self.producer=KafkaProducer(bootstrap_servers=bootstrap_server)
    # get data from matomo
    def sendMatomo(self,api,auth,siteID):
        message = MatomoLog(api,auth)
        message.initiliseLog(siteID,'day','yesterday')
        da=[] #for writing data to json file
        while True:
            message.getLogFromSiteId()
            da+=message.data
            print(len(da))
            for mes in message.data:
                topic = mes['siteName']
                self.producer.send(topic, json.dumps(mes).encode('utf-8'))
                # wait for message to be send 
                self.producer.flush()
            if len(message.data)==0 :
                break   
    # if we want to scale it, we just only add the new function for 
if __name__ =='__main__':
    api_url1 = 'https://matomo.eupgroup.net/index.php'
    auth_token2='108d97003517053884ef87f5dda5819e'
    pro = BatchProducer("localhost:9092")
    pro.sendMatomo(api_url1,auth_token2)


        