import os
from dotenv import load_dotenv
from Stream.middle.kafka_transformation.consumer_connection import consumer
from Stream.middle.kafka_transformation.producer_matomo import BatchProducer
from Stream.saver.hadoop_House.hadoop import hadoopInteration
from Stream.saver.hadoop_House.hadoopCreateNode import createNode
if __name__ =='__main__':
    """
    purpose : get data from matomo and then save to hadoop via kafka for high tolerance
    """
# prepare all the enviroment variable 
    load_dotenv()   
    PROJECT_NAME = os.getenv('PROJECT_NAME')
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS') or '192.168.1.199:9092'
    TOPIC_PEOPLE_BASIC_NAMES = os.getenv('TOPIC_PEOPLE_BASIC_NAMES')
    TOPIC_PEOPLE_BASIC_PARTITIONS = os.getenv('TOPIC_PEOPLE_BASIC_PARTITIONS')
    TOPIC_PEOPLE_BASIC_REPLICATION_FACTOR = os.getenv('TOPIC_PEOPLE_BASIC_REPLICATION_FACTOR')
    MATOMO_ENDPOINT_API =  os.getenv('MATOMO_ENDPOINT_API') or 'https://matomo.eupgroup.net/index.php' 
    MATOMO_AUTHENTICATION= os.getenv('MATOMO_AUTHENTICATION') or '108d97003517053884ef87f5dda5819e'
    """ size id topic shoulde be in array for iteration"""
    SITE_ID_TOPIC = [{'siteID':'1','topic':'mazii'},{'siteID':'2','topic':'hanij'}]
# running the project 
    pro = BatchProducer(BOOTSTRAP_SERVERS) #turn kafka server on
    """
    when set kafka get message from matomo, un-comment the following code
    """
    for site_topic in SITE_ID_TOPIC:
        try:
            siteID=site_topic['siteID']
            topic=site_topic['topic']
            #send request to matomo =>kafka
            pro.sendMatomo(MATOMO_ENDPOINT_API,MATOMO_AUTHENTICATION,siteID) 
            # get data from kafka => json
            consumerTest1 = consumer(BOOTSTRAP_SERVERS)
            consumerTest1.getMessageRuntime(topic)
            # create node folder in hadoop , following the topic
            createNodeHadoop = createNode(topic)
            createNodeHadoop.initilizeNode()
            # save data to hadoop
            hdfs_home = hadoopInteration()
            hdfs_home.saveToHadoop(topic)
            # remove file from server
            filename_deletion =site_topic['topic']+'_'+hdfs_home.fileName
            # os.remove(filename_deletion)
        except Exception as err:
            print(err)
    
