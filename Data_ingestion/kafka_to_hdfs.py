import os
from kafka import KafkaClient, SimpleConsumer
from datetime import datetime

kafka = KafkaClient("localhost:9092")

class HDFS_Consumer(object):
    
    '''
    This class is used to receive messages from Kafka, and save it to hdfs system.
    Messages are first saved to a temporary file on local machine, then transfered
    to hdfs atomically. 
    '''
    
    def __init__(self, hdfs_dir, max_count):
        
        '''
        hdfs_directory is the folder where data is saved on hdfs.
        '''
        
        self.hdfs_dir = hdfs_dir
        self.count = 0
        self.max_count = max_count
    
    def consume_topic(self,topic,group,temp_dir):

        '''
        This function receive messages from Friendsquare topic then save it to a temporary
        file: temp_dir, then transfer the file to hdfs.
        Create a kafka receiver to grap messages
        '''

        kafka_receiver = SimpleConsumer(kafka, group, topic, max_buffer_size=1310720000)
                
        # Create a temp file to store messages
        self.temp_file_path = "%s/%s.txt" % (temp_dir,str(self.count))
        
        temp_file = open(self.temp_file_path, 'w')

        hdfs_output_dir = "%s/%s" % (self.hdfs_dir, topic)

        # Create a hdfs directory to store output files
        os.system("hdfs dfs -mkdir -p %s" % hdfs_output_dir)        
        
        while self.count < self.max_count:

            # Get 1000 messages each time
            messages = kafka_receiver.get_messages(count = 1000, block = False)

            if not messages:
                continue
            
            # Write the messages to a file, one message per line
            for message in messages:
                temp_file.write(message.message.value + '\n')
            
            # Set each file size at 20 M
            if temp_file.tell() > 20000000:
                temp_file.close()
               
               # Put the file to hdfs
                hdfs_path = "%s/%s.txt" % (hdfs_output_dir,self.count)
                os.system("hdfs dfs -put -f %s %s" % (self.temp_file_path, hdfs_path))

                #remove the old file 
                os.remove(self.temp_file_path)

                #  Create a new temp file to store messages
                self.count +=1                
                self.temp_file_path = "%s/%s.txt" % (temp_dir,str(self.count))                
                temp_file = open(self.temp_file_path, 'w')

            # Inform zookeeper of position in the kafka queue
            kafka_receiver.commit()
        
        temp_file.close()
        
        
if __name__ == '__main__':
    
    hdfs_dir = "/user"
    group = "Spark"
    topic = "Friendsquare"
    temp_dir = "/home/ubuntu/friendsquare/kafkadata"
    max_count = 50
    
    hdfs_consumer = HDFS_Consumer(hdfs_dir, max_count)
    hdfs_consumer.consume_topic(topic,group,temp_dir)

