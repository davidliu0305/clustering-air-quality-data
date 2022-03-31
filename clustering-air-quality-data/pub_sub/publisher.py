from google.cloud import pubsub_v1
from params import PROJECT_ID
from pub_sub.data import get_AQ_data
import time

if __name__ == "__main__":

       # Replace  with your project id
    project = PROJECT_ID

    # Replace  with your pubsub topic
    pubsub_topic = f'projects/{PROJECT_ID}/topics/AQ_Topic'  

    # create publisher
    publisher = pubsub_v1.PublisherClient()
    
    
    data = get_AQ_data(start_date='2022-03-01',end_date='2022-03-10')
    
     # loop over each record
    for record in data:
        event_data = str(record).encode('utf-8')
        publisher.publish(pubsub_topic, event_data)
        time.sleep(0.5) 