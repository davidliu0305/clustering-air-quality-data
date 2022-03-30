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
    
    
    data = get_AQ_data()
    
     # loop over each record
    for record in data:
        event_data = record   # entire line of input CSV is the message
        print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
      #   publisher.publish(pubsub_topic, event_data)
        time.sleep(1) 