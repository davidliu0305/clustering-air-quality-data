from google.cloud import pubsub_v1
from params import PROJECT_ID

if __name__ == "__main__":

       # Replace  with your project id
    project = PROJECT_ID

    # Replace  with your pubsub topic
    pubsub_topic = ''  

    # create publisher
    publisher = pubsub_v1.PublisherClient()
    
    
    