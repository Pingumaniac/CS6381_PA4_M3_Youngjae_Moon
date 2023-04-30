"""
The first part of the code establishes a connection to the ZooKeeper service running on 
localhost:2181. It then creates several ZNodes (i.e., nodes in the ZooKeeper namespace) using 
the ensure_path method to create paths for topics, publishers, subscribers, and brokers. 
It then creates a TopicSelector object to retrieve all available topics and create ZNodes 
for each topic's subscribers.

The ChildrenWatch decorator is then used to watch for changes in the ZNodes. 

watchSubscribers function is another callback function that is invoked whenever 
there is a change in the '/topics/{t}/sub' ZNode for a specific topic. 
It simply prints out the updated list of subscribers for the topic.

watchTopics function is a callback function that is invoked whenever there is a change in 
the '/topics' ZNode. It loops through each topic, retrieves the topic's subscribers using 
the get method, and sets a watch on the '/topics/{t}/sub' ZNode using watchSubscribers function.
"""

from topic_selector import TopicSelector
from kazoo.client import KazooClient

zk = KazooClient(hosts='localhost:2181')
zk.start()
zk.ensure_path('/topics')
zk.ensure_path('/leader')
zk.ensure_path('/broker')

ts = TopicSelector()
topicList = ts.all()
for t in topicList:
    zk.ensure_path(f'/topics/{t}')
    zk.ensure_path(f'/topics/{t}/pub')
    zk.ensure_path(f'/topics/{t}/sub')
    
    @zk.ChildrenWatch(f'/topics/{t}/sub')
    def watchSubscribers(children):
        print(f'Topic {t} subscribers updated: {children}')

brokers = ['rb1', 'rb2', 'rb3']
for b in brokers:
    zk.ensure_path(f'/broker/{b}')

@zk.ChildrenWatch('/topics/{t}/sub')
def watchSubscribers(children):
    print(f'Topic {t} subscribers updated: {children}')

@zk.ChildrenWatch('/topics')
def watchTopics(children):
    for t in children:
        zk.get(f'/topics/{t}/sub', watch=watchSubscribers)