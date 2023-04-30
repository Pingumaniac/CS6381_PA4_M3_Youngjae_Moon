# CS6381_PA4_M3_Youngjae_Moon
About CS6381 PA4 Milestone3 Youngjae Moon

## What I've done

1. Load balancing for Milestone 1
2. Ownership strength for Milestone 2
3. Quality of Service (QoS) for Milestone 3

## What I have not done

1. Detailed testing and data visualisation

## Details of new functions implemented:

### For Discovery Appln, new functions for PA4 that are mostly for load balancing

1. getAvailablePublishers(topic) - This function will return a list of publishers who have published a given topic. This list can be used by the subscriber to select a publisher to subscribe to. This function will be called by the TopicSelector class

2. getTopicDistribution() - This function will return a dictionary that contains the number of publishers who have published each topic. This information can be used to determine which topics are popular and which are not, and can be used to perform load balancing.
    
3. loadBalance() - This function will be responsible for performing load balancing. It will use the getTopicDistribution() function to determine which topics are popular and which are not. It will then try to balance the number of publishers for each topic, by moving publishers from a popular topic to an unpopular topic.
    
4. setLoadBalancingStrategy(strategy): This function would allow the discovery application to specify the load balancing strategy that should be used when distributing topics among publishers. The strategy could be something like round-robin or weighted round-robin, where publishers with more available resources are given a larger share of the topics. This function could also be used to switch between different load balancing strategies at runtime.
    
5. setPublisherResources(publisher, resources): This function would allow the discovery application to specify the available resources (e.g. CPU, memory, bandwidth) for each publisher. This information could be used by the load balancing strategy to distribute topics more effectively. For example, a publisher with more CPU resources could be assigned more CPU-intensive topics, while a publisher with more bandwidth could be assigned more data-intensive topics.
   
6. handleLoadBalancingRequest(load_balancing_req): This function would handle incoming load balancing requests from other discovery nodes in the quorum. The request could include information about the current resource utilization of each publisher, as well as any constraints or preferences for how topics should be distributed. The function would use this information to update its own load balancing strategy and redistribute topics as needed.
    
7. redistributeTopics(): redistribute topics based on the new load balancing strategy

8. selectPublisher(): Select the publisher with the highest ownership strength for a topic.

9. backupLoadBalancingState(): This function would save the current load balancing state (e.g. publisher resources, topic assignments) to persistent storage. This would allow the load balancing state to be recovered in the event of a failure or restart.
    
10. recoverLoadBalancingState(): This function would load the saved load balancing state from persistent storage and restore the load balancing strategy and topic assignments. This would allow the load balancing state to be recovered after a failure or restart.

11. handleResourceUpdate(publisher, resources): This function would handle incoming updates to a publisher's available resources. This could be triggered by a publisher reporting a change in its resource availability, or by an external monitoring system detecting changes in the publisher's environment. The function would update the publisher's resources in the load balancing strategy and redistribute topics as needed.

12. calculatePublisherWeights(): This function would calculate the weights of each publisher


### For DiscoveryAppln, new functions for ownership strength

1. assignOwnershipStrength: Assigns ownership strength values to a publisher for a given topic based on ZooKeeper leader election. The ownership strength is a monotonically increasing integer value, where 1 is the highest strength.
    
2. removePublisherOwnershipStrength: Removes the ownership strength of a publisher for a given topic.

3. handlePublisherFailure: Handles a publisher failure by removing the publisher from the topics2pubs dictionary, removing the publisher's ownership strength for each topic, and reassigning ownership strength for the topic if there are still publishers left.

### For DiscoveryMW, new functions for ownership strength

1. assignOwner: randomly assign a publisher as owner of a topic

### For BrokerAppln, I have added new functions for load balancing (Milestone 1) and QoS (Milestone 3)

1. loadBalancing() -> implements load balancing by redistributing messages to available brokers

2. checkQOS() -> calculates metrics such as latency and throughput.

### For BrokerMW, new functions added and modified functions are

New functions added and modified for PA4:

1. register(): Registers a broker with a name, a list of topics, and optionally topic ownership and QoS levels.
    
2. update_topic_ownership(): Updates the topic ownership list for the broker.
    
3. update_qos_levels(): Updates the QoS levels for the broker's subscribed topics.
    
4. send_msg_pub(): Sends a message to a topic with the specified QoS level, defaulting to the topic's QoS level if not provided.

5. send_msg_pub_qos0(): Sends a message to a topic with QoS level 0 (fire-and-forget).

6. send_msg_pub_qos1(): Sends a message to a topic with QoS level 1 (acknowledged delivery).

7. send_msg_pub_qos2(): Sends a message to a topic with QoS level 2 (exactly-once delivery).

8. receive_msg_sub(): Receives a message from a subscribed topic with the specified QoS level, defaulting to the topic's QoS level if not provided.
    
9. receive_msg_sub_qos0(): Receives a message from a subscribed topic with QoS level 0 (fire-and-forget).
    
10. receive_msg_sub_qos1(): Receives a message from a subscribed topic with QoS level 1 (acknowledged delivery) and sends an acknowledgement.
   
11. receive_msg_sub_qos2(): Receives a message from a subscribed topic with QoS level 2 (exactly-once delivery) and sends an acknowledgement.
    
12. acknowledge_msg(): Acknowledges the receipt of a message with the given ID for the specified topic.
    
13. report_load(): Sends a load report message to the Discovery service and receives a response.

14. create_load_report_message(): Creates a load report message in JSON format.

15. event_loop(): Runs the broker's event loop, reporting load status to the Discovery service every 5 seconds.

### For SubscriberMW,

1. handle_load_balanced_publishers(): to do load balancing

2. matching(): Add the matching method to take history QoS into account

### I have updated disseminate method for PublisherMW. The disseminate method updates history for QoS.

### For QoS, I have made a Qos.py file.

This is a class that defines Quality of Service (QoS) parameters for message delivery. It has an __init__ method that initializes the QoS parameters, an update_qos method to update the parameters, and a __str__ method to represent the QoS parameters as a string. The history_size, reliability, durability, and deadline parameters can be set and updated through the update_qos method. The __str__ method returns a string representation of the QoS object with its current parameters.

## How to test this software

1. First start Zookeeper (after downloading this GitHub repository)
```
cd zookeeper/bin
sudo zkServer.sh start
source zkCli.sh
```

2. Run exp_generator.py for testing.
```
python3 exp_generator.py -P 3 -S 3 -B 3 -D 3
```
3. To stop Zookeper
```
sudo bin/zkServer.sh stop
```
