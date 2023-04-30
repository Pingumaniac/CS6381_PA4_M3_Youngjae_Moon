import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from enum import Enum  # for an enumeration we are using to describe what state we are in
from functools import wraps # for a decorator we are using to handle exceptions

# New import for PA4
import threading
import itertools
import json
from collections import defaultdict


class DiscoveryAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        WAIT = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__(self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics in the discovery service
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.num_topics = None # total num of topics in the discovery service
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.no_pubs = 0 # Initialise to 0
        self.no_subs = 0 # Initialise to 0
        self.no_broker = 0 # Initialise to 0
        self.pub_list = [] # Initialise to empty list
        self.sub_list = [] # Initialise to empty list
        self.broker_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = False
        self.topics2pubs = {}
        self.pubs2ip = {}
        
        # New variables for PA4
        self.publisher_resources = {}
        self.loadBalance_timer = None
        self.topic_ownership_strength = {}
        self.topic_history = defaultdict(list)
        self.history_length = {}  # Store the history length for each topic
        

    # Modified the configure method for PA4
    @handle_exception
    def configure(self, args, loadBalance_interval=10):
        self.logger.info("DiscoveryAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name  # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency  # frequency with which topics are disseminated
        self.num_topics = args.num_topics  # total num of topics we publish
        self.no_pubs = args.no_pubs
        self.no_subs = args.no_subs
        self.no_broker = args.no_broker
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        self.mw_obj = DiscoveryMW(self.logger)
        self.mw_obj.configure(args)  # pass remainder of the args to the m/w object
        self.logger.info("DiscoveryAppln::configure - configuration complete")
        # Schedule periodic load balancing
        self.logger.info("Scheduling periodic load balancing every {} seconds".format(loadBalance_interval))
        self.loadBalance_timer = threading.Timer(loadBalance_interval, self.loadBalance)
        self.loadBalance_timer.start()

    @handle_exception    
    def driver (self):
        self.logger.info("DiscoveryAppln::driver")
        self.dump()
        self.logger.info("DiscoveryAppln::driver - upcall handle")
        self.mw_obj.setWatch()
        self.mw_obj.set_upcall_handle(self)
        self.state = self.State.ISREADY
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("DiscoveryAppln::driver completed")

    @handle_exception    
    def register_request(self, reg_request):
        self.logger.info("DiscoveryAppln::register_request")
        status = False # success = True, failure = False
        reason = ""
        if reg_request.role == discovery_pb2.ROLE_PUBLISHER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_PUBLISHER")
            if len(self.pub_list) != 0:
                for pub in self.pub_list:
                    if pub[0] == reg_request.info.id:
                        reason = "The publisher name is not unique."
            if reason == "":
                self.pub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The publisher name is unique."
        elif reg_request.role == discovery_pb2.ROLE_SUBSCRIBER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_SUBSCRIBER")
            if len(self.sub_list) != 0:
                for sub in self.sub_list:
                    if sub[0] == reg_request.info.id:
                        reason = "The subscriber name is not unique."
            if reason == "":
                self.sub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The subscriber name is unique."
        elif reg_request.role == discovery_pb2.ROLE_BOTH:
            self.logger.info("DiscoveryAppln::register_request - ROLE_BOTH")
            if len(self.broker_list) != 0:
                reason = "There should be only one broker."
            if reason == "":
                self.broker_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The broker name is unique and there is only one broker."
        else:
            raise Exception("Role unknown: Should be either publisher, subscriber, or broker.")
        if len(self.pub_list) >= self.no_pubs and len(self.sub_list) >= self.no_subs:
            self.is_ready = True
        self.mw_obj.handle_register(status, reason)
        return 0

    @handle_exception
    def isready_request(self):
        self.logger.info("DiscoveryAppln:: isready_request")
        self.mw_obj.update_is_ready_status(True)
        return 0
    
    @handle_exception
    def handle_topic_request(self, topic_req):
        self.logger.info("DiscoveryAppln::handle_topic_request - start")
        pubTopicList = []
        for pub in self.pub_list:
            if any(topic in pub[3] for topic in topic_req.topiclist):
                self.logger.info("DiscoveryAppln::handle_topic_request - add pub")
                pubTopicList.append([pub[0], pub[1], pub[2]])     
        self.mw_obj.send_pubinfo_for_topic(pubTopicList)
        return 0

    @handle_exception    
    def handle_all_publist(self):
        self.logger.info ("DiscoveryAppln:: handle_all_publist")
        pubWithoutTopicList = []
        if len(self.pub_list) != 0:
            for pub in self.pub_list:
                pubWithoutTopicList.append([pub[0], pub[1], pub[2]])
        else:
            pubWithoutTopicList = []
        self.mw_obj.send_all_pub_list(pubWithoutTopicList)
        return 0
    
    @handle_exception
    def invoke_operation(self):
        self.logger.info("DiscoveryAppln::invoke_operation - start")
        if self.state == self.State.WAIT or self.state == self.State.ISREADY:
            return None
        else:
            raise ValueError("undefined")
        
    @handle_exception
    def backup(self):
        self.logger.info("DiscoveryAppln::backup - start")
        self.mw_obj.sendStateReplica(self.topics2pubs, self.pubs2ip, self.no_pubs, self.no_subs, self.state)

    @handle_exception
    def setBrokerInfo(self, broker):   
        self.logger.info("DiscoveryAppln::setBrokerInfo - start")
        self.broker = broker
        
    def setPublisherInfo(self, publishers):
        self.logger.info("DiscoveryAppln::setPublisherInfo - updating publishers")
        self.pubs2ip = {}
        self.topics2pubs = {}
        for pub in publishers:
            information = pub["id"]
            topiclist = pub["topiclist"]
            self.pubs2ip[information["id"]] = information
            for topic in topiclist:
                if topic not in self.topics2pubs:
                    self.topics2pubs[topic] = []
                self.topics2pubs[topic].append(information["id"])
        self.logger.info("DiscoveryAppln::setPublisherInfo - updated publishers: {}".format(self.pubs2ip))
        self.logger.info("DiscoveryAppln::setPublisherInfo - updated topics: {}".format(self.topics2pubs))
            
    @handle_exception
    def setState(self, topics2pubs, pubs2ip, no_pubs, no_subs):
        self.topics2pubs = topics2pubs
        self.pubs2ip = pubs2ip
        self.no_pubs = no_pubs
        self.no_subs = no_subs
    
    """
    New functions for PA4 that are mostly for load balancing
    1. getAvailablePublishers(topic) - This function will return a list of publishers who have 
    published a given topic. This list can be used by the subscriber to select a publisher to 
    subscribe to. This function will be called by the TopicSelector class
    2. getTopicDistribution() - This function will return a dictionary that contains the number 
    of publishers who have published each topic. This information can be used to determine which 
    topics are popular and which are not, and can be used to perform load balancing.
    3. loadBalance() - This function will be responsible for performing load balancing. 
    It will use the getTopicDistribution() function to determine which topics are popular and 
    which are not. It will then try to balance the number of publishers for each topic, by moving 
    publishers from a popular topic to an unpopular topic.
    4. setLoadBalancingStrategy(strategy): This function would allow the discovery application 
    to specify the load balancing strategy that should be used when distributing topics among 
    publishers. The strategy could be something like round-robin or weighted round-robin, where 
    publishers with more available resources are given a larger share of the topics. This function 
    could also be used to switch between different load balancing strategies at runtime.
    5. setPublisherResources(publisher, resources): This function would allow the discovery 
    application to specify the available resources (e.g. CPU, memory, bandwidth) for each publisher.
    This information could be used by the load balancing strategy to distribute topics more 
    effectively. For example, a publisher with more CPU resources could be assigned more 
    CPU-intensive topics, while a publisher with more bandwidth could be assigned more 
    data-intensive topics.
    6. handleLoadBalancingRequest(load_balancing_req): This function would handle incoming load 
    balancing requests from other discovery nodes in the quorum. The request could include 
    information about the current resource utilization of each publisher, as well as any 
    constraints or preferences for how topics should be distributed. The function would use this 
    information to update its own load balancing strategy and redistribute topics as needed.
    7. redistributeTopics(): redistribute topics based on the new load balancing strategy
    8. selectPublisher(): Select the publisher with the highest ownership strength for a topic.
    9. backupLoadBalancingState(): This function would save the current load balancing state 
    (e.g. publisher resources, topic assignments) to persistent storage. This would allow the 
    load balancing state to be recovered in the event of a failure or restart.
    10. recoverLoadBalancingState(): This function would load the saved load balancing state from
    persistent storage and restore the load balancing strategy and topic assignments. This would 
    allow the load balancing state to be recovered after a failure or restart.
    11. handleResourceUpdate(publisher, resources): This function would handle incoming updates 
    to a publisher's available resources. This could be triggered by a publisher reporting a 
    change in its resource availability, or by an external monitoring system detecting changes 
    in the publisher's environment. The function would update the publisher's resources in the 
    load balancing strategy and redistribute topics as needed.
    12. calculatePublisherWeights(): This function would calculate the weights of each publisher
    """
    
    # Returns a list of publishers who have published a given topic.
    @handle_exception
    def getAvailablePublishers(self, topic):
        publishers = []
        if topic in self.topics2pubs:
            publishers = self.topics2pubs[topic]
        return publishers

    # Returns a dictionary containing the number of publishers who have published each topic.
    @handle_exception
    def getTopicDistribution(self):
        topic_distribution = {}
        for topic, pubs in self.topics2pubs.items():
            topic_distribution[topic] = len(pubs)
        return topic_distribution

    # Performs load balancing by moving publishers from a popular topic to an unpopular topic.
    @handle_exception
    def loadBalance(self):
        # Get the topic distribution
        topic_distribution = self.getTopicDistribution()
        # Determine the most popular and least popular topics
        most_popular_topic = max(topic_distribution, key=topic_distribution.get)
        least_popular_topic = min(topic_distribution, key=topic_distribution.get)
        # If there is a significant difference in the number of publishers for these topics, move some publishers
        if topic_distribution[most_popular_topic] - topic_distribution[least_popular_topic] > 1:
            # Get the list of publishers for the most popular topic
            publishers_to_move = self.topics2pubs[most_popular_topic][:2]
            # Remove these publishers from the most popular topic
            self.topics2pubs[most_popular_topic] = self.topics2pubs[most_popular_topic][2:]
            # Add these publishers to the least popular topic
            if least_popular_topic not in self.topics2pubs:
                self.topics2pubs[least_popular_topic] = []
            self.topics2pubs[least_popular_topic].extend(publishers_to_move)
            # Update the pubs2ip dictionary
            for pub_id in publishers_to_move:
                self.pubs2ip[pub_id]["topiclist"].remove(most_popular_topic)
                self.pubs2ip[pub_id]["topiclist"].append(least_popular_topic)
            self.logger.info("Load balancing: Moved publishers {} from topic {} to topic {}".format(publishers_to_move, most_popular_topic, least_popular_topic))

    @handle_exception
    def setLoadBalancingStrategy(self, strategy):
        self.logger.info("DiscoveryAppln::setLoadBalancingStrategy - {}".format(strategy))
        # set the load balancing strategy to the specified value
        self.load_balancing_strategy = strategy

    @handle_exception
    def setPublisherResources(self, publisher, resources):
        self.logger.info("DiscoveryAppln::setPublisherResources - {}, resources: {}".format(publisher, resources))
        # update the resources for the specified publisher
        self.publisher_resources[publisher] = resources
    
    @handle_exception
    def handleLoadBalancingRequest(self, load_balancing_req):
        self.logger.info("DiscoveryAppln::handleLoadBalancingRequest - start")
        # get the resource utilization of each publisher from the request
        publisher_utilization = load_balancing_req.publisher_utilization
        # update our own resource utilization information for each publisher
        for publisher, utilization in publisher_utilization.items():
            self.publisher_resources[publisher] = utilization
        # update the load balancing strategy based on the new information
        # for example, we could use a weighted round-robin strategy based on the available resources
        publisher_weights = {pub: 1 / (res+1) for pub, res in self.publisher_resources.items()}
        publishers = list(publisher_weights.keys())
        weights = list(publisher_weights.values())
        self.load_balancing_strategy = itertools.cycle(publishers, weights) 
        # redistribute topics based on the new load balancing strategy
        self.redistributeTopics()
    
    @handle_exception
    def redistributeTopics(self):
        self.logger.info("DiscoveryAppln::redistributeTopics - start")
        # Iterate over all the topics and their associated publishers
        for topic, publishers in self.topics2pubs.items():
            # Create a list to store the new publishers for the topic
            new_publishers = []
            # Use the load balancing strategy to choose a publisher for the topic
            while len(new_publishers) < len(publishers):
                # Get the next publisher from the load balancing strategy
                chosen_publisher = next(self.load_balancing_strategy)
                # Check if the chosen publisher is in the original list of publishers for the topic
                if chosen_publisher in publishers:
                    new_publishers.append(chosen_publisher)
            # Update the list of publishers for the topic
            self.topics2pubs[topic] = new_publishers
        self.logger.info("DiscoveryAppln::redistributeTopics - end")

    
    @handle_exception
    def selectPublisher(self, topic):
        self.logger.info("DiscoveryAppln::selectPublisher - start")
        publishers = self.topics2pubs.get(topic, [])
        if not publishers:
            return None
        # Select the publisher with the highest ownership strength for the topic
        highest_strength = float('inf')
        selected_publisher = None
        for publisher in publishers:
            strength = self.topic_ownership_strength[topic].get(publisher, float('inf'))
            if strength < highest_strength:
                highest_strength = strength
                selected_publisher = publisher
        if selected_publisher is None:
            self.logger.error("No suitable publisher found for topic {}".format(topic))
            raise Exception("No suitable publisher found for topic {}".format(topic))
        return selected_publisher


    @handle_exception
    def backupLoadBalancingState(self):
        self.logger.info("DiscoveryAppln::backupLoadBalancingState - start")
        # save the current load balancing state to persistent storage
        with open("load_balancing_state.json", "w") as f:
            json.dump({
                "load_balancing_strategy": self.load_balancing_strategy,
                "publisher_resources": self.publisher_resources,
            "topic_assignments": self.topic_assignments
            }, f)

    @handle_exception
    def recoverLoadBalancingState(self):
        self.logger.info("DiscoveryAppln::recoverLoadBalancingState - start")
        # load the saved load balancing state from persistent storage
        with open("load_balancing_state.json", "r") as f:
            state = json.load(f)
            # restore the load balancing strategy, publisher resources, and topic assignments
            self.load_balancing_strategy = state["load_balancing_strategy"]
            self.publisher_resources = state["publisher_resources"]
            self.topic_assignments = state["topic_assignments"]
             # recreate the itertools.cycle object with the recovered publishers and weights
            publishers = state["load_balancing_strategy"]["publishers"]
            weights = state["load_balancing_strategy"]["weights"]
            self.load_balancing_strategy = itertools.cycle(zip(publishers, weights))
        # redistribute topics based on the recovered load balancing state
        for topic, publishers in self.topic_assignments.items():
            # check if there are any publishers for this topic
            if not publishers:
                continue
            # get the index of the last publisher used for this topic, or start from the beginning
            index = state.get(f"{topic}_index", 0)
            # rotate the list of publishers to start from the last used publisher
            publishers = publishers[index:] + publishers[:index]
            # update the topic assignments with the rotated list of publishers
            self.topic_assignments[topic] = publishers

    @handle_exception
    def handleResourceUpdate(self, publisher, resources):
        self.logger.info("DiscoveryAppln::handleResourceUpdate - {}, resources: {}".format(publisher, resources))
        # Update the resources for the specified publisher
        self.publisher_resources[publisher] = resources
        # Calculate the weights of each publisher based on their resource utilization
        weights, publishers = self.calculatePublisherWeights()
        # Create a weighted list of publishers based on their weights
        # For example, if a publisher has a weight of 1.5, it will appear in the list 15 times.
        weighted_publishers = []
        for i in range(len(publishers)):
            for j in range(int(weights[i]*10)):
                weighted_publishers.append(publishers[i])
    
        # Use a round-robin algorithm to select a publisher from the weighted list for each topic
        for topic, pub_list in self.topics2pubs.items():
            # Start from the index of the last publisher used for this topic
            last_index = self.topic_last_index.get(topic, 0)
            for i in range(len(pub_list)):
                # Select a publisher from the weighted list based on the round-robin index
                pub = weighted_publishers[last_index % len(weighted_publishers)]
                if pub in pub_list:
                    # Found a publisher for this topic
                    self.topic_assignments[topic] = pub
                    # Update the index of the last publisher used for this topic
                    self.topic_last_index[topic] = (last_index + 1) % len(weighted_publishers)
                    break

    @handle_exception
    def calculatePublisherWeights(self):
        weights = []
        publishers = []
        for pub, res in self.publisher_resources.items():
            weight = 1.0
            # adjust weight based on resource utilization
            if res["load"] > 0.7 * res["capacity"]:
                weight *= 0.5
            elif res["load"] > 0.5 * res["capacity"]:
                weight *= 0.8
            elif res["load"] < 0.2 * res["capacity"]:
                weight *= 1.5
            publishers.append(pub)
            weights.append(weight)
        # Normalize the weights so they sum up to 1
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]
        return weights, publishers
    
    """
    New functions for PA4 Milestone 2
    1. assignOwnershipStrength: Assigns ownership strength values to a publisher for a given topic 
    based on ZooKeeper leader election. The ownership strength is a monotonically increasing 
    integer value, where 1 is the highest strength.
    2. removePublisherOwnershipStrength: Removes the ownership strength of a publisher for a 
    given topic.
    3. handlePublisherFailure: Handles a publisher failure by removing the publisher from the 
    topics2pubs dictionary, removing the publisher's ownership strength for each topic, and 
    reassigning ownership strength for the topic if there are still publishers left.
    """
    @handle_exception
    def assignOwnershipStrength(self, topic, publisher):
        if topic not in self.topic_ownership_strength:
            self.topic_ownership_strength[topic] = {}
        # Perform ZooKeeper leader election to assign ownership strength values
        # Uuse a simple way to assign monotonically increasing ownership strength values
        # 1 is the highest strength, and so on
        current_strengths = list(self.topic_ownership_strength[topic].values())
        if current_strengths:
            next_strength = max(current_strengths) + 1
        else:
            next_strength = 1
    
        self.topic_ownership_strength[topic][publisher] = next_strength
        return next_strength
    
    @handle_exception
    def removePublisherOwnershipStrength(self, topic, publisher):
        if topic in self.topic_ownership_strength:
            if publisher in self.topic_ownership_strength[topic]:
                del self.topic_ownership_strength[topic][publisher]
                self.logger.info("Removed ownership strength for publisher {} and topic {}".format(publisher, topic))
            else:
                self.logger.warning("Publisher {} not found in ownership strength dictionary for topic {}".format(publisher, topic))
        else:
            self.logger.warning("Topic {} not found in ownership strength dictionary".format(topic))

    @handle_exception
    def handlePublisherFailure(self, publisher):
        self.logger.info("Handling publisher failure for publisher {}".format(publisher))
        # Remove the publisher from the self.topics2pubs dictionary
        for topic, publishers in self.topics2pubs.items():
            if publisher in publishers:
                publishers.remove(publisher)
                self.logger.info("Removed publisher {} from topic {}".format(publisher, topic))
                # Remove the publisher from the ownership strength dictionary
                self.removePublisherOwnershipStrength(topic, publisher)
                # If the topic still has publishers, reassign ownership strength
                if self.topics2pubs[topic]:
                    self.assignOwnershipStrength(topic, self.topics2pubs[topic][0])

    
    @handle_exception
    def dump(self):
        self.logger.info ("**********************************")
        self.logger.info ("DiscoveryAppln::dump")
        self.logger.info ("     Lookup: {}".format (self.lookup))
        self.logger.info ("     Dissemination: {}".format (self.dissemination))
        self.logger.info ("**********************************")
 
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Discovery Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="discovery", help="Discovery")
    parser.add_argument("-r", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")
    parser.add_argument("-t", "--port", type=int, default=5555, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    parser.add_argument("-P", "--no_pubs", type=int, default=1, help="Number of publishers")
    parser.add_argument("-S", "--no_subs", type=int, default=1, help="Number of subscribers")
    parser.add_argument("-B", "--no_broker", type=int, default=1, help="Number of brokers")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    # New code for PA3
    parser.add_argument("-q", "--quorum", type=int, default=3, help="Number of discovery nodes in the quorum, default=3")
    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IPv4 address for the zookeeper service, default = localhost:2181")
    return parser.parse_args()
    
def main():
    try:
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        logger.debug("Main: obtain the Discovery appln object")
        discovery_app = DiscoveryAppln(logger)
        logger.debug("Main: configure the Discovery appln object")
        discovery_app.configure(args)
        logger.debug("Main: invoke the Discovery appln driver")
        discovery_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()