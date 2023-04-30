import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from enum import Enum  # for an enumeration we are using to describe what state we are in
from functools import wraps # for a decorator we are using to handle exceptions

class BrokerAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        CHECKMSG = 4,
        RECEIVEANDDISSEMINATE = 5,
        COMPLETED = 6
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__ (self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics that we publish on
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.msg_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = None
    
    @handle_exception
    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency # frequency with which topics are disseminated
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        self.mw_obj = BrokerMW(self.logger)
        self.mw_obj.configure(args) # pass remainder of the args to the m/w object
        self.topiclist = ["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location"] # Subscribe to all topics
        self.logger.info("BrokerAppln::configure - configuration complete")

    @handle_exception    
    def driver(self):
        self.logger.info("BrokerAppln::driver")
        self.dump()
        self.logger.info("BrokerAppln::driver - upcall handle")
        self.mw_obj.set_upcall_handle(self)
        self.mw_obj.setWatch()
        self.state = self.State.REGISTER
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("BrokerAppln::driver completed")
    
    # Modified for PA4
    @handle_exception    
    def invoke_operation(self):
        self.logger.info("BrokerAppln::invoke_operation - state: {}".format(self.state))
        if self.state == self.State.REGISTER:
            return None
        elif self.state == self.State.ISREADY:
            return None
        elif self.state == self.State.CHECKMSG:
            time.sleep(5)  # Adjust the interval as needed
            self.loadBalancing()
            self.checkQOS()

    @handle_exception
    def setSubscription(self, publist):
        self.logger.info("BrokerAppln::setSubscription")
        self.mw_obj.subscribe(publist)
    
    """
    New code for PA4
    1. loadBalancing() -> implements load balancing by redistributing messages to available brokers
    2. checkQOS() -> calculates metrics such as latency and throughput.
    """
    @handle_exception
    def loadBalancing(self):
        # Get the current load on the broker (e.g., number of messages in the queue)
        current_load = len(self.msg_list)
        # Set a threshold for load balancing
        load_threshold = 100
        # If the current load exceeds the threshold, trigger load redistribution
        if current_load > load_threshold:
            # Communicate with the Discovery service to get the list of available brokers
            other_brokers = self.mw_obj.get_available_brokers()  
            # Exclude the current broker from the list
            other_brokers = [broker for broker in other_brokers if broker["addr"] != self.addr or broker["port"] != self.port]
            if other_brokers:
                # Calculate the number of messages to be redistributed to each broker
                num_brokers = len(other_brokers)
                messages_per_broker = current_load // (num_brokers + 1)
                # Send the excess messages to the other brokers
                for broker in other_brokers:
                    # Determine the messages to be sent to this broker
                    messages_to_send = self.msg_list[:messages_per_broker]
                    # Remove the messages from the current broker's queue
                    self.msg_list = self.msg_list[messages_per_broker:]
                    # Send the messages to the target broker
                    self.mw_obj.send_messages_to_broker(broker["addr"], broker["port"], messages_to_send)  
                self.logger.info("Load redistribution completed.")
        else:
            self.logger.warning("No other brokers available for load redistribution.")

    @handle_exception
    def checkQOS(self):
        # Calculate metrics such as latency or throughput
        # For simplicity, let's assume you store the timestamps of message arrivals in a list called `msg_timestamps`
        msg_timestamps = []  # This should be populated with actual timestamps
        # Calculate the average latency of the last 10 messages
        if len(msg_timestamps) > 10:
            last_10_timestamps = msg_timestamps[-10:]
            latencies = [t2 - t1 for t1, t2 in zip(last_10_timestamps[:-1], last_10_timestamps[1:])]
            avg_latency = sum(latencies) / len(latencies)
            self.logger.info(f"Average latency of the last 10 messages: {avg_latency}") 
        # Calculate the throughput (messages per second) in the last second
        now = time.time()
        last_second_msgs = [t for t in msg_timestamps if now - t < 1]
        throughput = len(last_second_msgs)
        self.logger.info(f"Throughput in the last second: {throughput} messages per second")
    
    @handle_exception
    def dump(self):
        self.logger.info("**********************************")
        self.logger.info("BrokerAppln::dump")
        self.logger.info("     Name: {}".format (self.name))
        self.logger.info("     Number of nodes in distributed hash table: {}".format(self.M))
        self.logger.info("     Lookup: {}".format (self.lookup))
        self.logger.info("     Dissemination: {}".format (self.dissemination))
        self.logger.info("     Iterations: {}".format (self.iters))
        self.logger.info("     Frequency: {}".format (self.frequency))
        self.logger.info ("**********************************")

# Parse command line arguments
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="broker", help="broker")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this broker to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number on which our underlying Broker ZMQ service runs, default=5576")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default dht.json") # changed to dht.json from localhost:5555
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IPv4 address for the zookeeper service, default = localhost:2181")
    return parser.parse_args()

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        broker_app = BrokerAppln (logger)
        # configure the object
        logger.debug("Main: configure the Broker appln object")
        broker_app.configure(args)
        # now invoke the driver program
        logger.debug("Main: invoke the Broker appln driver")
        broker_app.driver()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()