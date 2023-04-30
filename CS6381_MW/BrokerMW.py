import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps
import time
import json
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch

# Import for PA4
from collections import defaultdict
import threading

class BrokerMW(PinguMW):
    def __init__ (self, logger):
        super().__init__(logger)
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None # will be a ZMQ XPUB socket for representing publisher
        self.sub = None # will be a ZMQ XSUB socket for representing publisher
        # New variables for PA4
        self.topic_owners = {} # Topic ownership mapping
        self.qos_levels = defaultdict(int) # Default QoS level for topics
        self.pending_messages = defaultdict(list) # Messages waiting for acknowledgement
        self.lock = threading.Lock() # Mutex for thread-safe operations

        
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    @handle_exception
    def configure (self, args):
        self.logger.info("BrokerMW::configure")
        self.port = args.port
        self.addr = args.addr
        context = zmq.Context()  # returns a singleton object
        self.poller = zmq.Poller()
        self.req = context.socket(zmq.REQ)
        self.pub = context.socket(zmq.XPUB)
        self.sub = context.socket(zmq.XSUB)
        self.poller.register(self.req, zmq.POLLIN)
        self.poller.register(self.sub, zmq.POLLIN)
        connect_str = "tcp://" + args.discovery
        self.req.connect(connect_str)
        bind_string = "tcp://*:" + str(self.port)
        self.pub.bind(bind_string)
        self.logger.info("BrokerMW::configure completed")
    
    @handle_exception
    def handle_reply(self):
        self.logger.info("BrokerMW::handle_reply")
        bytesRcvd = self.req.recv()
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.ParseFromString(bytesRcvd)
        if (discovery_response.msg_type == discovery_pb2.TYPE_REGISTER):
            timeout = self.upcall_obj.register_response(discovery_response.register_resp)
        elif (discovery_response.msg_type == discovery_pb2.TYPE_ISREADY):
            timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
        elif (discovery_response.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
            timeout = self.upcall_obj.allPublishersResponse(discovery_response.allpubs_resp)
        else: 
            raise ValueError ("Unrecognized response message")
        return timeout
    
    """
    New functions added and modified for PA4:
    1. register(): Registers a broker with a name, a list of topics, and optionally topic 
    ownership and QoS levels.
    2. update_topic_ownership(): Updates the topic ownership list for the broker.
    3. update_qos_levels(): Updates the QoS levels for the broker's subscribed topics.
    4. send_msg_pub(): Sends a message to a topic with the specified QoS level, 
    defaulting to the topic's QoS level if not provided.
    5. send_msg_pub_qos0(): Sends a message to a topic with QoS level 0 (fire-and-forget).
    6. send_msg_pub_qos1(): Sends a message to a topic with QoS level 1 (acknowledged delivery).
    7. send_msg_pub_qos2(): Sends a message to a topic with QoS level 2 (exactly-once delivery).
    8. receive_msg_sub(): Receives a message from a subscribed topic with the specified QoS level, 
    defaulting to the topic's QoS level if not provided.
    9. receive_msg_sub_qos0(): Receives a message from a subscribed topic with QoS level 0 (fire-and-forget).
    10. receive_msg_sub_qos1(): Receives a message from a subscribed topic with QoS level 1 
    (acknowledged delivery) and sends an acknowledgement.
    11. receive_msg_sub_qos2(): Receives a message from a subscribed topic with QoS level 2 
    (exactly-once delivery) and sends an acknowledgement.
    12. acknowledge_msg(): Acknowledges the receipt of a message with the given ID for the 
    specified topic.
    13. report_load(): Sends a load report message to the Discovery service and receives a response.
    14. create_load_report_message(): Creates a load report message in JSON format.
    15. event_loop(): Runs the broker's event loop, reporting load status to the Discovery service 
    every 5 seconds.
    """
    
    @handle_exception
    def register(self, name, topiclist, ownership_list=None, qos_levels=None):
        super().register("BrokerMW", name, topiclist)
        if ownership_list:
            self.update_topic_ownership(ownership_list)
        if qos_levels:
            self.update_qos_levels(qos_levels)

    @handle_exception
    def update_topic_ownership(self, ownership_list):
        with self.lock:
            for topic in ownership_list:
                self.topic_owners[topic] = True

    @handle_exception
    def update_qos_levels(self, qos_levels):
        with self.lock:
            for topic, level in qos_levels.items():
                self.qos_levels[topic] = level
                
    @handle_exception
    def send_msg_pub(self, send_str, topic, qos=None):
        if qos is None:
            qos = self.qos_levels.get(topic, 0)
        if qos == 0:
            self.send_msg_pub_qos0(send_str)
        elif qos == 1:
            self.send_msg_pub_qos1(send_str, topic)
        elif qos == 2:
            self.send_msg_pub_qos2(send_str, topic)
        else:
            self.logger.error(f"Invalid QoS level: {qos}")

    @handle_exception
    def send_msg_pub_qos0(self, send_str):
        self.logger.info("BrokerMW::send_msg_pub_qos0 - disseminate messages to subscribers from broker")
        self.logger.info("BrokerMW::send_msg_pub_qos0 - {}".format(send_str))
        self.pub.send(bytes(send_str, "utf-8"))

    @handle_exception
    def send_msg_pub_qos1(self, send_str, topic):
        # Implement QoS Level 1 message sending
        self.logger.info("BrokerMW::send_msg_pub_qos1 - disseminate messages to subscribers from broker with QoS 1")
        self.logger.info("BrokerMW::send_msg_pub_qos1 - {}".format(send_str))
        msg_id = str(uuid.uuid4())
        msg_payload = {"id": msg_id, "content": send_str}
        self.pending_messages[topic].append(msg_payload)
        self.pub.send_json(msg_payload)

    @handle_exception
    def send_msg_pub_qos2(self, send_str, topic):
        # Implement QoS Level 2 message sending
        # QoS 2 is similar to QoS 1 but with additional acknowledgment steps.
        self.logger.info("BrokerMW::send_msg_pub_qos2 - disseminate messages to subscribers from broker with QoS 2")
        self.logger.info("BrokerMW::send_msg_pub_qos2 - {}".format(send_str))
        msg_id = str(uuid.uuid4())
        msg_payload = {"id": msg_id, "content": send_str}
        self.pending_messages[topic].append(msg_payload)
        self.pub.send_json(msg_payload)

    @handle_exception
    def receive_msg_sub(self, topic):
        if not self.topic_owners.get(topic, False):
            self.logger.error(f"Received message for topic {topic} without ownership")
            return None
        msg = self.sub.recv_string()
        qos = self.qos_levels.get(topic, 0)
        if qos == 0:
            return self.receive_msg_sub_qos0(msg)
        elif qos == 1:
            return self.receive_msg_sub_qos1(msg, topic)
        elif qos == 2:
            return self.receive_msg_sub_qos2(msg, topic)
        else:
            self.logger.error(f"Invalid QoS level: {qos}")
            return None

    @handle_exception
    def receive_msg_sub_qos0(self, msg):
        self.logger.info("BrokerMW::recv_msg_sub_qos0 - received message = {}".format(msg))
        return msg

    @handle_exception
    def receive_msg_sub_qos1(self, msg, topic):
        self.logger.info("BrokerMW::recv_msg_sub_qos1 - received message with QoS 1 = {}".format(msg))
        msg_json = json.loads(msg)
        msg_id = msg_json.get("id")
        if msg_id:
            self.acknowledge_msg(topic, msg_id)
        return msg_json.get("content")

    @handle_exception
    def receive_msg_sub_qos2(self, msg, topic):
        # QoS 2 is similar to QoS 1 but with additional acknowledgment steps.
        self.logger.info("BrokerMW::recv_msg_sub_qos2 - received message with QoS 2 = {}".format(msg))
        msg_json = json.loads(msg)
        msg_id = msg_json.get("id")
        if msg_id:
            self.acknowledge_msg(topic, msg_id)
        return msg_json.get("content")

    @handle_exception
    def acknowledge_msg(self, topic, msg_id):
        with self.lock:
            for i, msg in enumerate(self.pending_messages[topic]):
                if msg["id"] == msg_id:
                    del self.pending_messages[topic][i]
                    break
    
    @handle_exception
    def report_load(self):
        # Create a load report message
        load_report_msg = self.create_load_report_message()
        # Send the load report message to the Discovery service
        self.req.send_string(load_report_msg)
        # Receive the response from the Discovery service
        response = self.req.recv_string()
    
    @handle_exception
    def create_load_report_message(self):
        # Create and populate a load report message using JSON
        load_report = {
            "msg_type": "LOAD_REPORT",
            "load_value": "sample_load_value"  # replace with actual load value
        }
        # Serialize the message to a JSON string
        serialized_msg = json.dumps(load_report)
        return serialized_msg

    @handle_exception
    def event_loop(self, timeout=None):
        while not self.exit_flag:
            # Run the event loop
            super().event_loop("BrokerMW", self.req, timeout)
            # Report load status every 5 seconds (or any other desired interval)
            time.sleep(5)
            self.report_load()
            
    
    def send_messages_to_broker(self, addr, port, messages):
        # Create a new ZMQ socket to communicate with the target broker
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect(f"tcp://{addr}:{port}")
        # Send the messages to the target broker
        for msg in messages:
            socket.send_json(msg)
        # Close the socket after sending the messages
        socket.close()
    
    def is_ready(self):
        super().is_ready("BrokerMW")
    
    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()
    
   
    @handle_exception
    def receiveAllPublishers(self):
        self.logger.info("BrokerMW::receiveAllPublishers - start")
        allpubs_request = discovery_pb2.LookupAllPubsReq()
        discovery_request = discovery_pb2.DiscoveryReq()
        discovery_request.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_request.allpubs_req.CopyFrom(allpubs_request)
        buf2send = discovery_request.SerializeToString()
        self.req.send(buf2send) 
        self.logger.info("BrokerMW::receiveAllPublishers - end")

    @handle_exception    
    def connect2pubs(self, IP, port):
        connect_str = "tcp://" + IP + ":" + str(port)
        self.logger.info("BrokerMW:: connect2pubs method. connect_str = {}".format(connect_str))
        self.sub.connect(connect_str)
        
    @handle_exception
    def setRequest(self):
        while self.zk.exists("/leader") == None:
            time.sleep(1)
        meta = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
        if self.discovery != None:
            self.logger.info("SubscriberMW::set_req: disconnecting from {}".format(self.discovery))
            self.req.disconnect(self.discovery)
        self.req.connect(meta["repAddress"])
        self.discovery = meta["repAddress"]
        self.logger.info("BrokerMW::set_req: connected to {}".format(self.discovery))

    @handle_exception
    def setWatch(self):
        @self.zk.DataWatch("/broker")
        def watchBroker(data, stat):
            if data is None:
                self.logger.info("BrokerMW::watchBroker: broker node has been deleted. Trying to become leader")
                self.brokerLeader()
                
        @self.zk.DataWatch("/leader")
        def watchLeader(data, stat):
            self.logger.info("BrokerMW::watchLeader: leader node changed")
            self.setRequest()
        
        @self.zk.ChildrenWatch("/publisher")
        def watchPublishers(children):
            self.logger.info("BrokerMW::watchPublishers: publishers changed, re-subscribing")
            publishers = []
            for c in children:
                path = "/publisher/" + c
                data, _ = self.zk.get(path)
                publishers.append(json.loads(data.decode("utf-8"))['id'])
            self.logger.info("BrokerMW::watch_pubs: {}".format(publishers))
            self.subscribe(publishers)

    @handle_exception
    def brokerLeader(self, name):
        self.zk.start()
        self.logger.info("BrokerMW::brokerLeader: connected to zookeeper")
        try:
            self.logger.info("BrokerMW::brokerLeader: broker node does not exist, creating self")
            addr = {"id": name, "addr": self.addr, "port": self.port}
            data = json.dumps(addr)
            self.zk.create("/broker", value=data.encode('utf-8'), ephemeral=True, makepath=True)
        except NodeExistsError:
            self.logger.info("BrokerMW::brokerLeader: broker node exists")
    
    @handle_exception    
    def subscribe(self, publist):
        self.logger.info("BrokerMW::subscribe")
        for pub in publist:
            addr = "tcp://" + pub['addr'] + ":" + str(pub['port'])
            self.logger.info("BrokerMW::subscribe: subscribing to {}".format(addr))
            self.sub.connect(addr)