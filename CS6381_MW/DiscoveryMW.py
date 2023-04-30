import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch
import time
import json
import random

class DiscoveryMW(PinguMW):
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__ (self, logger):
        super().__init__(logger)
        self.rep = None # will be a ZMQ REP socket for reply
        self.zk = None # for zookeeper client
        self.pub = None # Publisher from leader to replicas
        self.sub = None # Subscriber from leader to replicas
        
        # New variables for PA4
        self.owners = {} # Mapping of topics to their respective owner
        self.assigned_topics = {} # Mapping of publishers to topics they own
        
    @handle_exception
    def configure (self, args):
        self.logger.info("DiscoveryMW::configure")
        self.port = args.port
        self.addr = args.addr
        context = zmq.Context()  # returns a singleton object
        self.poller = zmq.Poller()
        self.rep = context.socket(zmq.ROUTER)
        self.poller.register(self.rep, zmq.POLLIN)
        bind_string = "tcp://*:" + str(self.port)
        self.rep.bind(bind_string)
        self.pub = context.socket(zmq.PUB)
        self.sub = context.socket(zmq.SUB)
        self.poller.register(self.sub, zmq.POLLIN)
        bindString = "tcp://*:" + str(self.port + 1)
        self.pub.bind(bindString)
        self.logger.info("DiscoveryMW::configure: create ZK client")
        self.zk = KazooClient(hosts=args.zookeeper)
        self.quorum = args.quorum
        self.assureQuorum(args.name)
        self.logger.info("DiscoveryMW::configure: ZK client state = {}".format(self.zk.state))
        self.logger.info("DiscoveryMW::configure completed")
        
    # run the event loop where we expect to receive sth
    @handle_exception
    def event_loop(self, timeout=None):
        self.logger.info("DiscoveryMW::event_loop - start")
        while self.handle_events:
            events = dict(self.poller.poll(timeout=timeout))
            if not events:
                timeout = self.upcall_obj.invoke_operation()
            elif self.rep in events:
                timeout = self.handle_request()
            elif self.sub in events:
                timeout = self.receiverFromLeader()
            else:
                raise Exception("Unknown event after poll")
        self.logger.info("DiscoveryMW::event_loop - end")
        
    @handle_exception
    def handle_request(self):
        self.logger.info("DiscoveryMW::handle_request")
        bytesRcvd = self.rep.recv()
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.ParseFromString(bytesRcvd)
        self.logger.info("DiscoveryMW::handle_request - bytes received")
        if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
            self.logger.info("DiscoveryMW::handle_request - register")
            timeout = self.upcall_obj.register_request(disc_req.register_req)
        elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
            self.logger.info("DiscoveryMW::handle_request - is ready")
            timeout = self.upcall_obj.isready_request()
        elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
            self.logger.info("DiscoveryMW::handle_request - all pubs")
            timeout = self.upcall_obj.handle_all_publist()
        elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
            self.logger.info("DiscoveryMW::handle_request - pub by topic")
            timeout = self.upcall_obj.handle_topic_request(disc_req.lookup_req)
        else: 
            raise ValueError("Unrecognized response message")
        return timeout

    @handle_exception    
    def handle_register(self, status, reason):
        self.logger.info("DiscoveryMW::handle_register:: check whether the registration has been successful")
        register_response = discovery_pb2.RegisterResp() 
        if status: # if status is true, registration = successful
            # Assign an owner for the newly registered publisher's topic
            topic = disc_req.register_req.topic
            self.assignOwner(topic)
            register_response.status = discovery_pb2.Status.STATUS_SUCCESS
        else: # otherwise failure
            register_response.status = discovery_pb2.Status.STATUS_FAILURE
        register_response.reason = reason
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_REGISTER
        discovery_response.register_resp.CopyFrom(register_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)
        self.logger.info("DiscoveryMW::handle_register:: registration status has been checked. plz check the message")
        return 0

    @handle_exception        
    def update_is_ready_status(self, is_ready):
        self.logger.info("DiscoveryMW::update_is_ready_status:: Start this method")
        ready_response = discovery_pb2.IsReadyResp() 
        ready_response.status = is_ready
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_ISREADY
        discovery_response.isready_resp.CopyFrom(ready_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)
        self.logger.info("DiscoveryMW::update_is_ready_status:: is_ready status sent.")

    @handle_exception
    def send_pubinfo_for_topic(self, pub_in_topic):
        self.logger.info("DiscoveryMW::send_pubinfo_for_topic:: Start this method")
        lookup_response = discovery_pb2.LookupPubByTopicResp() 
        for pub in pub_in_topic:
            reg_info = lookup_response.publisher_info.add()
            reg_info.id = pub[0] # name
            reg_info.addr = pub[1] # addr
            reg_info.port = pub[2] # port
            self.logger.info("DiscoveryMW::send_pubinfo_fo_topic:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        discovery_response.lookup_resp.CopyFrom(lookup_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)  
        self.logger.info ("DiscoveryMW::send_pubinfo_for_topic:: List of publishers sent")

    @handle_exception    
    def send_all_pub_list(self, pub_list):
        self.logger.info ("DiscoveryMW::send_all_pub_list:: Start this method")
        lookup_response = discovery_pb2.LookupAllPubsResp()
        for pub in pub_list:
            reg_info = lookup_response.publist.add()
            reg_info.id = pub[0] # name
            reg_info.addr = pub[1] # addr
            reg_info.port = pub[2] # port
            self.logger.info("DiscoveryMW::send_all_pub_list:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_response.allpubs_resp.CopyFrom(lookup_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)

    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()
        
    @handle_exception     
    def assureQuorum(self, name):
        self.logger.info("DiscoveryMW::assureQuorum - start")
        self.zk.start()
        self.logger.info("DiscoveryMW::assureQuorum: ZK client state = {}".format(self.zk.state))
        self.zk.create("/discovery/" + name, ephemeral=True, makepath=True)
        while len(self.zk.get_children("/discovery")) < self.quorum:
            self.logger.info("DiscoveryMW::assureQuorum: quorum_size not met, waiting for 3s")
            time.sleep(3)
            self.logger.info("DiscoveryMW::assureQuorum: quorum_size met, creating leader")
            self.createLeader(name)
    
    @handle_exception
    def createLeader(self, name):            
        try:
            self.logger.info("DiscoveryMW::createLeader: - start")
            repAddress = "tcp://" + self.addr + ":" + str(self.port)
            pubAddress = "tcp://" + self.addr + ":" + str(self.port + 1)
            metaJSON = json.dumps({"name": name, "repAddress": repAddress, "pubAddress": pubAddress})
            self.zk.create("/leader", value=metaJSON.encode("utf-8"), ephemeral=True, makepath=True)
            self.logger.info("DiscoveryMW::createLeader: leader created")
        except NodeExistsError:
            self.logger.info("DiscoveryMW::createLeader: leader already exists, connecting to leader through the SUB socket")
            metadata = json.loads(self.zk.get("/leader")[0].decode("utf-8"))
            self.logger.info("DiscoveryMW::createLeader: leader address = {}".format(metadata["pubAddress"]))
            self.sub.connect(metadata["pubAddress"])
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "backup")
            return
     
    @DataWatch("/leader")
    @handle_exception
    def watchLeader(self, data):
        if data is None:
            self.logger.info("DiscoveryMW::watchLeader - start")
            self.createLeader()
        
    @DataWatch("/broker")
    @handle_exception
    def watchBroker(self, data):
        if data:
            self.logger.info("DiscoveryMW::watchBroker - start")
            aboutBroker = json.loads(data.decode("utf-8"))
            self.upcall_obj.setBrokerInfo(aboutBroker)

    @handle_exception
    def waitBroker(self):
        while not self.zk.exists("/broker"):
            time.sleep(1)
    
    @handle_exception
    def sendStateReplica(self, topics2pubs, pubs2ip, no_pubs, no_subs, state):
        self.logger.info("DiscoveryMW::send_state_to_replica - start")
        topics2pubsJSON = json.dumps(topics2pubs)
        pubs2ipJSON = json.dumps(pubs2ip)
        self.pub.send_multipart([ topics2pubsJSON.encode("utf-8"), pubs2ipJSON.encode("utf-8"), no_pubs, no_subs])
    
    @handle_exception
    def receiverFromLeader(self):
        dataReceived = self.sub.recv_multipart()
        topics2pubs = json.loads(dataReceived[0].decode("utf-8"))
        pubs2ip = json.loads(dataReceived[1].decode("utf-8"))
        self.upcall_obj.setState(topics2pubs, pubs2ip, dataReceived[2], dataReceived[3])
        
    """
    New functions for PA4
    assignOwner: randomly assign a publisher as owner of a topic
    """
    @handle_exception
    def assignOwner(self, topic):
        publishers = self.upcall_obj.topics2pubs[topic]
        if not publishers:
            return None
        owner = random.choice(publishers)
        self.owners[topic] = owner
        return owner