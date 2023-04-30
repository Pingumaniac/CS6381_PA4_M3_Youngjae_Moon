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
import timeit 
import signal 
import csv 

class SubscriberMW(PinguMW):
  def handle_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
      try:
        return func(*args, **kwargs)
      except Exception as e:
        raise e
    return wrapper

  def __init__(self, logger):
    super().__init__(logger)
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.sub = None # will be a ZMQ SUB socket for dissemination
    self.zk = None # for zookeeper client
    self.disc= None
    self.lookupMethod = None
    # New variables for PA4
    self.requested_qos = None  # Add requested_qos attribute

  @handle_exception
  def configure(self, args):
    self.logger.info("SubscriberMW::configure")
    self.port = args.port
    self.addr = args.addr
    context = zmq.Context()  # returns a singleton object
    self.poller = zmq.Poller()
    self.req = context.socket(zmq.REQ)
    self.sub = context.socket(zmq.SUB)
    self.poller.register(self.req, zmq.POLLIN)
    self.poller.register(self.sub, zmq.POLLIN)
    connect_str = "tcp://" + args.discovery
    self.req.connect(connect_str)
    self.zk = KazooClient(hosts=args.zookeeper)
    self.zk.start()
    self.setRequest()
    self.logger.info("SubscriberMW::configure completed")

  def event_loop(self, timeout=None):
    super().event_loop("SubscriberMW", self.req, timeout)

  @handle_exception
  def handle_reply(self):
    self.logger.info("SubscriberMW::handle_reply")
    bytesRcvd = self.req.recv()
    discovery_response = discovery_pb2.DiscoveryResp()
    discovery_response.ParseFromString(bytesRcvd)
    if discovery_response.msg_type == discovery_pb2.TYPE_REGISTER:
      timeout = self.upcall_obj.register_response(discovery_response.register_resp)
    elif discovery_response.msg_type == discovery_pb2.TYPE_ISREADY:
      timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
    elif discovery_response.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
      timeout = self.upcall_obj.receiveSubscribedPublishersResponse(discovery_response.lookup_resp)
    else: 
      raise ValueError ("Unrecognized response message")
    return timeout
            
  def register (self, name, topiclist):
    super().register("SubscriberMW", name, topiclist)

  def is_ready(self):
    super().is_ready("SubscriberMW")

  @handle_exception
  def receiveSubscribedPublishers(self, topiclist):
    self.logger.info("SubscriberMW::receiveSubscribedPublishers - start")
    lookup_request = discovery_pb2.LookupPubByTopicReq()
    lookup_request.topiclist[:] = topiclist
    discovery_request = discovery_pb2.DiscoveryReq()
    discovery_request.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
    discovery_request.lookup_req.CopyFrom(lookup_request)
    buf2send = discovery_request.SerializeToString()
    self.req.send(buf2send) 
    self.logger.info("SubscriberMW::receiveSubscribedPublishers - end")
  
  @handle_exception
  def makeSubscription(self, pub, topiclist):
    self.logger.info("SubscriberMW::makeSubscription - start")
    self.connect2pubs(pub.addr, pub.port)
    for topic in topiclist:
      self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
      self.logger.info("SubscriberMW::makeSubscription - topic: {}".format(topic))
    
  @handle_exception
  def receive(self):
    self.logger.info("SubscriberMW:: receive messages")
    msg = self.sub.recv_string()
    self.logger.info("SubscriberMW:: received message = {}".format (msg))
    return msg 
            
  # here we save a pointer (handle) to the application object
  def set_upcall_handle(self, upcall_obj):
    super().set_upcall_handle(upcall_obj)
        
  def disable_event_loop(self):
    super().disable_event_loop()
  
  @handle_exception
  def connect2pubs(self, IP, port):
    connect_str = "tcp://" + IP + ":" + str(port)
    self.logger.info("SubscriberMW:: connect2pubs method. connect_str = {}".format(connect_str))
    self.sub.connect(connect_str)
  
  @handle_exception
  def setRequest(self):
    while self.zk.exists("/leader") == None:
      time.sleep(2)
      metadata = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
      if self.disc != None:
        self.logger.info("SubscriberMW::setRequest:: disconnecting from {}".format(self.disc))
        self.req.disconnect(self.disc)
        self.req.connect(metadata["repAddress"])
        self.disc = metadata["repAddress"]
        self.logger.info("SubscriberMW::setRequest:: - successfully connected to the leader")
  
  @handle_exception
  def setWatch(self):
    @self.zk.DataWatch("/leader")
    def watchLeader():
      self.logger.info("SubscriberMW::watchLeader:: Leader node has changed")
      self.setRequest()
      return
          
    @self.zk.DataWatch("/broker")
    def watchBroker(data):
      self.logger.info("SubscriberMW::watchBroker:: Broker node has changed")
      if self.lookup_method == "Broker" and data is not None:
        metadata = json.loads(self.zk.get("/broker")[0].decode('utf-8'))
        self.writeToCSV([metadata])

    @self.zk.ChildrenWatch("/publisher")
    def watchPublishers(children):
      self.logger.info("SubscriberMW::watchPublishers:: Publishers have changed, re-subscribing")
      if self.lookup_method == "Direct":
        pubs = []
        for c in children:
          path = "/publisher/" + c
          data, _ = self.zk.get(path)
          pubs.append(json.loads(data.decode("utf-8")))
          self.logger.info("DiscoveryMW::watchPublishers:: {}".format(pubs))
          self.upcall_obj.setPublisherInfo(pubs)

  @handle_exception
  def writeToCSV(self, publist):
    with open(self.filename, "w", newline='') as f:
      writer = csv.writer(f)
      writer.writerow(["Time", "Latency"])
      self.startTime = timeit.default_timer()
      for pub in publist:
        addr = "tcp://" + pub.addr + ":" + str(pub.port)
        self.sub.connect(addr)
  
  # New functions for PA4
  @handle_exception
  def handle_load_balanced_publishers(self, publishers):
    self.logger.info("SubscriberMW::handle_load_balanced_publishers - start")
    for pub in publishers:
      self.makeSubscription(pub, pub.topiclist)
    self.logger.info("SubscriberMW::handle_load_balanced_publishers - end")
  
  @handle_exception
  def upcall_obj_set_publisher_info(self, publishers):
    self.upcall_obj.setPublisherInfo(publishers)
  
  # Add the matching method to take history QoS into account
  def matching(self, offered_qos, requested_qos):
    # Check for history QoS compatibility
    if offered_qos.history_size < requested_qos.history_size:
      return False
    else:
      return True