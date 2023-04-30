import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch
import json
import timeit
import time

class PublisherMW(PinguMW):
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
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.zk = None 
    self.disc = None 
    self.name = None 
    
    # New variables for PA4
    self.ownership_strength = {}  # Ownership strength per topic
    self.history = {}  # History of samples per topic
    self.history_size = {}  # Maximum history size per topic


  @handle_exception
  def configure(self, args):
    self.logger.info("PublisherMW::configure")
    self.port = args.port
    self.addr = args.addr
    context = zmq.Context()  # returns a singleton object
    self.poller = zmq.Poller()
    self.zk = KazooClient(hosts=args.zookeeper)
    self.req = context.socket(zmq.REQ)
    self.pub = context.socket(zmq.PUB)
    self.poller.register(self.req, zmq.POLLIN)
    self.setRequest()
    connect_str = "tcp://" + args.discovery
    self.req.connect(connect_str)
    bind_string = "tcp://*:" + str(self.port)
    self.pub.bind (bind_string)
    
    @self.zk.DataWatch("/leader")
    def watchLeader(data, stat):
      meta = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
      self.logger.info("PublisherMW::watch_leader: disconnecting req and redirecting to new leader")
      if self.disc != None:
        self.req.disconnect(self.disc)
      self.req.connect(meta["repAddress"])
      self.disc = meta["repAddress"]
      self.logger.info("Successfully connected to the new leader")  
    self.logger.info("PublisherMW::configure completed")

  def event_loop(self, timeout=None):
   super().event_loop("PublisherMW", self.req, timeout)
            
  @handle_exception
  def handle_reply(self):
    self.logger.info("PublisherMW::handle_reply")
    bytesRcvd = self.req.recv()
    discovery_response = discovery_pb2.DiscoveryResp()
    discovery_response.ParseFromString(bytesRcvd)
    if discovery_response.msg_type == discovery_pb2.TYPE_REGISTER:
      timeout = self.upcall_obj.register_response(discovery_response.register_resp)
    elif discovery_response.msg_type == discovery_pb2.TYPE_ISREADY:
      timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
    else:
      raise ValueError("Unrecognized response message")
    return timeout
            

  def is_ready(self):
    super().is_ready("PublisherMW")
            
  # here we save a pointer (handle) to the application object
  def set_upcall_handle(self, upcall_obj):
    super().set_upcall_handle(upcall_obj)
        
  def disable_event_loop(self):
    super().disable_event_loop()
    
  @handle_exception
  def register(self, name, topiclist):
    self.logger.info("PublisherMW::register - register publisher to ZK")
    data = {}
    data["id"] = {"id": name, "addr": self.addr, "port": self.port} 
    data["topiclist"] = topiclist
    data_json = json.dumps(data)
    self.zk.create("/publisher/" + self.name, value=data_json.encode("utf-8"), ephemeral=True, makepath=True)
    self.logger.info ("PublisherMW::register - sent register message and now now wait for reply")
  
  @handle_exception
  def setRequest(self):
    self.zk.start()
    while self.zk.exists("/leader") == None:
      time.sleep(1)
    metadata = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
    self.req.connect(metadata["repAddress"])
    self.logger.debug("Successfully connected to the leader")
    
  # Modified for PA4
  @handle_exception
  def disseminate(self, id, topic, data, current_time, qos=None, ownership=None, history_size=None):
    send_str = topic + ":" + id + ":" + data + ":" + current_time
    self.logger.info("PublisherMW::disseminate - {}".format (send_str))
    self.pub.send(bytes(send_str, "utf-8"))
    # Update history
    if topic not in self.history:
      self.history[topic] = []
      self.history_size[topic] = history_size or self.history_size.get(topic, 10)
    self.history[topic].append((id, data, current_time, qos, ownership))
    if len(self.history[topic]) > self.history_size[topic]:
      self.history[topic].pop(0)