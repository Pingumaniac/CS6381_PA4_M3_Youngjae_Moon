from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from functools import wraps

class PinguMW():
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper    

    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num where we are going to publish our topics
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop
        
    @handle_exception
    def event_loop(self, name_of_MW, zmq_socket, timeout=None):
        logmsg = str(name_of_MW) + "::event_loop - run the event loop"
        self.logger.info(logmsg)
        while self.handle_events:  
            events = dict(self.poller.poll (timeout=timeout))
            if name_of_MW == "PublisherMW" or name_of_MW == "SubscriberMW" or name_of_MW == "BrokerMW":
                if not events:  # it starts with a True value
                    timeout = self.upcall_obj.invoke_operation()
                elif zmq_socket in events: 
                    timeout = self.handle_reply()
            else:
                raise Exception("Unknown event after poll")
        logmsg = str(name_of_MW) + "::event_loop - out of the event loop"
        self.logger.info(logmsg)

    @handle_exception
    def register(self, name_of_MW, name, topiclist):
        self.logger.info(str(name_of_MW) + "::register - start")
        reg_info = discovery_pb2.RegistrantInfo()
        reg_info.id = name  # ID
        reg_info.addr = self.addr  # IP 
        reg_info.port = self.port # PORT
        register_req = discovery_pb2.RegisterReq()  
            
        if name_of_MW == "SubscriberMW":
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a subscriber
        elif name_of_MW == "PublisherMW": 
            register_req.role = discovery_pb2.ROLE_PUBLISHER  # we are a publisher
        elif name_of_MW == "BrokerMW":
            register_req.role = discovery_pb2.ROLE_BOTH # we are a broker
                
        register_req.info.CopyFrom(reg_info)  
        register_req.topiclist[:] = topiclist  
        disc_req = discovery_pb2.DiscoveryReq() 
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER  
        disc_req.register_req.CopyFrom(register_req)
        self.logger.info(str(name_of_MW) + "::register - done building the outer message")
        buf2send = disc_req.SerializeToString()
        self.req.send(buf2send) 
        self.logger.info(str(name_of_MW) + "::register - sent register message and now wait for reply")

    @handle_exception
    def is_ready(self, name_of_MW):
        self.logger.info(str(name_of_MW) + "::is_ready - start")
        isready_req = discovery_pb2.IsReadyReq() 
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_ISREADY
        disc_req.isready_req.CopyFrom(isready_req)
        buf2send = disc_req.SerializeToString()
        self.logger.info("Stringified serialized buf = {}".format (buf2send))
        self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes
        self.logger.info(str(name_of_MW) + "::is_ready - request sent and now wait for reply")
    
    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj
        
    def disable_event_loop (self):
        self.handle_events = False