# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We can execute locally or in Mininet or Cloud native environments
#

import sys
import argparse  # command line parsing
import logging  # for logger
import random # for random number generator for the sleep param
import time # for sleep

# we need to import this package
import zmq

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="Peer Application")
  
  # Now specify all the optional arguments we support
  
  parser.add_argument ("-n", "--name", default="Peer", help="This peer's name, default = Peer")

  # I have kept this here but we are not using it.
  parser.add_argument ("--spp", action="store_true", help="Whether to create one REQ socket per peer or not. Use --no-spp for single socket")

  parser.add_argument ("-p", "--port", type=int, default=4444, help="Port number on which this pier executes, default 4444")
  
  parser.add_argument ("-u", "--url", default="localhost:4444", help="URL of the peer we connect to. Could be multiple comma separated")
    
  parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")
  
  return parser.parse_args()


###################################
#
# Main program
#
###################################
def main ():
  try:
    # obtain a system wide logger and initialize it to debug level to begin with
    logging.info ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger ("Peer")
    
    logger.debug ("Current libzmq version is %s" % zmq.zmq_version())
    logger.debug ("Current  pyzmq version is %s" % zmq.__version__)

    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # To use ZMQ, every session needs this singleton context object
    logger.debug ("Acquire ZMQ context")
    context = zmq.Context()

    # get the poller
    logger.debug ("Acquire the poller object")
    poller = zmq.Poller ()

    #  Socket to respond to peer's request. Note that because the peer in the role of
    # server responds or replies, we create a REP socket
    logger.debug ("Obtain a REP socket for replies")
    rep = context.socket (zmq.REP)

    # Now bind this to the port we are listening on
    bind_str = "tcp://*:" + str (args.port)
    logger.debug ("Bind the REP socket for replies")
    rep.bind (bind_str)

    # register this socket for POLLIN events
    logger.debug ("Register the REP socket for POLLIN events")
    poller.register (rep, zmq.POLLIN)
    
    #  Socket to talk to other peers. Note that because the client is the "requestor", we
    # create a socket of type REQ.
    # maintain socket per peer. We could have maintained the same socket to all peers
    # but ZMQ load balances requests and we want to test one to one connections. Hence
    # one socket per peer.
    req = []
    logger.debug ("Connecting to peers {}".format (args.url))
    # our URL could be a comma separated list. So we create a socket and
    # connect to each. 
    connect_str_list = args.url.split (",")
    for item in connect_str_list:
      # create one socket per peer
      socket = context.socket (zmq.REQ)
      
      # connect it
      connect_str = "tcp://" + item
      socket.connect (connect_str)
      
      # register this socket for POLLIN events
      logger.debug ("Register the REQ socket for POLLIN events")
      poller.register (socket, zmq.POLLIN)
      
      # add to our list of req sockets
      req.append (socket)

    # To kickstart things, send some dummy message to all the peer(s)
    req_num = 1  # start with request number 1
    logger.debug ("Peer sending the first request")
    request = args.name + " Request #" + str (req_num)
    for i in range (len (req)):
      req[i].send_string (request)
    
    # handle incoming requests and propagate ahead if there is  chain
    while True:

      logger.debug ("Poller waiting for next event")
      events = dict (poller.poll ())

      # now check which socket is enabled
      if (rep in events):
        # incoming request from previous tier
        incoming_request = rep.recv_string ()
        logger.debug ("Received incoming request from peer: {}".format (incoming_request))
        # send dummy response
        reply = args.name + "->" + incoming_request
        logger.debug ("Replying {} to peer". format (reply))
        rep.send_string (reply)

      # We should be receiving responses from our peers. 
      req_num += 1  # next request num to be sent
        
      # check which of the peer connected sockets were enabled
      for i in range (len (req)):
        if req[i] in events:
          # incoming request from peer  i
          incoming_reply = req[i].recv_string ()
          logger.debug ("Received reply: {}".format (incoming_reply))

          # now send the next request to this peer
          logger.debug ("Peer sending the next request to peer")
          request = args.name + " Request #" + str (req_num)
          req[i].send_string (request)

        # sleep some time and send next request
        sleep_time = random.choice ([0.2, 0.4, 0.6, 0.8, 1.0])
        time.sleep (sleep_time)
        
      
  except Exception as e:
    logger.error ("Exception caught in main - {}".format (e))
    return

    
###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  main ()
    
