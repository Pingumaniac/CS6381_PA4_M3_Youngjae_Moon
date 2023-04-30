# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose:
#
# Experiment generator where we create scripts that can be run on Mininet
# for now, and later for cloud Kubernetes.
#
# We ask the user to supply the topology, e.g., single,N or linear,N or
# tree,fanout=N,depth=M
# The user must also supply us the number of expected Discovery DHT nodes in the system
# and the number of publishers and subscribers. Since it is possible that multiple publishers
# and discovery DHT nodes may end up on the same host, we must also ensure that
# their port numbers are different. Hence, we also provide the user to define the base port
# number to be used by discovery (e.g., 5555) and publisher (e.g., 7777), and then we
# keep adding 1 to the port if the same service gets deployed on the same node.
#
# Since our topic helper has 9 topics, we also have to make sure that the num of topics
# published or subscribed are 5 or more so that there is some overlap.
#
# Note, our script generation logic will place these entities randomly across the nodes of
# system. We make no effort to load balance or just use round robin allocation (which would
# have been simpler). But this becomes too deterministic and sort of equally distributed
# scenario.

import random  # random number generation
import argparse  # argument parsing
import json  # for JSON
import logging  # for logging. Use it in place of print statements.


class ExperimentGenerator:
    """Experiment generator class."""

    def __init__(self, logger):
        self.num_disc = None  # num of discovery DHT instances
        self.num_pub = None  # num of publishers
        self.num_sub = None  # num of subscribers
        self.num_broker = None
        self.disc_base_port = None  # starting port num if multiple of the same service is deployed on the node
        self.pub_base_port = None  # same for this
        self.broker_base_port = None
        self.num_mn_nodes = None  # num of nodes in mininet topo; will be derived
        self.disc_dict = {}  # dictionary of generated discovery DHT instances
        self.pub_dict = {}  # dictionary of generated publisher instances
        self.sub_dict = {}  # dictionary of generated subscriber instances
        self.broker_dict = {}
        self.script_file = None  # for the experiment script
        self.logger = logger  # The logger

    def configure(self, args):
        """Configure the experiment generator."""
        self.logger.debug("ExperimentGenerator::configure")

        self.num_disc = args.num_disc_dht
        self.num_pub = args.num_pub
        self.num_sub = args.num_sub
        self.num_broker = args.num_broker
        self.disc_base_port = args.disc_base_port
        self.pub_base_port = args.pub_base_port
        self.broker_base_port = args.broker_base_port
        self.script_file = args.script_file

        mn_topo = args.mn_topo.split(",")
        if mn_topo[0] == "single" or mn_topo[0] == "linear":
            self.num_mn_nodes = int(mn_topo[1])
        elif mn_topo[0] == "tree":
            fanout = int(mn_topo[1].split("=")[1])
            depth = int(mn_topo[2].split("=")[1])
            self.num_mn_nodes = fanout**depth
        else:
            raise ValueError("Bad mininet topology")

        # Initialize the dictionaries.
        for i in range(self.num_mn_nodes):
            self.disc_dict["h" + str(i + 1)] = []
            self.pub_dict["h" + str(i + 1)] = []
            self.sub_dict["h" + str(i + 1)] = []
            self.broker_dict["h" + str(i + 1)] = []

    def dump(self):
        """Print the configuration into logs."""

        self.logger.info("*******ExperimentGenerator::DUMP***********")
        self.logger.info(f"Num DHT instances = {self.num_disc}")
        self.logger.info(f"Num pubs = {self.num_pub}")
        self.logger.info(f"Num subs = {self.num_sub}")
        self.logger.info(f"Num brokers = {self.num_broker}")
        self.logger.info(f"Base discovery port = {self.disc_base_port}")
        self.logger.info(f"Base pub port = {self.pub_base_port}")
        self.logger.info(f"Base broker port = {self.broker_base_port}")
        self.logger.info(f"Num Mininet nodes = {self.num_mn_nodes}")
        self.logger.info(f"Discovery dictionary = {self.disc_dict}")
        self.logger.info(f"Publisher dictionary = {self.pub_dict}")
        self.logger.info(f"Subscriber dictionary = {self.sub_dict}")
        self.logger.info(f"Broker dictionary = {self.broker_dict}")
        self.logger.info("**************************************************")

    def gen_dict_values(self, prefix, index):
        """Generate the values for the dictionary for the given prefix and index."""

        # Generate our id
        id = prefix + str(index)

        # Generate a host on which we will deploy this entity
        mn_host_num = random.randint(1, self.num_mn_nodes)
        host = "h" + str(mn_host_num)
        ip = "10.0.0." + str(mn_host_num)

        # Generate the port number
        if prefix == "disc":
            port = self.disc_base_port + len(self.disc_dict[host])
        elif prefix == "pub":
            port = self.pub_base_port + len(self.pub_dict[host])
        elif prefix == "broker":
            port = self.broker_base_port + len(self.broker_dict[host])
        else:
            port = None

        # return the generated parameters
        return id, host, ip, port

    def populate_dict(self, prefix, num_entities):
        """Populate the dictionary with the given prefix and number of entities."""
        self.logger.debug(f"ExperimentGenerator::populate_dict - {prefix} {num_entities}")

        if prefix == "disc":
            target_dict = self.disc_dict
        elif prefix == "pub":
            target_dict = self.pub_dict
        elif prefix == "sub":
            target_dict = self.sub_dict
        elif prefix == "broker":
            target_dict = self.broker_dict
        else:
            raise ValueError(f"populate_dict::unknown prefix: {prefix}")

        for i in range(num_entities):
            id, host, ip, port = self.gen_dict_values(prefix, index=i + 1)
            target_dict[host].append({"id": id, "IP": ip, "port": port})

    def gen_exp_script(self):
        """Generate the bash experiment script."""
        self.logger.debug("ExperimentGenerator::gen_exp_script")

        with open(self.script_file, "w") as f:
            # Add Zookeeper
            cmdline = "h1 /opt/zookeeper/bin/zkServer.sh start-foreground > tests/logs/zk1.txt 2>&1 &\n"
            f.write(cmdline)

            # Add the broker nodes
            for i in range(self.num_mn_nodes):
                host = "h" + str(i + 1)
                host_list = self.broker_dict[host]
                for nested_dict in host_list:
                    cmdline = (
                        f"{host} export PYTHONPATH=$PWD"
                        + " "
                        + "&& python3 src/broker/broker.py"
                        + " "
                        + f"-p {nested_dict['port']}"
                        + " "
                        + f"> tests/logs/{nested_dict['id']}.txt 2>&1 &\n"
                    )
                    f.write(cmdline)

            # Add the discovery nodes
            for i in range(self.num_mn_nodes):
                host = "h" + str(i + 1)
                host_list = self.disc_dict[host]
                for nested_dict in host_list:
                    cmdline = (
                        host
                        + " export PYTHONPATH=$PWD &&"
                        + " python3 src/discovery/discovery.py "
                        + "-n "
                        + nested_dict["id"]
                        + " "
                        + "-a "
                        + str(nested_dict["IP"])
                        + " "
                        + "-p "
                        + str(nested_dict["port"])
                        + " "
                        + "> tests/logs/"
                        + nested_dict["id"]
                        + ".txt 2>&1 &\n"
                    )
                    f.write(cmdline)

            # Add the publisher nodes
            for i in range(self.num_mn_nodes):
                host = "h" + str(i + 1)
                host_list = self.pub_dict[host]
                for nested_dict in host_list:
                    num_topics = random.randint(5, 9)
                    cmdline = (
                        host
                        + " export PYTHONPATH=$PWD &&"
                        + " python3 src/publisher/publisher.py "
                        + "-n "
                        + nested_dict["id"]
                        + " "
                        + "-a "
                        + str(nested_dict["IP"])
                        + " "
                        + "-p "
                        + str(nested_dict["port"])
                        + " "
                        + "-T "
                        + str(num_topics)
                        + " "
                        + "> tests/logs/"
                        + nested_dict["id"]
                        + ".txt 2>&1 &\n"
                    )
                    f.write(cmdline)

            # Add the subscriber nodes
            for i in range(self.num_mn_nodes):
                host = "h" + str(i + 1)
                host_list = self.sub_dict[host]
                for nested_dict in host_list:
                    # generate intested in topics in the range of 5 to 9 because
                    # our topic helper currently has 9 topics in it.
                    num_topics = random.randint(5, 9)

                    # build the command line
                    cmdline = (
                        host
                        + " export PYTHONPATH=$PWD &&"
                        + " python3 src/subscriber/subscriber.py "
                        + "-n "
                        + nested_dict["id"]
                        + " "
                        + "-T "
                        + str(num_topics)
                        + " "
                        + self.generate_run_number()
                        + " "
                        + "> tests/logs/"
                        + nested_dict["id"]
                        + ".txt 2>&1 &\n"
                    )
                    f.write(cmdline)

        f.close()

    def generate_run_number(self):
        return f"-R {self.num_disc}-{self.num_pub}-{self.num_sub}"

    def driver(self):
        self.logger.debug("ExperimentGenerator::driver")

        # Just dump the contents
        self.dump()

        # Now generate the entries for our discovery dht nodes
        self.populate_dict("disc", self.num_disc)

        # Now generate the entries for our publishers
        self.populate_dict("pub", self.num_pub)

        # Now generate the entries for our subscribers
        self.populate_dict("sub", self.num_sub)

        # Now generate the entries for our brokers
        self.populate_dict("broker", self.num_broker)

        self.dump()

        # Now generate experiment script
        self.gen_exp_script()


def parseCmdLineArgs():
    """Parse command line arguments."""

    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="HashCollisionTest")

    # Now specify all the optional arguments we support
    #
    parser.add_argument(
        "-b",
        "--bits_hash",
        type=int,
        choices=[8, 16, 24, 32, 40, 48, 56, 64],
        default=48,
        help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48",
    )

    parser.add_argument(
        "-D",
        "--num_disc_dht",
        type=int,
        default=20,
        help="Number of Discovery DHT instances, default 20",
    )

    parser.add_argument(
        "-P", "--num_pub", type=int, default=5, help="number of publishers, default 5"
    )

    parser.add_argument(
        "-S", "--num_sub", type=int, default=5, help="number of subscribers, default 5"
    )

    parser.add_argument("-B", "--num_broker", type=int, default=0, help="Enable broker")

    parser.add_argument(
        "-d",
        "--disc_base_port",
        type=int,
        default=5555,
        help="base port for discovery, default 5555",
    )

    parser.add_argument(
        "-p",
        "--pub_base_port",
        type=int,
        default=7777,
        help="base port for publishers, default 7777",
    )

    parser.add_argument(
        "--broker_base_port",
        type=int,
        default=8888,
        help="base port for broker, default 8888",
    )

    parser.add_argument(
        "-t",
        "--mn_topo",
        default="single,20",
        help="Mininet topology, default single,20 - other possibilities include linear,N or tree,fanout=N,depth=M",
    )

    parser.add_argument(
        "-f",
        "--script_file",
        default="mnexperiment.txt",
        help="Experiment file to be sourced from Mininet prompt, default mnexperiment.txt",
    )

    parser.add_argument(
        "-l",
        "--loglevel",
        type=int,
        default=logging.DEBUG,
        choices=[
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ],
        help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG",
    )

    return parser.parse_args()


def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("ExperimentGenerator")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        logger.debug(f"Main: effective log level is {logger.getEffectiveLevel()}")

        # Obtain the test object
        logger.debug("Main: obtain the ExperimentGenerator object")
        gen_obj = ExperimentGenerator(logger)

        # configure the object
        logger.debug("Main: configure the generator object")
        gen_obj.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the driver")
        gen_obj.driver()

    except Exception as e:
        logger.error(f"Exception caught in main - {e}")
        return


if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(message)s",
    )

    main()
