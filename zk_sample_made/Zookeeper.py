from kazoo.client import KazooClient

zk = KazooClient(hosts='localhost:2181')
zk.start()