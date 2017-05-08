import sys

sys.path.append('gen-py')

from recommenderservice import Recommender
from recommenderservice.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol




def main():
    # Make socket
    transport = TSocket.TSocket('localhost', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Recommender.Client(protocol)

    # Connect!
    transport.open()

    client.ping()
    print('ping()')

    req = ItemRequest()
    req.item_id = '1'
    req.cityName = 'shanghai'
    req.category = 'ershouche'
    rec_ = client.fetchRecByItem(req)
    print(rec_)

if __name__ == "__main__":
    main()