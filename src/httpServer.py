import sys


from thrift.protocol import TJSONProtocol
from thrift.server import THttpServer

class HelloSvcHandler:

  def hello_func(self):
    print("Hello Called")
    return "hello from Python"

def main():
    processor = HelloSvc.Processor(HelloSvcHandler())
    protoFactory = TJSONProtocol.TJSONProtocolFactory()
    port = 9090
    server = THttpServer.THttpServer(processor, ("localhost", port), protoFactory)
    print("Python server running on port " + str(port))
    server.serve()

if __name__=="__main__":
    main()