import os
import logging
import grpc
import grpc_excel2csv_pb2
import grpc_excel2csv_pb2_grpc

from concurrent import futures

def get_filepath(filename, extension, dir=os.path.dirname(__file__)):
  return os.path.abspath(f'{dir}/../test/server/{filename}{extension}')

class Excel2Csv(grpc_excel2csv_pb2_grpc.Excel2CsvServicer):
  def SayHello(self, request, context):
    return grpc_excel2csv_pb2.HelloRequest()

  def UploadFile(self, request_iterator, context):
    data = bytearray()

    for request in request_iterator:
      if request.metadata.filename and request.metadata.extension:
        filepath = get_filepath(request.metadata.filename, request.metadata.extension)
        continue
      data.extend(request.chunk_data)
    with open(filepath, 'wb') as f:
      f.write(data)
    return grpc_excel2csv_pb2.StringResponse(message='Success!')

  def DownloadFile(self, request, context):
    chunk_size = 1024

    filepath = get_filepath(request.filename, request.extension)
    if os.path.exists(filepath):
      with open(filepath, mode="rb") as f:
        while True:
          chunk = f.read(chunk_size)
          if chunk:
            entry_response = grpc_excel2csv_pb2.FileResponse(chunk_data=chunk)
            yield entry_response
          else:  # The chunk was empty, which means we're at the end of the file
            return

def serve():
  port = "50051"
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  grpc_excel2csv_pb2_grpc.add_Excel2CsvServicer_to_server(Excel2Csv(), server)
  server.add_insecure_port("[::]:" + port)
  server.start()
  print("Server started, listening on " + port)
  server.wait_for_termination()

if __name__ == "__main__":
  logging.basicConfig()
  print(f'cwd = {get_filepath("", "")}')
  serve()
