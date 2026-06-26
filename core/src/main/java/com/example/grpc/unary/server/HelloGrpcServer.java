package com.example.grpc.unary.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class HelloGrpcServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    Server grpcServer = ServerBuilder.forPort(8080).addService(new HelloGrpcServiceImpl()).build();
    grpcServer.start();
    grpcServer.awaitTermination();
  }
}
