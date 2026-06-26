package com.example.grpc.unary.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class HelloGrpcClient {
  public static void main(String[] args) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();

    HelloGrpcClientCaller caller = new HelloGrpcClientCaller(channel);

    try {
      System.out.println("--- Starting Unary Example ---");
      caller.sendUnaryBlocking();

      System.out.println("--- Starting Server Streaming Example ---");
      caller.sendServerStreamingBlocking();

      System.out.println("--- Starting Client Streaming Example ---");
      caller.sendClientStreamingAsync();

      System.out.println("--- Starting Bidirectional Streaming Example ---");
      caller.sendBidirectionalStreamingAsync();

    } catch (InterruptedException e) {
      System.out.println("Client interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    } finally {
      channel.shutdown();
    }
  }
}
