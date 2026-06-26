package com.example.grpc.unary.server;

import com.example.grpc.HelloGrpc;
import com.example.grpc.HelloRequest;
import com.example.grpc.HelloResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;

public class HelloGrpcServiceImpl extends HelloGrpc.HelloImplBase {

  // 1. Unary RPC
  @Override
  public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
    System.out.println("=== Get Request: [%s]".formatted(request));

    HelloResponse response =
        HelloResponse.newBuilder()
            .setGreetingMessage("Hello, %s".formatted(request.getName()))
            .setQuestionMessage("What do you do for fun?")
            .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  // 2. Server Streaming RPC
  @Override
  public void lotsOfReplies(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
    System.out.println("=== Get Request (Server Streaming): [%s]".formatted(request));
    String[] questions = {
      "What do you do for fun?", "What kind of books do you like?", "What is your favorite color?"
    };

    for (String question : questions) {
      HelloResponse response =
          HelloResponse.newBuilder()
              .setGreetingMessage("Hello, %s".formatted(request.getName()))
              .setQuestionMessage(question)
              .build();
      responseObserver.onNext(response);
    }
    responseObserver.onCompleted();
  }

  // 3. Client Streaming RPC
  @Override
  public StreamObserver<HelloRequest> lotsOfGreetings(
      StreamObserver<HelloResponse> responseObserver) {
    return new StreamObserver<>() {
      List<String> names = new ArrayList<>();

      @Override
      public void onNext(HelloRequest request) {
        System.out.println("=== Get Request (Client Streaming): " + request.getName());
        names.add(request.getName());
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(
            HelloResponse.newBuilder()
                .setGreetingMessage("Hello, [%s]".formatted(String.join(",", names)))
                .build());
        responseObserver.onCompleted();
      }

      @Override
      public void onError(Throwable t) {
        log.error("Error in lotsOfGreetings", t);
      }
    };
  }

  // 4. Bidirectional Streaming RPC
  @Override
  public StreamObserver<HelloRequest> bidiHello(StreamObserver<HelloResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(HelloRequest request) {
        System.out.println("=== Get Request (Bidi Streaming): " + request.getName());
        responseObserver.onNext(
            HelloResponse.newBuilder()
                .setGreetingMessage("Hello, %s".formatted(request.getName()))
                .setQuestionMessage("What do you do for fun?")
                .build());
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }

      @Override
      public void onError(Throwable t) {
        log.error("Error in bidiHello", t);
      }
    };
  }
}
