package com.example.grpc.unary.client;

import com.example.grpc.HelloGrpc;
import com.example.grpc.HelloRequest;
import com.example.grpc.HelloResponse;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HelloGrpcClientCaller {
  private final HelloGrpc.HelloBlockingStub blockingStub;
  private final HelloGrpc.HelloStub asyncStub;
  private final List<String> names = Arrays.asList("herojoon", "example-user", "grpc-fan");

  public HelloGrpcClientCaller(ManagedChannel channel) {
    blockingStub = HelloGrpc.newBlockingStub(channel);
    asyncStub = HelloGrpc.newStub(channel);
  }

  // 1. Unary RPC
  public void sendUnaryBlocking() {
    System.out.println(">>> Send Call (Unary)");
    HelloResponse response =
        blockingStub.sayHello(
            HelloRequest.newBuilder()
                .setName("herojoon")
                .setAge(10)
                .setMessage("Hello, Glad to meet you.")
                .build());
    System.out.println(">>> Response Data => [%s]".formatted(response));
  }

  // 2. Server Streaming RPC
  public void sendServerStreamingBlocking() {
    System.out.println(">>> Send Call (Server Streaming)");
    Iterator<HelloResponse> helloResponseIterator =
        blockingStub.lotsOfReplies(
            HelloRequest.newBuilder()
                .setName("herojoon")
                .setAge(10)
                .setMessage("Hello, Glad to meet you.")
                .build());

    helloResponseIterator.forEachRemaining(
        helloResponse -> {
          System.out.println(">>> Response Data => [%s]".formatted(helloResponse));
        });
  }

  // 3. Client Streaming RPC
  public void sendClientStreamingAsync() throws InterruptedException {
    System.out.println(">>> Send Call (Client Streaming)");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<HelloResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(HelloResponse response) {
            System.out.println(">>> Response: %s".formatted(response));
          }

          @Override
          public void onError(Throwable t) {
              System.out.println("Error in sendClientStreamingAsync" + t.getMessage());
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            System.out.println(">>> Completed sendClientStreamingAsync");
            finishLatch.countDown();
          }
        };

    StreamObserver<HelloRequest> requestObserver = asyncStub.lotsOfGreetings(responseObserver);
    for (String name : names) {
      requestObserver.onNext(HelloRequest.newBuilder().setName(name).build());
      Thread.sleep(500);
    }
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES);
  }

  // 4. Bidirectional Streaming RPC
  public void sendBidirectionalStreamingAsync() throws InterruptedException {
    System.out.println(">>> Send Call (Bidi Streaming)");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<HelloResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(HelloResponse response) {
            System.out.println(">>> Response: %s".formatted(response));
          }

          @Override
          public void onError(Throwable t) {
            System.out.println("Error in sendBidirectionalStreamingAsync: " + t.getMessage());
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            System.out.println(">>> Completed sendBidirectionalStreamingAsync");
            finishLatch.countDown();
          }
        };

    StreamObserver<HelloRequest> requestObserver = asyncStub.bidiHello(responseObserver);
    for (String name : names) {
      requestObserver.onNext(HelloRequest.newBuilder().setName(name).build());
      Thread.sleep(500);
    }
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES);
  }
}
