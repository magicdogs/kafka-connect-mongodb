package io.confluent.kafka.connect.mongodb;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class SingleResultFutureTest {

  @Test(timeout = 10000)
  public void getSuccess() throws ExecutionException, InterruptedException {
    final SingleResultFuture<String> future = new SingleResultFuture<>();
    final String EXPECTED_RESULT = "This is a test message";
    assertFalse(future.isDone());
    assertFalse(future.isCancelled());

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
          future.onResult(EXPECTED_RESULT, null);
        } catch (InterruptedException e) {

        }
      }
    });
    thread.start();

    String actualResult = future.get();
    assertEquals(EXPECTED_RESULT, actualResult);
    assertTrue(future.isDone());
  }

  @Test(timeout = 10000, expected = ExecutionException.class)
  public void getFailure() throws ExecutionException, InterruptedException {
    final SingleResultFuture<String> future = new SingleResultFuture<>();
    assertFalse(future.isDone());
    assertFalse(future.isCancelled());

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
          future.onResult(null, new IllegalStateException("This is busted"));
        } catch (InterruptedException e) {

        }
      }
    });
    thread.start();
    String actualResult = future.get();
  }
}
