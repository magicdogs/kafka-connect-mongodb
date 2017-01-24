/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.mongodb;



import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class SingleResultFutureTest {

//  @Test(timeout = 10000)
//  public void getSuccess() throws ExecutionException, InterruptedException {
//    final SingleResultFuture<String> future = new SingleResultFuture<>();
//    final String EXPECTED_RESULT = "This is a test message";
//    assertFalse(future.isDone());
//    assertFalse(future.isCancelled());
//
//    Thread thread = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          Thread.sleep(100);
//          future.onResult(EXPECTED_RESULT, null);
//        } catch (InterruptedException e) {
//
//        }
//      }
//    });
//    thread.start();
//
//    String actualResult = future.get();
//    assertEquals(EXPECTED_RESULT, actualResult);
//    assertTrue(future.isDone());
//  }
//
//  @Test(timeout = 10000, expected = ExecutionException.class)
//  public void getFailure() throws ExecutionException, InterruptedException {
//    final SingleResultFuture<String> future = new SingleResultFuture<>();
//    assertFalse(future.isDone());
//    assertFalse(future.isCancelled());
//
//    Thread thread = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          Thread.sleep(100);
//          future.onResult(null, new IllegalStateException("This is busted"));
//        } catch (InterruptedException e) {
//
//        }
//      }
//    });
//    thread.start();
//    String actualResult = future.get();
//  }
}
