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

import com.mongodb.async.SingleResultCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SingleResultFuture<T> implements Future<T>, SingleResultCallback<T> {
  final CountDownLatch countDownLatch = new CountDownLatch(1);
  T result;
  Throwable throwable;

  @Override
  public void onResult(T t, Throwable throwable) {
    this.result = t;
    this.throwable = throwable;
    this.countDownLatch.countDown();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return this.countDownLatch.getCount() == 0;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    this.countDownLatch.await();

    if (null != this.throwable) {
      throw new ExecutionException("Exception encountered", this.throwable);
    }

    return this.result;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    if (!this.countDownLatch.await(timeout, unit)) {
      throw new TimeoutException("Timeout while waiting for result");
    }

    if (null != this.throwable) {
      throw new ExecutionException("Exception encountered", this.throwable);
    }

    return this.result;
  }
}
