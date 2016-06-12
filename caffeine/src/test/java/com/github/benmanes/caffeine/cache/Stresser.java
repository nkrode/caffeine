/*
 * Copyright 2014 Ben Manes. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.github.benmanes.caffeine.testing.ConcurrentTestHarness;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A stress test to observe if the cache has a memory leak by not being able to drain the buffers
 * fast enough.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class Stresser {
  private static final String[] STATUS =
    { "Idle", "Required", "Processing -> Idle", "Processing -> Required" };
  private static final int THREADS = 2;// * Runtime.getRuntime().availableProcessors();
  private static final int WRITE_MAX_SIZE = (1 << 12);
  private static final int TOTAL_KEYS = (1 << 20);
  private static final int MASK = TOTAL_KEYS - 1;
  private static final int STATUS_INTERVAL = 5;

  private final BoundedLocalCache<Integer, Integer> local;
  private final LoadingCache<Integer, Integer> cache;
  private final Integer[] ints;

  private final int maximum;
  private final Stopwatch stopwatch;
  private final boolean reads = false;

  volatile int times;

  public Stresser() {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setPriority(Thread.MAX_PRIORITY)
        .setDaemon(true)
        .build();
    Executors.newSingleThreadScheduledExecutor(threadFactory)
        .scheduleAtFixedRate(this::status, STATUS_INTERVAL, STATUS_INTERVAL, SECONDS);
    maximum = reads ? TOTAL_KEYS : WRITE_MAX_SIZE;
    cache = Caffeine.newBuilder()
        .maximumSize(maximum)
        .executor(Executors.newWorkStealingPool())
        .recordStats()
//        .build(k -> k);
        .<Integer, Integer>buildAsync(k -> k)
        .synchronous();
    LocalAsyncLoadingCache.AsMapView view = (LocalAsyncLoadingCache.AsMapView) cache.asMap();
    local = (BoundedLocalCache<Integer, Integer>) view.delegate;
//    local = (BoundedLocalCache<Integer, Integer>) cache.asMap();
    ints = new Integer[TOTAL_KEYS];
    Arrays.setAll(ints, key -> {
      cache.put(key, key);
      return key;
    });
    cache.cleanUp();
    stopwatch = Stopwatch.createStarted();
    status();
  }

  public void run() throws InterruptedException {
    ConcurrentTestHarness.timeTasks(THREADS, () -> {
      int index = ThreadLocalRandom.current().nextInt();
      for (;;) {
        Integer key = ints[index++ & MASK];
        if (reads) {
          cache.getIfPresent(key);
        } else {
          cache.refresh(key);
          //cache.cleanUp();
        }
        if (times > 10) {
          return;
        }
      }
    });

    System.out.println("Waiting...");
    try {
      await().until(() -> {
        //local.evictionLock.acquireUninterruptibly();
        int pendingWrites = local.writeBuffer().size();
        //local.evictionLock.release();
        return pendingWrites == 0;
      });
    } catch (Throwable t) {
      status();
      t.printStackTrace();
    }
    System.out.println("Done!");
  }

  private void status() {
    local.evictionLock.acquireUninterruptibly();
    int pendingWrites = local.writeBuffer().size();
    int drainStatus = local.drainStatus();
    local.evictionLock.release();

    LocalTime elapsedTime = LocalTime.ofSecondOfDay(stopwatch.elapsed(TimeUnit.SECONDS));
    System.out.printf("---------- %s ----------%n", elapsedTime);
    System.out.printf("Pending reads: %,d; writes: %,d%n", local.readBuffer.size(), pendingWrites);
    System.out.printf("Drain status = %s (%s)%n", STATUS[drainStatus], drainStatus);
    System.out.printf("Evictions = %,d%n", cache.stats().evictionCount());
    System.out.printf("Size = %,d (max: %,d)%n", local.data.mappingCount(), maximum);
    System.out.printf("Lock = [%s%n", StringUtils.substringAfter(
        local.evictionLock.toString(), "["));
    System.out.printf("Pending tasks = %,d%n",
        ForkJoinPool.commonPool().getQueuedSubmissionCount());

    long maxMemory = Runtime.getRuntime().maxMemory();
    long freeMemory = Runtime.getRuntime().freeMemory();
    long allocatedMemory = Runtime.getRuntime().totalMemory();
    System.out.printf("Max Memory = %,d bytes%n", maxMemory);
    System.out.printf("Free Memory = %,d bytes%n", freeMemory);
    System.out.printf("Allocated Memory = %,d bytes%n", allocatedMemory);
    times++;

    System.out.println("Finished iteration #" + times);
    System.out.println();
  }

  public static void main(String[] args) throws Exception {
    new Stresser().run();
  }
}
