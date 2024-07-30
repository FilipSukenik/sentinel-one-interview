package com.scalyr.s3search.search.impl;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.search.SearchStrategy;
import com.scalyr.s3search.textsearch.TextSearcher;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class SemaphoreSearchStrategy extends SearchStrategy {

  public SemaphoreSearchStrategy(SimulatedS3Client s3Client, TextSearcher searcher) {

    super(s3Client, searcher);
  }

  @Override
  public int executeStrategy(int startEpoch, int endEpoch) {

    var amountOfWorkers = Runtime.getRuntime().availableProcessors();
    var semaphoreForExecution = new Semaphore(amountOfWorkers);
    var executorServiceForDownloading = Executors.newFixedThreadPool(amountOfWorkers);
    var executorServiceForProcessing = Executors.newFixedThreadPool(amountOfWorkers);
    var result = new AtomicInteger(0);
    var tasksFinished = new AtomicInteger(0);
    IntStream.range(startEpoch, endEpoch)
      .forEach(value ->
        {
          try {
            semaphoreForExecution.acquire();
            executorServiceForDownloading.submit(() -> {
              byte[] data = null;
              String fileName = "epoch_" + value;
              do {

                try {
                  data = s3Client.readFileFromS3("s3SimulationFiles", fileName);
                } catch (SimulatedS3Client.FlakyNetworkException e) {
                  System.err.println("Flaky exception was thrown. Retrying file: " + fileName);
                  continue;
                }
                byte[] finalData = data;
                executorServiceForProcessing.submit(() -> {
                  result.addAndGet(searcher.countMatchesInBlob(finalData, 0, finalData.length));
                  semaphoreForExecution.release();
                  tasksFinished.incrementAndGet();
                  if (endEpoch - startEpoch == tasksFinished.get()) {
                    executorServiceForProcessing.shutdown();
                  }
                });
              } while (data == null);
            });
          } catch (InterruptedException e) {
            System.err.println(
              "Execution thread was interrupted. Unable to calculate value for epoch: " + value + ", message: " + e.getMessage());
          }
        }
      );
    try {
      executorServiceForProcessing.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      System.err.println("Waiting for calculation result was interrupted. Returning partial calculation result");
    }
    return result.get();
  }
}
