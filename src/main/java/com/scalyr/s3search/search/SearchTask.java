package com.scalyr.s3search.search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.textsearch.TextSearcher;

public class SearchTask implements Runnable {

  private final SimulatedS3Client s3Client;

  private final TextSearcher searcher;

  private final int epochIndex;

  private int result;

  public SearchTask(SimulatedS3Client s3Client, TextSearcher searcher, int epochIndex) {

    this.s3Client = s3Client;
    this.searcher = searcher;
    this.epochIndex = epochIndex;
  }

  @Override
  public void run() {

    try {
      byte[] epochData = s3Client.readFileFromS3("s3SimulationFiles", "epoch_" + epochIndex);
      result = searcher.countMatchesInBlob(epochData, 0, epochData.length);
    } catch (Exception ignored) {
      // TODO please handle properly
      System.err.println("ignored exception was caught!");
    }
  }

  public int getResult() {

    return result;
  }
}
