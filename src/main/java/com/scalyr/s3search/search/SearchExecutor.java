package com.scalyr.s3search.search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.search.impl.SemaphoreSearchStrategy;
import com.scalyr.s3search.textsearch.TextSearcher;

public class SearchExecutor {

  private static final double NETWORK_FAILURE_RATE = 0.025;

  public int execute(String[] args, int startEpoch, int endEpoch) {

    String searchTerm = args.length > 0 ? args[0] : "pewter";

    SimulatedS3Client s3Client = new SimulatedS3Client(NETWORK_FAILURE_RATE);

    int result = countMatchesInEpochs(startEpoch, endEpoch, searchTerm, s3Client);

    System.out.format("%d matches found for '%s' and variants%n", result, searchTerm);
    return result;
  }

  /**
   * Return the number of matches for searchString in each of a series of "epochs" stored in Amazon S3.
   *
   * @param startEpoch   Index of the first epoch to search (inclusive).
   * @param endEpoch     Index just after the last epoch to search (exclusive).
   * @param searchString String to search for.
   * @param s3Client     Accessor used to read the files to search.
   * @return The count of matches across all epochs.
   */
  public int countMatchesInEpochs(int startEpoch, int endEpoch, String searchString,
    SimulatedS3Client s3Client) {

    TextSearcher searcher = new TextSearcher(searchString);
    long startTime = System.currentTimeMillis();

    var searchStrategy = new SemaphoreSearchStrategy(s3Client, searcher);
    var result = searchStrategy.executeStrategy(startEpoch, endEpoch);

    System.out.format("Execution time is %d%n", Math.abs(startTime - System.currentTimeMillis()));
    return result;
  }
}
