package com.scalyr.s3search.search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.textsearch.TextSearcher;
import java.util.Arrays;

public class SearchExecutor {

  private static final double NETWORK_FAILURE_RATE = 0.0025;

  public void execute(String[] args) throws SimulatedS3Client.FlakyNetworkException {

    String searchTerm = args.length > 0 ? args[0] : "pewter";

    SimulatedS3Client s3Client = new SimulatedS3Client(NETWORK_FAILURE_RATE);

    int result = countMatchesInEpochs(0, 1, searchTerm, s3Client);

    System.out.format("%d matches found for '%s' and variants%n", result, searchTerm);
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

    int[] results = new int[endEpoch - startEpoch];
    for (int epochIndex = startEpoch; epochIndex < endEpoch; epochIndex++) {
      var task = new SearchTask(s3Client, searcher, epochIndex);
      task.run();
      results[epochIndex - startEpoch] = task.getResult();
    }

    return Arrays.stream(results).sum();
  }
}
