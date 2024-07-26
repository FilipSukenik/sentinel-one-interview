package com.scalyr.s3search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.s3simulation.SimulatedS3Client.FlakyNetworkException;
import com.scalyr.s3search.textsearch.TextSearcher;
import java.util.stream.IntStream;

/**
 * A naive single-threaded implementation of searching for a string in multiple S3 objects.
 * <p>
 * A partial list of the things it does not do:
 * <p>
 * - Measure execution time
 * - Handle network exceptions
 * - Use threads or tasks
 *
 * @copyright 2024 SentinelOne, Inc.
 */
public class Main {

  private static final double NETWORK_FAILURE_RATE = 0.0025;

  public static void main(String[] args) throws FlakyNetworkException {

    String searchTerm = args.length > 0 ? args[0] : "pewter";

    SimulatedS3Client s3Client = new SimulatedS3Client(NETWORK_FAILURE_RATE);

    int result = countMatchesInEpochs(0, 100, searchTerm, s3Client);

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
  public static int countMatchesInEpochs(int startEpoch, int endEpoch, String searchString,
    SimulatedS3Client s3Client) {

    TextSearcher searcher = new TextSearcher(searchString);

    return IntStream.rangeClosed(startEpoch, endEpoch).parallel().map(iteration ->
      executeStep(iteration, searchString, s3Client, searcher)
    ).sum();

    //    return Arrays.stream(results).sum();
  }

  private static int executeStep(int iteration, String searchString, SimulatedS3Client s3Client, TextSearcher searcher) {

    int retries = 0;
    do {
      try {
        retries++;
        byte[] epochData = s3Client.readFileFromS3("s3SimulationFiles", "epoch_" + iteration);
        return searcher.countMatchesInBlob(epochData, 0, epochData.length);
      } catch (FlakyNetworkException e) {
        System.out.println("Network failure on epoch " + iteration);
      }
    } while (retries < 3);
    return 0;
  }
}
