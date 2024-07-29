package com.scalyr.s3search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client.FlakyNetworkException;
import com.scalyr.s3search.search.SearchExecutor;

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

  public static void main(String[] args) throws FlakyNetworkException {

    new SearchExecutor().execute(args, 0, 100);
  }
}
