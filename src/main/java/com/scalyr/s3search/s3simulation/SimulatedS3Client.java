package com.scalyr.s3search.s3simulation;

import com.scalyr.s3search.utilities.FastRandom;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Implements a simulated version of Amazon S3.
 *
 * @copyright 2024 SentinelOne, Inc.
 */
public class SimulatedS3Client {
  /**
   * Root of the local filesystem tree containing simulated S3 objects.
   */
  private final File rootDirectory;

  /**
   * Simulation of the network through which we read from S3.
   */
  private final NetworkSimulator networkSimulator;

  /**
   * Fraction of `readFileFromS3` calls that should throw `FlakyNetworkException`.
   */
  private final double exceptionRate;


  /** Models a transient network error. */
  public static final class FlakyNetworkException extends IOException {
    public FlakyNetworkException(String msg) {
      super(msg);
    }
  }



  /**
   * Random number generator used to generate simulated queue delays. We use our FastRandom class
   * because it is threadsafe.
   */
  private final FastRandom rng = new FastRandom();


  /*
   * Standard constructor.
   *
   * @param exceptionRate How frequently a download should fail w/ exception
   */
  public SimulatedS3Client(double exceptionRate) {
    this(new File("."), new NetworkSimulator(), exceptionRate);
  }

  /**
   * Construct a SimulatedS3Client to read files from the specified local disk directory, with a default
   * exception rate. We expect files to appear at ROOTDIRECTORY/bucketName/objectName.
   *
   * @param rootDirectory Root of the local filesystem tree containing simulated S3 objects. Each
   *     bucketName corresponds to a subdirectory of this directory.
   * @param networkSimulator dependency
   * @param exceptionRate How frequently a download should fail w/ exception
   */
  SimulatedS3Client(File rootDirectory, NetworkSimulator networkSimulator, double exceptionRate) {
    this.rootDirectory    = rootDirectory;
    this.networkSimulator = networkSimulator;
    this.exceptionRate    = exceptionRate;
  }

  /**
   * Return the contents of a specified S3 object.
   *
   * This method will take some time to return, reflecting simulated delays for disk and network access.
   */
  public byte[] readFileFromS3(String bucketName, String objectName) throws FlakyNetworkException {
    if (rng.nextDouble() < exceptionRate)
      throw new FlakyNetworkException("transient network error, please retry");

    byte[] result;
    try {
      Thread.sleep(simulatedDiskReadTime()); // simulate S3 internal read performance
      result = Files.readAllBytes(new File(rootDirectory, bucketName + "/" + objectName).toPath());
    } catch (IOException|InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    networkSimulator.waitForTraffic(result.length); // simulate network contention
    return result;
  }

  /**
   * Generate a simulated delay to read data from disk. This is intended to model one component of
   * S3 read time -- the actual disk seek, plus any queuing delays. Network transfer time is
   * modeled separately. We assume that disk transfer time is buried in the network transfer time.
   *
   * @return Simulated read delay, in milliseconds.
   */
  private int simulatedDiskReadTime() {
    // Based on the following real-world measurements for reading 256KB of data from S3 in a single
    // thread on a fast instance:
    //
    // Minimum time:         13 ms
    // 10th percentile:      38 ms
    // 50th percentile:      58 ms
    // 90th percentile:      78 ms
    // 99th percentile:     216 ms
    // 99.9th percentile:   527 ms
    // Maximum time:       3737 ms

    int percentile = rng.nextInt(1000);
    if (percentile < 100) {
      // Below 10th percentile.
      return randomValueInRange(13, 38);
    } else if (percentile < 500) {
      // Between 10th and 50th percentile.
      return randomValueInRange(38, 58);
    } else if (percentile < 900) {
      // Between 50th and 90th percentile.
      return randomValueInRange(58, 78);
    } else if (percentile < 990) {
      // Between 90th and 99th percentile.
      return randomValueInRange(78, 216);
    } else if (percentile < 999) {
      // Between 99th and 99.9th percentile.
      return randomValueInRange(216, 527);
    } else {
      // Above the 99.9th percentile.
      return randomValueInRange(527, 3737);
    }
  }

  /**
   * Return a random number in the range [low, high).
   */
  private int randomValueInRange(int low, int high) {
    return low + rng.nextInt(high - low);
  }
}
