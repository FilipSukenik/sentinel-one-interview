package com.scalyr.s3search.search.impl;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.search.SearchStrategy;
import com.scalyr.s3search.textsearch.TextSearcher;
import java.util.stream.IntStream;

public class EndlessResourcesSearchStrategy extends SearchStrategy {

  public EndlessResourcesSearchStrategy(SimulatedS3Client s3Client,
    TextSearcher searcher) {

    super(s3Client, searcher);
  }

  @Override
  public int executeStrategy(int startEpoch, int endEpoch) {

    return IntStream.range(startEpoch, endEpoch).parallel().map(value ->
    {
      byte[] data = null;
      do {
        try {
          data = s3Client.readFileFromS3("s3SimulationFiles", "epoch_" + value);
        } catch (SimulatedS3Client.FlakyNetworkException ignored) {
          // will retry forever
        }
      } while (data == null);
      return searcher.countMatchesInBlob(data, 0, data.length);
    }).sum();
  }
}
