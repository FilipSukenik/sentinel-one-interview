package com.scalyr.s3search.search;

import com.scalyr.s3search.s3simulation.SimulatedS3Client;
import com.scalyr.s3search.textsearch.TextSearcher;

public abstract class SearchStrategy {

  protected final SimulatedS3Client s3Client;

  protected final TextSearcher searcher;

  public SearchStrategy(SimulatedS3Client s3Client, TextSearcher searcher) {

    this.s3Client = s3Client;
    this.searcher = searcher;
  }

  public abstract int executeStrategy(int startEpoch, int endEpoch);
}
