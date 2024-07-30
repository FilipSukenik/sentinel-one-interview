package com.scalyr.s3search.search;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SearchExecutorIntegrationTest {

  @Test
  void integrationTestOfSearchExecutor() {
    var executor = new SearchExecutor();
    var result = executor.execute(new String[0], 0, 100);
    Assertions.assertEquals(1874, result);
  }
}