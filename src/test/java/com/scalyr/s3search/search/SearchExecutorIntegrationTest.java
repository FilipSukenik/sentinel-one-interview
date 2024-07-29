package com.scalyr.s3search.search;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SearchExecutorIntegrationTest {

  @Test
  void integrationTestOfSearchExecutor() {

    var executor = new SearchExecutor();
    var result = executor.execute(new String[0], 0, 1);
    Assertions.assertEquals(16, result);
  }
}