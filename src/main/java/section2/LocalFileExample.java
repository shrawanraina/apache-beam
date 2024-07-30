package section2;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.values.*;

public class LocalFileExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> output =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/users.csv"));
    output.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section2/output")
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
