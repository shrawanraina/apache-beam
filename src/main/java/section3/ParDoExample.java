package section3;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class ParDoExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> userPColl =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/users.csv"));
    PCollection<String> output = userPColl.apply(ParDo.of(new CityFilter()));
    output.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section3/output-par-do")
            .withHeader(Constants.HEADER)
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
