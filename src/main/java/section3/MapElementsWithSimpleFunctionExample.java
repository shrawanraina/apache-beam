package section3;

import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


public class MapElementsWithSimpleFunctionExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> userPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/users.csv"));
    PCollection<String> output = userPColl.apply(MapElements.via(new User()));
    output.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section3/output-map-elements")
            .withNumShards(1).withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
