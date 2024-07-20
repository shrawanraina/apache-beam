package section4;

import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

// TODO: Implement
public class DistinctExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> fruitPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/fruits-with-duplicate.csv"));
    PCollection<String> distinctFruitPColl = fruitPColl.apply(Distinct.<String>create());
    distinctFruitPColl.apply(
        TextIO.write().to(Constants.BASE_PATH + "section4/output-distinct").withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
