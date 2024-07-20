package section3;

import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class FlattenExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> losAngelesPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/input-los-angeles.csv"));
    PCollection<String> phoenixPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/input-phoenix.csv"));
    PCollection<String> austinPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/input-austin.csv"));
    PCollectionList<String> combinedPColl = PCollectionList.of(losAngelesPColl).and(phoenixPColl)
        .and(austinPColl);
    PCollection<String> output = combinedPColl.apply(Flatten.pCollections());
    output.apply(TextIO.write().to(Constants.BASE_PATH + "section3/output-flatten")
        .withHeader(Constants.HEADER).withNumShards(1)
        .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }

}
