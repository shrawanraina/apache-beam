package section3;

// TODO: Implement


import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class PartitionExample {

  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> userPColl = pipeline.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/users.csv"));
    PCollectionList<String> partitionList = userPColl.apply(Partition.of(3, new PartitionByCity()));
    PCollection<String> losAngelesList = partitionList.get(0);
    PCollection<String> phoenixList = partitionList.get(1);
    PCollection<String> austinList = partitionList.get(2);
    losAngelesList.apply(
        TextIO.write().to(Constants.BASE_PATH + "section3/output-los-angeles").withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    phoenixList.apply(
        TextIO.write().to(Constants.BASE_PATH + "section3/output-phoenix").withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    austinList.apply(
        TextIO.write().to(Constants.BASE_PATH + "section3/output-austin").withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    pipeline.run();
  }
}
