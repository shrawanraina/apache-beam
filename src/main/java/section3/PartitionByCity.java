package section3;

import org.apache.beam.sdk.transforms.Partition.*;

class PartitionByCity implements PartitionFn<String> {

  @Override
  public int partitionFor(String elem,
      int numPartitions) {
    String[] row = elem.split(",");
    if ("Los Angeles".equals(row[5])) {
      return 0;
    } else if ("Phoenix".equals(row[5])) {
      return 1;
    } else if ("Austin".equals(row[5])) {
      return 2;
    }
    return -1;
  }
}
