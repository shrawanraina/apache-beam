package section4;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class CountExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> orderPColl =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/orders.csv"));
    PCollection<Long> count = orderPColl.apply(Count.globally());
    count.apply(
        ParDo.of(
            new DoFn<Long, Void>() {
              @ProcessElement
              public void processElement(@Element Long count) {
                System.out.println(count);
              }
            }));
    p.run();
  }
}
