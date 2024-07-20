package section4;

import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class GroupByKeyExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    // Step 1: Read data into PCollection
    PCollection<String> orderPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/orders.csv"));
    // Step 2: Convert List to KV<String, Integer>
    PCollection<KV<String, Integer>> orderKV = orderPColl.apply(ParDo.of(
        new DoFn<String, KV<String, Integer>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            String[] row = context.element().split(",");
            context.output(KV.of(row[0], Integer.valueOf(row[2])));
          }
        }));
    // Step 3: Apply GroupByKey and build KV<String, Iterable<Integer>>
    PCollection<KV<String, Iterable<Integer>>> orderKVIterable = orderKV.apply(
        GroupByKey.<String, Integer>create());
    // Step 4: Convert to String
    PCollection<String> output = orderKVIterable.apply(
        ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            String userId = context.element().getKey();
            Iterable<Integer> prices = context.element().getValue();
            Integer total = 0;
            for (Integer price : prices) {
              total += price;
            }
            context.output(userId + "," + total);
          }
        }));
    output.apply(
        TextIO.write().to(Constants.BASE_PATH + "section4/output-group-by-key").withNumShards(1)
            .withHeader("UserId,Total")
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }

}
