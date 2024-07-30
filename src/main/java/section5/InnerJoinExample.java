package section5;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.*;
import org.apache.beam.sdk.values.*;

public class InnerJoinExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    // Step 1: Read data and store as KV
    PCollection<KV<String, String>> userKV =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/users.csv"))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        String[] row = context.element().split(",");
                        context.output(KV.of(row[0], row[1] + "," + row[2] + "," + row[3]));
                      }
                    }));

    PCollection<KV<String, String>> orderKV =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/orders.csv"))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        String[] row = context.element().split(",");
                        context.output(KV.of(row[0], row[1] + "," + row[2]));
                      }
                    }));
    // Step 2: Create TupleTag object
    // ? Is TupleTag required
    final TupleTag<String> orderTuple = new TupleTag<>();
    final TupleTag<String> userTuple = new TupleTag<>();
    // Step 3: Combine data using CoGroupByKey
    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of(orderTuple, orderKV)
            .and(userTuple, userKV)
            .apply(CoGroupByKey.<String>create());
    // Step 4: Iterate over CoGbkResult and build String
    PCollection<String> output =
        result.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    String key = context.element().getKey();
                    CoGbkResult value = context.element().getValue();
                    Iterable<String> orderIterable = value.getAll(orderTuple);
                    Iterable<String> userIterable = value.getAll(userTuple);
                    //   TODO: Optimize nested for loop
                    for (String order : orderIterable) {
                      for (String user : userIterable) {
                        context.output(key + "," + order + "," + user);
                      }
                    }
                  }
                }));
    // Step 5: Store the group by result
    output.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section5/output-inner-join")
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run().waitUntilFinish();
  }
}
