package section3;

import java.util.*;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class SideInputExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    // Read the input file and store as PCollection
    PCollection<String> addressPColl =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/email.csv"));
    PCollection<KV<String, String>> addressMap =
        addressPColl.apply(
            ParDo.of(
                new DoFn<String, KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    String[] row = context.element().split(",");
                    context.output(KV.of(row[0], row[1]));
                  }
                }));
    // Create PCollectionView for side input
    PCollectionView<Map<String, String>> addressMapView = addressMap.apply(View.asMap());
    // Read user input and store as PCollection
    PCollection<String> userPColl =
        p.apply(TextIO.read().from(Constants.BASE_PATH + "data/users.csv"));
    PCollection<String> usersMissingAddress =
        userPColl.apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void process(ProcessContext context) {
                        // To access the side input in the process
                        Map<String, String> addressMap = context.sideInput(addressMapView);
                        String line = context.element();
                        String[] row = line.split(",");
                        if (!addressMap.containsKey(row[0])) {
                          context.output(line);
                        }
                      }
                    })
                .withSideInputs(addressMapView));
    usersMissingAddress.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section3/output-side-input")
            .withHeader(Constants.HEADER)
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
