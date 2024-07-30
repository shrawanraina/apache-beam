package section3;

import java.util.*;

import common.Constants;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class FlatMapExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> input =
        p.apply(
            Create.of(
                Arrays.asList(
                    "Hi, how are you?",
                    "I am doing well. How are you?",
                    "Fine",
                    "Where it the latest report for this quarter?")));
    PCollection<String> words =
        input.apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String s) -> Arrays.asList(s.split(" "))));
    words.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section3/output-flat-map-elements")
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run().waitUntilFinish();
  }
}
