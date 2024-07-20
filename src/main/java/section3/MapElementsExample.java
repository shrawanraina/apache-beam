package section3;

import common.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class MapElementsExample {

  public static void main(String[] args) {
    MapElementOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(MapElementOptions.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> pFruitList = p.apply(TextIO.read().from(options.getInputFilePath()));
    PCollection<String> output = pFruitList.apply(
        MapElements.into(
                TypeDescriptors.strings()) // Specifies that the output of the Map transformation will be a PCollection of integers.
            .via((String fruit) -> fruit.toUpperCase()));
    output.apply(
        TextIO.write().to(options.getOutputFilePath()).withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run().waitUntilFinish();
  }

}
