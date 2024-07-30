package section2;

import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.*;

public class LocalFileWithOptionsExample {

  public static void main(String[] args) {
    LocalFileOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(LocalFileOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> output = p.apply(TextIO.read().from(options.getInputFilePath()));
    output.apply(
        TextIO.write()
            .to(options.getOutputFilePath())
            .withNumShards(1)
            .withSuffix(options.getExtension()));
    p.run();
  }
}
