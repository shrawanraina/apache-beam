package section4;

import org.apache.beam.sdk.transforms.*;

public class PrintElementFn extends DoFn<Integer, Void> {

  @ProcessElement
  public void processElement(@Element Integer input) {
    System.out.println(input);
  }
}
