package section3;

import org.apache.beam.sdk.transforms.*;

class CityFilter extends DoFn<String, String> {

  @ProcessElement
  public void processElement(ProcessContext context) {
    String line = context.element();
    String[] row = line.split(",");
    if ("Los Angeles".equals(row[5])) {
      context.output(line);
    }
  }
}
