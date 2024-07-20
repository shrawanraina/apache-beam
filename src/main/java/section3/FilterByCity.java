package section3;

import org.apache.beam.sdk.transforms.*;

class FilterByCity implements SerializableFunction<String, Boolean> {

  @Override
  public Boolean apply(String input) {
    return input.contains("Los Angeles");
  }
}
