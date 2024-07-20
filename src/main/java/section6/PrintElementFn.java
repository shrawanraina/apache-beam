package section6;

import org.apache.avro.generic.*;
import org.apache.beam.sdk.transforms.*;

public class PrintElementFn extends SimpleFunction<GenericRecord, Void> {

  @Override
  public Void apply(GenericRecord input) {
    // TODO: Print the elements of the GenericRecord
    return null;
  }
}
