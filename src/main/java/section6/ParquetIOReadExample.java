package section6;

import common.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.parquet.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class ParquetIOReadExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    Schema schema = BeamUserUtil.getSchema();
    PCollection<GenericRecord> input = p.apply(
        ParquetIO.read(schema).from(Constants.BASE_PATH + "section6/output.parquet"));
    input.apply(MapElements.via(new PrintElementFn()));
    p.run().waitUntilFinish();
  }
}
