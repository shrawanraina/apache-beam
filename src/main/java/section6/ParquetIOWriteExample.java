package section6;

import common.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.extensions.avro.coders.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.parquet.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class ParquetIOWriteExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    Schema schema = BeamUserUtil.getSchema();
    // Step 1: read csv
    PCollection<String> userPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/users.csv"));
    // Step 2: Convert CSV input to Generic Record collection
    PCollection<GenericRecord> output = userPColl.apply(MapElements.via(new ConvertCsvToGeneric()))
        .setCoder(AvroCoder.of(GenericRecord.class, schema));
    // Step 3: Write records to parquet file
    output.apply(
        FileIO.<GenericRecord>write().via(ParquetIO.sink(schema))
            .to(Constants.BASE_PATH + "section6/parquet-example")
            .withNumShards(1).withSuffix(".parquet"));
    p.run().waitUntilFinish();
  }
}
