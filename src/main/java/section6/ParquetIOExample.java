package section6;

import common.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

class BeamUserUtil {

  public static Schema getSchema() {
    String SCHEMA_STRING = "{\"namespace\":\"apachebeam.section6\",\n"
        + "\"type\":\"record\",\n"
        + "\"name\":\"ParquetExample\",\n"
        + "\"fields\":[\n"
        + "{\"name\":\"UserId\",\"type\":\"string\"},\n"
        + "{\"name\":\"Zipcode\",\"type\":\"string\"},\n"
        + "{\"name\":\"FirstName\",\"type\":\"string\"},\n"
        + "{\"name\":\"LastName\",\"type\":\"string\"},\n"
        + "{\"name\":\"Gender\",\"type\":\"string\"},\n"
        + "{\"name\":\"City\",\"type\":\"string\"}"
        + "]\n"
        + "}";
    Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
    return SCHEMA;
  }
}

class ConvertCsvToGeneric extends SimpleFunction<String, GenericRecord> {

  @Override
  public GenericRecord apply(String input) {
    String[] row = input.split(",");
    Schema schema = BeamUserUtil.getSchema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("UserId", row[0]);
    record.put("Zipcode", row[1]);
    record.put("FirstName", row[2]);
    record.put("LastName", row[3]);
    record.put("Gender", row[4]);
    record.put("City", row[5]);
    return record;
  }
}

public class ParquetIOExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    Schema schema = BeamUserUtil.getSchema();
    // Step 1: read csv
    PCollection<String> userPColl = p.apply(
        TextIO.read().from(Constants.BASE_PATH + "common/users.csv"));
    // Step 2: Convert CSV input to Generic Record collection
    // PCollection<GenericRecord> output = userPColl.apply(MapElements.via(new ConvertCsvToGeneric()))
    //     .setCoder(AvroCoder.of(GenericRecord.class, schema));
    // Step 3: Write records to parquet file
    // output.apply(
    //     FileIO.<GenericRecord>write().via(ParquetIO.sink(schema))
    //         .to(Constants.BASE_PATH + "section6/parquet-example")
    //         .withNumShards(1).withSuffix(".parquet"));
    p.run().waitUntilFinish();
  }
}
