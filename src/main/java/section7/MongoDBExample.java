package section7;

import common.Constants;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MongoDBExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> input = p.apply(TextIO.read().from(Constants.BASE_PATH + "common/users.csv"));
        PCollection<Document> output = input.apply(ParDo.of(new DoFn<String, Document>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String[] row = Objects.requireNonNull(context.element()).split(",");
                Map<String, Object> userDoc = new HashMap<>();
                userDoc.put("userId", row[0]);
                userDoc.put("zipcode", row[1]);
                userDoc.put("firstName", row[2]);
                userDoc.put("lastName", row[3]);
                userDoc.put("gender", row[4]);
                userDoc.put("city", row[5]);
                context.output(new Document(userDoc));
            }
        }));
        output.apply(MongoDbIO.write().withUri("mongodb://localhost:27017").withDatabase("apachebeam").withCollection("users"));
        p.run();
    }
}
