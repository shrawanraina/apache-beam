package section2;

import common.*;
import java.util.*;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class InMemoryExample {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<Customer> custPColl = p.apply(Create.of(getCustomers()));
    PCollection<String> pStrList = custPColl.apply(
        MapElements.into(TypeDescriptors.strings())
            .via((Customer customer) -> customer.getName()));

    pStrList.apply(TextIO.write().to(
            Constants.BASE_PATH + "section2/customers")
        .withNumShards(1).withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }

  static List<Customer> getCustomers() {
    Customer firstCustomer = new Customer("1001", "John");
    Customer secondCustomer = new Customer("1002", "Mary");
    List<Customer> customers = new ArrayList<>();
    customers.add(firstCustomer);
    customers.add(secondCustomer);
    return customers;
  }

}
