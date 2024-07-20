package section3;

import org.apache.beam.sdk.transforms.*;

// TODO: Move to its own class
class User extends SimpleFunction<String, String> {

  @Override
  public String apply(String input) {
    String[] row = input.split(",");
    String userId = row[0];
    String zipCodeStr = row[1];
    String firstName = row[2];
    String lastName = row[3];
    String gender = row[4];
    String output = userId + "," + zipCodeStr + "," + firstName + "," + lastName + ",";
    if (gender.equals("1")) {
      output += "M";
    } else if (gender.equals("2")) {
      output += "F";
    }
    return output;
  }
}
