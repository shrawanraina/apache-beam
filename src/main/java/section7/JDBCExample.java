package section7;

import common.Constants;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

public class JDBCExample {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    PCollection<String> output =
        p.apply(
            JdbcIO.<String>read()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                            "com.mysql.cj.jdbc.Driver",
                            "jdbc:mysql://127.0.0.1:3306/apachebeam?useSSL=false")
                        .withUsername("root")
                        .withPassword("admin2024"))
                .withQuery("SELECT id, name, price from products WHERE name = ? ")
                .withStatementPreparator(
                    (JdbcIO.StatementPreparator)
                        preparedStatement -> preparedStatement.setString(1, "iPhone"))
                .withRowMapper(
                    (JdbcIO.RowMapper<String>)
                        resultSet ->
                            resultSet.getString(1)
                                + ","
                                + resultSet.getString(2)
                                + ","
                                + resultSet.getString(3)));
    output.apply(
        TextIO.write()
            .to(Constants.BASE_PATH + "section7/output")
            .withNumShards(1)
            .withSuffix(Constants.SUFFIX_CSV));
    p.run();
  }
}
