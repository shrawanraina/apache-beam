package section3;

import org.apache.beam.sdk.options.*;

public interface MapElementOptions extends PipelineOptions {

  void setInputFilePath(String filePath);

  String getInputFilePath();

  void setOutputFilePath(String filePath);

  String getOutputFilePath();

}
