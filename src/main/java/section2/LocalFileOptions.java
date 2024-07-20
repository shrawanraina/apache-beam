package section2;

import org.apache.beam.sdk.options.*;

public interface LocalFileOptions extends PipelineOptions {

  void setInputFilePath(String filePath);

  String getInputFilePath();

  void setOutputFilePath(String filePath);

  String getOutputFilePath();

  void setExtension(String ext);

  String getExtension();

}
