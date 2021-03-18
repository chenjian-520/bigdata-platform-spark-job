package sparkJob.sparkCore.domain;

import scala.Serializable;

public class RuleJson implements Serializable {
    private String fileInPath;

    private String fileOutPath;

    private String separator;

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getFileOutPath() {
        return fileOutPath;
    }

    public void setFileOutPath(String fileOutPath) {
        this.fileOutPath = fileOutPath;
    }

    public String getFileInPath() {
        return fileInPath;
    }

    public void setFileInPath(String fileInPath) {
        this.fileInPath = fileInPath;
    }

    public RuleJson() {
    }
}
