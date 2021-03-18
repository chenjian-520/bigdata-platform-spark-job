package sparkJob.sparkCore.domain;

import scala.Serializable;
import java.util.List;
import java.util.Map;

public class UserGroupSparkParam implements Serializable {
    private String groupId;

    private String groupName;

    private Map<String, String> output;

    private Map<String, Map<String, String>> input;

    private Map<String, String> variables;

    private Map<String, List<Map<String, String>>> columns;

    private Map<String, List<Map<String, String>>> rules;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Map<String, String> getOutput() {
        return output;
    }

    public void setOutput(Map<String, String> output) {
        this.output = output;
    }

    public Map<String, Map<String, String>> getInput() {
        return input;
    }

    public void setInput(Map<String, Map<String, String>> input) {
        this.input = input;
    }

    public Map<String, String> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, String> variables) {
        this.variables = variables;
    }

    public Map<String, List<Map<String, String>>> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, List<Map<String, String>>> columns) {
        this.columns = columns;
    }

    public Map<String, List<Map<String, String>>> getRules() {
        return rules;
    }

    public void setRules(Map<String, List<Map<String, String>>> rules) {
        this.rules = rules;
    }


}