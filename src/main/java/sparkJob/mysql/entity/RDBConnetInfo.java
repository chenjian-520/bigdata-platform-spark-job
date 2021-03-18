package sparkJob.mysql.entity;

import java.util.Date;

public class RDBConnetInfo {
    private String id;
    private String code;
    private String type;
    private String dbUrl;
    private String dbUsername;
    private String dbPassword;
    private Boolean isActive;
    private Date editDate;
    private String editor;
    private String description;

    public RDBConnetInfo() {
    }

    public RDBConnetInfo(String id, String code, String type, String dbUrl, String dbUsername, String dbPassword, Boolean isActive, Date editDate, String editor, String description) {
        this.id = id;
        this.code = code;
        this.type = type;
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
        this.isActive = isActive;
        this.editDate = editDate;
        this.editor = editor;
        this.description = description;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return this.code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDbUrl() {
        return this.dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUsername() {
        return this.dbUsername;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public String getDbPassword() {
        return this.dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public Boolean getActive() {
        return this.isActive;
    }

    public void setActive(Boolean active) {
        this.isActive = active;
    }

    public Date getEditDate() {
        return this.editDate;
    }

    public void setEditDate(Date editDate) {
        this.editDate = editDate;
    }

    public String getEditor() {
        return this.editor;
    }

    public void setEditor(String editor) {
        this.editor = editor;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String toString() {
        return "RDBConnetInfo{id='" + this.id + '\'' + ", code='" + this.code + '\'' + ", type='" + this.type + '\'' + ", dbUrl='" + this.dbUrl + '\'' + ", dbUsername='" + this.dbUsername + '\'' + ", dbPassword='" + this.dbPassword + '\'' + ", isActive=" + this.isActive + ", editDate=" + this.editDate + ", editor='" + this.editor + '\'' + ", description='" + this.description + '\'' + '}';
    }
}
