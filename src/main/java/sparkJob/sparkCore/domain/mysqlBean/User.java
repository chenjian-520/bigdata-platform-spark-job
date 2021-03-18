package sparkJob.sparkCore.domain.mysqlBean;

import lombok.Data;
import scala.Serializable;

@Data
public class User implements Serializable {

    private String name;

    private String sex;

    private String age;

    @Override
    public String toString() {
        return "user{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                '}';
    }
}
