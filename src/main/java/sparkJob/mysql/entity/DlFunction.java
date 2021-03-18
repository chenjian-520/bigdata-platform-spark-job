package sparkJob.mysql.entity;

import java.io.Serializable;
import java.util.function.Function;

public interface DlFunction<T, R> extends Function<T, R>, Serializable {
}
