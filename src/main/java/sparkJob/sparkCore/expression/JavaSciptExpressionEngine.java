package sparkJob.sparkCore.expression;

import scala.Serializable;
import javax.script.*;
import java.util.Map;

public class JavaSciptExpressionEngine implements Serializable {
    /** 单例
    * */
    private static JavaSciptExpressionEngine single = new JavaSciptExpressionEngine();

    /**表达式引擎
     * */
    private static ScriptEngine engine = null;

    /**
     * 私有化构造器
     */
    private JavaSciptExpressionEngine(){

    }

    /**
     *获取引擎
     */
    public static JavaSciptExpressionEngine getEngine(){
        return single;
    }

  /**
   *计算表达式
   */
  public boolean calucate(String script, Map<String,Object> variables) throws ScriptException{
      ScriptContext scriptContext=new SimpleScriptContext();
      ScriptEngineManager factory = new ScriptEngineManager();
      engine =factory.getEngineByName("JavaScript");
      variables.entrySet().forEach(variable ->{
          scriptContext.setAttribute(variable.getKey(),variable.getValue(),ScriptContext.ENGINE_SCOPE);
      });
      return (Boolean)engine.eval(script,scriptContext);
  }
}
