import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Testspark {

    @Test
    public void chenjian() throws ParseException {
        String str = "31/Mar/2020:02:33:05 -0400";
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd");
        Date parse = sdf.parse(str);

        System.out.println(sdf2.format(parse));

    }
}
