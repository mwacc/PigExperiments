package pig.experiments;

import org.springframework.batch.core.launch.support.CommandLineJobRunner;

import java.net.URL;
import java.util.Enumeration;

public class RunApp {

    public static void main(String[] argv) throws Exception {
        if( argv.length == 1 ) {
            CommandLineJobRunner.main(new String[]{"spring-context.xml", "pigJob", argv[0]});
        } else {
            CommandLineJobRunner.main(new String[]{"spring-context.xml", "pigJob", String.format("exec.args='%d'",System.currentTimeMillis())});
        }
    }

}