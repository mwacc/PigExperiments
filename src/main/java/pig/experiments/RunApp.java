package pig.experiments;

import org.springframework.batch.core.launch.support.CommandLineJobRunner;
import pig.experiments.utils.FixHadoopOnWindows;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

public class RunApp {

    private final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH");

    public static void main(String[] argv) throws Exception {

        FixHadoopOnWindows.runFix();

        String dateTimePattern = formatter.format(new Date());
        dateTimePattern = "2013/11/08/06";

        if( argv.length == 1 ) {
            CommandLineJobRunner.main(new String[]{"spring-context.xml", "pigJob", String.format("%s=%s", JobConstants.WORKING_HOUR, dateTimePattern ), argv[0]});
        } else {
            CommandLineJobRunner.main(new String[]{"spring-context.xml", "pigJob", String.format("%s=%s", JobConstants.WORKING_HOUR, dateTimePattern )});
        }
    }

}