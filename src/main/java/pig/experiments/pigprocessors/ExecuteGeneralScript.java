package pig.experiments.pigprocessors;

import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.List;
import java.util.Properties;

public class ExecuteGeneralScript extends AbstractPigExecutor {

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Properties scriptParameters = new Properties();
        scriptParameters.put("input", super.getInputPath());
        scriptParameters.put("output", super.getOutputPat());
        scriptParameters.put("parallel", 1);

        boolean isFailed = false;
        System.out.println("\nRun Pig job................\n");
        List<ExecJob> jobs = super.getPigTemplate().executeScript(super.getSciptName(), scriptParameters);

        for(ExecJob job : jobs) {
            if( ExecJob.JOB_STATUS.FAILED == job.getStatus() ) {
                isFailed = true;
            }

        }

        System.out.println("Fished " + (isFailed ? "unsuccessful" : "successful"));


        return RepeatStatus.FINISHED;
    }
}