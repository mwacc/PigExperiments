package pig.experiments.pigprocessors;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.hadoop.pig.PigTemplate;

public abstract class AbstractPigExecutor implements Tasklet {

    private PigTemplate pigTemplate;
    private String sciptName;
    private String inputPath;
    private String outputPat;

    public abstract RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception;

    public PigTemplate getPigTemplate() {
        return pigTemplate;
    }

    public void setPigTemplate(PigTemplate pigTemplate) {
        this.pigTemplate = pigTemplate;
    }

    public String getSciptName() {
        return sciptName;
    }

    public void setSciptName(String sciptName) {
        this.sciptName = sciptName;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPat() {
        return outputPat;
    }

    public void setOutputPat(String outputPat) {
        this.outputPat = outputPat;
    }
}
