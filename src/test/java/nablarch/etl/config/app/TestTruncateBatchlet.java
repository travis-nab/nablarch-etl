package nablarch.etl.config.app;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.RootConfig;
import nablarch.etl.config.TruncateStepConfig;

@Named
@Dependent
public class TestTruncateBatchlet extends AbstractBatchlet {

    @Inject
    private JobContext jobContext;

    @Inject
    private StepContext stepContext;

    @EtlConfig
    @Inject
    private RootConfig etlConfig;

    @Override
    public String process() throws Exception {

        String jobId = jobContext.getJobName();
        String stepId = stepContext.getStepName();

        final TruncateStepConfig config = etlConfig.getStepConfig(jobId, stepId);
        final List<Class<?>> entities = config.getEntities();

        assertThat(entities.size(), is(2));

        assertThat(entities.get(0)
                .getName(), is("nablarch.etl.config.app.TestDto"));
        assertThat(entities.get(1)
                .getName(), is("nablarch.etl.config.app.TestDto3"));
        return "SUCCESS";
    }
}
