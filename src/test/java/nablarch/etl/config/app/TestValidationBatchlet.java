package nablarch.etl.config.app;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.RootConfig;
import nablarch.etl.config.ValidationStepConfig;

@Named
@Dependent
public class TestValidationBatchlet extends AbstractBatchlet {

    @Inject
    private JobContext jobContext;

    @Inject
    private StepContext stepContext;

    @EtlConfig
    @Inject
    private RootConfig etlConfig;

    @Override
    public String process() throws Exception {
        ValidationStepConfig config = etlConfig.getStepConfig(
                jobContext.getJobName(), stepContext.getStepName());

        assertThat(config, is(notNullValue()));
        assertThat(config.getBean()
                .getName(), is(TestDto.class.getName()));
        assertThat(config.getErrorEntity()
                .getName(), is(TestDtoErrorEntity.class.getName()));
        assertThat(config.getMode(), is(ValidationStepConfig.Mode.ABORT));

        assertThat(config.getErrorLimit(), is(100));

        return "SUCCESS";
    }
}
