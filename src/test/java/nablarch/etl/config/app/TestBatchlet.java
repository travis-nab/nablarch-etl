package nablarch.etl.config.app;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.etl.config.ConfigIntegrationTest;
import nablarch.etl.config.DbToFileStepConfig;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.FileToDbStepConfig;
import nablarch.etl.config.RootConfig;

/**
 * {@link ConfigIntegrationTest}で使用する{@link javax.batch.api.Batchlet}。
 */
@Named
@Dependent
public class TestBatchlet extends AbstractBatchlet {

    @Inject
    private JobContext jobContext;

    @Inject
    private StepContext stepContext;

    @EtlConfig
    @Inject
    private RootConfig etlConfig;

    @Override
    public String process() {

        String jobId = jobContext.getJobName();
        String stepId = stepContext.getStepName();

        if ("step1".equals(stepId)) {

            FileToDbStepConfig config = etlConfig.getStepConfig(jobId, stepId);
            assertThat(config, is(notNullValue()));
            assertThat(config.getBean().getName(), is(TestDto.class.getName()));
            assertThat(config.getFile(), is(new File("base/input/test-input.csv")));
            assertThat(config.getSqlLoaderControlFileBasePath(), is(new File("base/control")));
            assertThat(config.getSqlLoaderOutputFileBasePath(), is(new File("base/log")));

        } else if ("step3".equals(stepId)) {

            DbToFileStepConfig config = etlConfig.getStepConfig(jobId, stepId);
            assertThat(config, is(notNullValue()));
            assertThat(config.getBean().getName(), is(TestDto3.class.getName()));
            assertThat(config.getSqlId(), is("SELECT_TEST3"));
            assertThat(config.getFile(), is(new File("base/output/test-output.csv")));

        } else {
            fail("not happen");
        }

        return "SUCCESS";
    }
}
