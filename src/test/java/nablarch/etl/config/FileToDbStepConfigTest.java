package nablarch.etl.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.File;

import nablarch.etl.config.app.TestDto;

import org.junit.Before;
import org.junit.Test;

/**
 * {@link FileToDbStepConfig}のテスト。
 */
public class FileToDbStepConfigTest {

    // 正常な設定値
    private JobConfig jobConfig;
    private final String stepId = "step1";
    private final Class<?> bean = TestDto.class;
    private final String fileName = "input.csv";

    @Before
    public void setUp() {
        jobConfig = new JobConfig() {{
            setJobId("job1");
            setInputFileBasePath(new File("/job/input"));
            setOutputFileBasePath(new File("/job/output"));
            setSqlLoaderControlFileBasePath(new File("/job/control"));
            setSqlLoaderOutputFileBasePath(new File("/job/log"));
        }};
        jobConfig.initialize(
                new RootConfig() {{
                    setInputFileBasePath(new File("/base/input"));
                    setOutputFileBasePath(new File("/base/output"));
                    setSqlLoaderControlFileBasePath(new File("/base/control"));
                    setSqlLoaderOutputFileBasePath(new File("/base/log"));
                }});
    }

    /**
     * 正常に設定された場合、値が取得できること。
     */
    @Test
    public void testNormalSetting() {

        FileToDbStepConfig sut = new FileToDbStepConfig() {{
            setStepId(stepId);
            setBean(bean);
            setFileName(fileName);
        }};
        sut.initialize(jobConfig);

        assertThat(sut.getStepId(), is("step1"));
        assertThat(sut.getBean().getName(), is(TestDto.class.getName()));
        assertThat(sut.getFile(), is(notNullValue()));
        assertThat(sut.getFile(), is(new File("/job/input/input.csv")));
        assertThat(sut.getSqlLoaderControlFileBasePath(), is(new File("/job/control")));
    }
}
