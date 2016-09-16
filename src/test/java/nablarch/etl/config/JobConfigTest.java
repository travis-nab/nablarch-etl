package nablarch.etl.config;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Test;

/**
 * {@link JobConfig}のテスト。
 */
public class JobConfigTest {

    // 正常な設定値
    private final String jobId = "job1";
    private final File inputFileBasePath = new File("/job/input");
    private final File  outputFileBasePath = new File("/job/output");
    private final File controlFileBasePath = new File("/job/control");
    private final File logFileBasePath = new File("/job/log");

    private RootConfig config = new RootConfig() {{
        setInputFileBasePath(new File("/base/input"));
        setOutputFileBasePath(new File("/base/output"));
        setSqlLoaderControlFileBasePath(new File("/base/control"));
        setSqlLoaderOutputFileBasePath(new File("/base/log"));
    }};

    /**
     * {@link JobConfig}でファイルベースパスが指定されない場合、
     * {@link RootConfig}のファイルベースパスが使われること。
     */
    @Test
    public void testDefaultFileBasePath() {

        JobConfig sut = new JobConfig() {{
            setJobId(jobId);
        }};
        sut.initialize(config);

        assertThat(sut.getInputFileBasePath(), is(new File("/base/input")));
        assertThat(sut.getOutputFileBasePath(), is(new File("/base/output")));
        assertThat(sut.getSqlLoaderControlFileBasePath(), is(new File("/base/control")));
        assertThat(sut.getSqlLoaderOutputFileBasePath(), is(new File("/base/log")));
    }

    /**
     * {@link JobConfig}でファイルベースパスが指定された場合、
     * {@link RootConfig}のファイルベースパスが使われないこと。
     */
    @Test
    public void testOverrideFileBasePath() {

        JobConfig sut = new JobConfig() {{
            setJobId(jobId);
            setInputFileBasePath(inputFileBasePath);
            setOutputFileBasePath(outputFileBasePath);
            setSqlLoaderControlFileBasePath(controlFileBasePath);
            setSqlLoaderOutputFileBasePath(logFileBasePath);
        }};
        sut.initialize(config);

        assertThat(sut.getInputFileBasePath(), is(new File("/job/input")));
        assertThat(sut.getOutputFileBasePath(), is(new File("/job/output")));
        assertThat(sut.getSqlLoaderControlFileBasePath(), is(new File("/job/control")));
        assertThat(sut.getSqlLoaderOutputFileBasePath(), is(new File("/job/log")));
    }
}
