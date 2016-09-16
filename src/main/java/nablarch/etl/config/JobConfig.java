package nablarch.etl.config;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import nablarch.core.util.annotation.Published;

/**
 * ジョブの設定を保持するクラス。
 * <p>
 * ファイルのベースパスに関する設定項目は、ジョブの設定にて指定がない場合、
 * ETLの設定({@link RootConfig})を使用する。
 * 
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public class JobConfig {

    /** ジョブID */
    private String jobId;

    /** 入力ファイルのベースパス */
    private File inputFileBasePath;

    /** 出力ファイルのベースパス */
    private File outputFileBasePath;

    /** SQLLoaderに使用するコントロールファイルのベースパス */
    private File sqlLoaderControlFileBasePath;

    /** SQLLoaderが出力するファイルのベースパス */
    private File sqlLoaderOutputFileBasePath;

    /** ステップの設定。キーはステップID */
    private Map<String, StepConfig> steps = Collections.emptyMap();

    /** ETLの設定 */
    private RootConfig etlConfig;

    /**
     * ジョブIDを取得する。
     * @return ジョブID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * ジョブIDを設定する。
     * @param jobId ジョブID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    /**
     * 入力ファイルのベースパスを取得する。
     * @return 入力ファイルのベースパス
     */
    public File getInputFileBasePath() {
        return override(inputFileBasePath, etlConfig.getInputFileBasePath());
    }

    /**
     * 入力ファイルのベースパスを設定する。
     * @param inputFileBasePath 入力ファイルのベースパス
     */
    public void setInputFileBasePath(File inputFileBasePath) {
        this.inputFileBasePath = inputFileBasePath;
    }

    /**
     * 出力ファイルのベースパスを取得する。
     * @return 出力ファイルのベースパス
     */
    public File getOutputFileBasePath() {
        return override(outputFileBasePath, etlConfig.getOutputFileBasePath());
    }

    /**
     * 出力ファイルのベースパスを設定する。
     * @param outputFileBasePath 出力ファイルのベースパス
     */
    public void setOutputFileBasePath(File outputFileBasePath) {
        this.outputFileBasePath = outputFileBasePath;
    }

    /**
     * SQLLoaderに使用するコントロールファイルのベースパスを取得する。
     * @return SQLLoaderに使用するコントロールファイルのベースパス
     */
    public File getSqlLoaderControlFileBasePath() {
        return override(sqlLoaderControlFileBasePath, etlConfig.getSqlLoaderControlFileBasePath());
    }

    /**
     * SQLLoaderに使用するコントロールファイルのベースパスを設定する。
     * @param sqlLoaderControlFileBasePath SQLLoaderに使用するコントロールファイルのベースパス
     */
    public void setSqlLoaderControlFileBasePath(File sqlLoaderControlFileBasePath) {
        this.sqlLoaderControlFileBasePath = sqlLoaderControlFileBasePath;
    }

    /**
     * SQLLoaderが出力するファイルのベースパスを取得する。
     * @return SQLLoaderが出力するファイルのベースパス
     */
    public File getSqlLoaderOutputFileBasePath() {
        return override(sqlLoaderOutputFileBasePath, etlConfig.getSqlLoaderOutputFileBasePath());
    }

    /**
     * SQLLoaderが出力するファイルのベースパスを設定する。
     * @param sqlLoaderOutputFileBasePath SQLLoaderが出力するファイルのベースパス
     */
    public void setSqlLoaderOutputFileBasePath(File sqlLoaderOutputFileBasePath) {
        this.sqlLoaderOutputFileBasePath = sqlLoaderOutputFileBasePath;
    }

    /**
     * ステップの設定を取得する。
     * @return ステップの設定
     */
    public Map<String, StepConfig> getSteps() {
        return steps;
    }

    /**
     * ステップの設定を設定する。
     * @param steps ステップの設定
     */
    public void setSteps(Map<String, StepConfig> steps) {
        this.steps = steps;
    }

    /**
     * ステップの設定を順に初期化する。
     * @param etlConfig ETLの設定
     */
    public void initialize(RootConfig etlConfig) {

        this.etlConfig = etlConfig;

        for (Map.Entry<String, StepConfig> entry : steps.entrySet()) {
            StepConfig stepConfig = entry.getValue();
            stepConfig.setStepId(entry.getKey());
            stepConfig.initialize(this);
        }
    }

    /**
     * 設定値の上書きを行うヘルパーメソッド。
     * @param value 値
     * @param defaultValue デフォルト値
     * @return 値がnullでない場合は値。値がnullの場合はデフォルト値
     */
    private <T> T override(T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }
}
