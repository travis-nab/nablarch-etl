package nablarch.etl.config;

import nablarch.core.util.annotation.Published;

/**
 * ステップの設定をサポートするクラス。
 * 
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public abstract class StepConfig {

    /** ステップID */
    private String stepId;

    /** ジョブの設定 */
    private JobConfig jobConfig;

    /**
     * ステップIDを取得する。
     * @return ステップID
     */
    public String getStepId() {
        return stepId;
    }

    /**
     * ステップIDを設定する。
     * @param stepId ステップID
     */
    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    /**
     * ジョブの設定を取得する。
     * @return ジョブの設定
     */
    public JobConfig getJobConfig() {
        return jobConfig;
    }

    /**
     * 設定値から初期化を行う。
     * @param jobConfig ジョブの設定
     */
    public final void initialize(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
        onInitialize();
    }

    /**
     * 設定値から初期化を行う。
     */
    protected abstract void onInitialize();
}
