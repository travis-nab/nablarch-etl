package nablarch.etl;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.List;

import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.common.dao.EntityUtil;
import nablarch.common.dao.UniversalDao;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.FileToDbStepConfig;
import nablarch.etl.config.StepConfig;
import nablarch.fw.batch.progress.ProgressLogger;


/**
 * データベースのテーブルに対してデータを書き込む{@link javax.batch.api.chunk.ItemWriter}実装クラス。
 * <p/>
 * {@link UniversalDao#insert(Object)}を使用して、Entityオブジェクトの内容をデータベースに登録する。
 *
 * @author Hisaaki Shioiri
 */
@Named
@Dependent
public class DatabaseItemWriter extends AbstractItemWriter {

    /** {@link JobContext} */
    private final JobContext jobContext;

    /** {@link StepContext} */
    private final StepContext stepContext;

    /** ETLの設定 */
    private final StepConfig stepConfig;

    /**
     * コンストラクタ。
     *
     * @param jobContext {@link JobContext}
     * @param stepContext {@link StepContext}
     * @param stepConfig ステップの設定
     */
    @Inject
    public DatabaseItemWriter(
            final JobContext jobContext,
            final StepContext stepContext,
            @EtlConfig final StepConfig stepConfig) {
        this.jobContext = jobContext;
        this.stepContext = stepContext;
        this.stepConfig = stepConfig;
    }

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (stepConfig instanceof DbToDbStepConfig) {
            loggingStartChunk(EntityUtil.getTableName(((DbToDbStepConfig)stepConfig).getBean()));
        } else if (stepConfig instanceof FileToDbStepConfig) {
            loggingStartChunk(EntityUtil.getTableName(((FileToDbStepConfig)stepConfig).getBean()));
        } else {
            throw new InvalidEtlConfigException(
                    "unsupported config type. supported class is DbToDbStepConfig or FileToDbStepConfig."
                            + " step config class: " + getStepConfigClassName());
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        UniversalDao.batchInsert(items);
    }

    /**
     * 進捗ログを出力する。
     * @param tableName 登録先テーブル名
     */
    private void loggingStartChunk(final String tableName) {
        ProgressLogger.write(MessageFormat.format("job name: [{0}] step name: [{1}] write table name: [{2}]",
                jobContext.getJobName(), stepContext.getStepName(), tableName));
    }

    /**
     * {@link StepConfig}のクラス名を返す。
     * <p>
     * {@link StepConfig}が{@code null}の場合は、文字列の{@code null}を返す。
     *
     * @return StepConfigのクラス名
     */
    private String getStepConfigClassName() {
        return stepConfig == null ? "null" : stepConfig.getClass()
                                                       .getName();
    }
}
