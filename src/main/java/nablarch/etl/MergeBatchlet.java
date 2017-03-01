package nablarch.etl;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.common.dao.UniversalDao;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionContext;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.etl.config.DbToDbStepConfig.UpdateSize;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.StepConfig;
import nablarch.etl.generator.MergeSqlGeneratorFactory;
import nablarch.fw.batch.ee.progress.ProgressManager;

/**
 * 入力リソース(SELECT文の結果)を出力テーブルにMERGEする{@link javax.batch.api.Batchlet}実装クラス。
 *
 * @author Hisaaki Shioiri
 */
@Named
@Dependent
public class MergeBatchlet extends AbstractBatchlet {

    /** {@link JobContext} */
    private final JobContext jobContext;

    /** {@link StepContext} */
    private final StepContext stepContext;

    /** ETLの設定 */
    private final DbToDbStepConfig stepConfig;

    /** 範囲更新のヘルパークラス */
    private final RangeUpdateHelper rangeUpdateHelper;

    /** 進捗状況を管理するBean */
    private final ProgressManager progressManager;

    /**
     * コンストラクト。
     * @param jobContext {@link JobContext}
     * @param stepContext {@link StepContext}
     * @param stepConfig ステップの設定
     * @param rangeUpdateHelper 範囲更新のヘルパー
     * @param progressManager 進捗状況を管理するBean
     */
    @Inject
    public MergeBatchlet(
            final JobContext jobContext,
            final StepContext stepContext,
            @EtlConfig final StepConfig stepConfig,
            final RangeUpdateHelper rangeUpdateHelper,
            final ProgressManager progressManager) {
        this.jobContext = jobContext;
        this.stepContext = stepContext;
        this.stepConfig = (DbToDbStepConfig) stepConfig;
        this.rangeUpdateHelper = rangeUpdateHelper;
        this.progressManager = progressManager;
    }


    /**
     * 一括でのMERGE処理を行う。
     *
     * @return 結果(SUCCESS固定)
     * @throws Exception 例外
     */
    @Override
    public String process() throws Exception {

        final String jobId = jobContext.getJobName();
        final String stepId = stepContext.getStepName();

        EtlUtil.verifyRequired(jobId, stepId, "bean", stepConfig.getBean());
        EtlUtil.verifyRequired(jobId, stepId, "sqlId", stepConfig.getSqlId());
        EtlUtil.verifyRequired(jobId, stepId, "mergeOnColumns", stepConfig.getMergeOnColumns());

        final AppDbConnection connection = DbConnectionContext.getConnection();
        final String mergeSql = MergeSqlGeneratorFactory.create(DbConnectionContext.getTransactionManagerConnection())
                                                        .generateSql(stepConfig);
        final SqlPStatement statement = connection.prepareStatement(mergeSql);

        final UpdateSize updateSize = stepConfig.getUpdateSize();

        if (updateSize == null) {
            progressManager.setInputCount(UniversalDao.countBySqlFile(stepConfig.getBean(), stepConfig.getSqlId()));
            progressManager.outputProgressInfo(statement.executeUpdate());
        } else {
            EtlUtil.verifySqlRangeParameter(stepConfig);
            rangeUpdateHelper.verifyUpdateSize(updateSize);

            final Long maxSize = rangeUpdateHelper.getMaxLineNumber(stepConfig);
            progressManager.setInputCount(maxSize);
            
            final Range range = new Range(updateSize.getSize(), maxSize);
            while (range.next()) {
                statement.setLong(1, range.from);
                statement.setLong(2, range.to);
                statement.executeUpdate();
                commit();
                progressManager.outputProgressInfo(range.to);
            }
        }

        return "SUCCESS";
    }

    /**
     * コミットを行う。
     */
    private static void commit() {
        TransactionContext.getTransaction().commit();
    }
}
