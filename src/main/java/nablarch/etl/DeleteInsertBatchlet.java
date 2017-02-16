package nablarch.etl;

import java.text.MessageFormat;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.common.dao.EntityUtil;
import nablarch.common.dao.UniversalDao;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionContext;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.etl.config.DbToDbStepConfig.InsertMode;
import nablarch.etl.config.DbToDbStepConfig.UpdateSize;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.StepConfig;
import nablarch.etl.generator.InsertSqlGenerator;
import nablarch.fw.batch.ee.progress.ProgressManager;
import nablarch.fw.batch.progress.ProgressLogger;

/**
 * テーブル間のデータ転送を行う{@link javax.batch.api.Batchlet}実装クラス。
 * <p/>
 * 移送先テーブルのデータをクリーニング後に、移送元のデータを一括で移送先のテーブルに転送（登録）する。
 *
 * @author Hisaaki Shioiri
 */
@Named
@Dependent
public class DeleteInsertBatchlet extends AbstractBatchlet {

    /** {@link JobContext} */
    private final JobContext jobContext;

    /** {@link StepContext} */
    private final StepContext stepContext;

    /** 範囲更新のヘルパークラス */
    private final RangeUpdateHelper rangeUpdateHelper;

    /** ETLの設定 */
    private final StepConfig stepConfig;

    /** 進捗状況を管理するBean */
    private final ProgressManager progressManager;

    /**
     * コンストラクタ。
     *
     * @param jobContext {@link JobContext}
     * @param stepContext {@link StepContext}
     * @param rangeUpdateHelper {@link RangeUpdateHelper}
     * @param stepConfig ステップ設定
     * @param progressManager {@link ProgressManager}
     */
    @Inject
    public DeleteInsertBatchlet(
            final JobContext jobContext,
            final StepContext stepContext,
            final RangeUpdateHelper rangeUpdateHelper,
            @EtlConfig final StepConfig stepConfig,
            final ProgressManager progressManager) {
        this.jobContext = jobContext;
        this.stepContext = stepContext;
        this.rangeUpdateHelper = rangeUpdateHelper;
        this.stepConfig = stepConfig;
        this.progressManager = progressManager;
    }

    /**
     * 一括登録処理を行う。
     *
     * @return 結果(SUCCESS固定)
     * @throws Exception 例外
     */
    @Override
    public String process() throws Exception {

        final DbToDbStepConfig config = (DbToDbStepConfig) stepConfig;

        verify(config);

        final AppDbConnection connection = DbConnectionContext.getConnection();

        cleaning(connection, config);

        insert(connection, config);

        return "SUCCESS";
    }

    /**
     * 設定値の検証を行う。
     *
     * @param config 設定
     */
    private void verify(final DbToDbStepConfig config) {
        final String stepName = stepContext.getStepName();
        final String jobName = jobContext.getJobName();
        EtlUtil.verifyRequired(jobName, stepName, "bean", config.getBean());
        EtlUtil.verifyRequired(jobName, stepName, "sqlId", config.getSqlId());

        final UpdateSize updateSize = config.getUpdateSize();
        final InsertMode insertMode = config.getInsertMode();

        if (insertMode == InsertMode.ORACLE_DIRECT_PATH && updateSize != null) {
            throw new InvalidEtlConfigException("Oracle Direct Path mode does not support UpdateSize.");
        }

        if (updateSize != null) {
            EtlUtil.verifySqlRangeParameter(config);
            rangeUpdateHelper.verifyUpdateSize(updateSize);
        }
    }

    /**
     * テーブルのクリーニング処理を行う。
     *
     * @param connection データベース接続
     * @param config 設定
     */
    private void cleaning(final AppDbConnection connection, final DbToDbStepConfig config) {
        final String tableName = EntityUtil.getTableName(config.getBean());
        final SqlPStatement statement = connection.prepareStatement(
                "delete from " + tableName);
        statement.execute();
        loggingCleaning(tableName, statement.getUpdateCount());
    }

    /**
     * テーブルへの登録処理を行う。
     *
     * @param connection データベース接続
     * @param config 設定
     */
    private void insert(final AppDbConnection connection, final DbToDbStepConfig config) {
        final InsertMode mode = config.getInsertMode();
        final UpdateSize updateSize = config.getUpdateSize();
        final InsertSqlGenerator sqlGenerator = mode.getInsertSqlGenerator();

        final SqlPStatement statement = connection.prepareStatement(sqlGenerator.generateSql(config));
        
        if (mode == InsertMode.ORACLE_DIRECT_PATH || updateSize == null) {
            progressManager.setInputCount(UniversalDao.countBySqlFile(config.getBean(), config.getSqlId()));
            progressManager.outputProgressInfo(statement.executeUpdate());
        } else {
            final Long maxLineNum = rangeUpdateHelper.getMaxLineNumber(config);
            progressManager.setInputCount(maxLineNum);
            final Range range = new Range(updateSize.getSize(), maxLineNum);
            while (range.next()) {
                statement.setLong(1, range.from);
                statement.setLong(2, range.to);
                statement.executeUpdate();
                commit();
                progressManager.outputProgressInfo(range.to);
            }
        }
    }

    /**
     * コミットを行う。
     */
    private static void commit() {
        TransactionContext.getTransaction().commit();
    }

    /**
     * クリーニングのログを出力する。
     * @param tableName クリーンしたテーブル名
     * @param deleteCount 削除した件数
     */
    private void loggingCleaning(final String tableName, final int deleteCount) {
        ProgressLogger.write(MessageFormat.format(""
                        + "job name: [{0}] step name: [{1}] table name: [{2}] delete count: [{3}]",
                jobContext.getJobName(), stepContext.getStepName(), tableName, deleteCount));
    }
}
