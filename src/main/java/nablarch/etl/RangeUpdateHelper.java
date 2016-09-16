package nablarch.etl;

import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.util.annotation.Published;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.etl.config.DbToDbStepConfig.UpdateSize;
import nablarch.etl.generator.MaxLineNumberSqlGenerator;

/**
 * Range更新のヘルパークラス。
 *
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
@Dependent
public class RangeUpdateHelper {

    /** {@link JobContext} */
    @Inject
    private JobContext jobContext;

    /** {@link StepContext} */
    @Inject
    private StepContext stepContext;

    /**
     * {@link UpdateSize}の検証を行う。
     * <p/>
     * {@link UpdateSize}が非nullの場合に以下の検証を行う。
     * <ul>
     * <li>{@link UpdateSize#getBean()}が設定されていること</li>
     * <li>{@link UpdateSize#getSize()}が設定されていること</li>
     * <li>{@link UpdateSize#getSize()}が0より大きいこと</li>
     * </ul>
     *
     * @param updateSize {@link UpdateSize}
     */
    public void verifyUpdateSize(final UpdateSize updateSize) {
        final String jobId = jobContext.getJobName();
        final String stepId = stepContext.getStepName();
        EtlUtil.verifyRequired(jobId, stepId, "updateSize > size", updateSize.getSize());
        verifyUpdateSize(jobId, stepId, "updateSize > size", updateSize.getSize());
        EtlUtil.verifyRequired(jobId, stepId, "updateSize > bean", updateSize.getBean());
    }

    /**
     * 更新サイズの設定値を検証する。
     * <p/>
     * 値が1未満の場合は、{@link IllegalArgumentException}を送出する。
     *
     * @param jobId ジョブID
     * @param stepId ステップID
     * @param key キー
     * @param value 値
     */
    private static void verifyUpdateSize(
            final String jobId, final String stepId, final String key, final Integer value) {

        if (value <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be greater than 0. jobId = [%s], stepId = [%s], size = [%s]",
                            key, jobId, stepId, value));
        }
    }

    /**
     * 入力元テーブルのLINE_NUMBERカラムの最大値を取得する。
     *
     * @param config {@link DbToDbStepConfig}
     * @return 入力元テーブルのLINE_NUMBERカラムの最大値
     */
    public Long getMaxLineNumber(final DbToDbStepConfig config) {
        final MaxLineNumberSqlGenerator sqlGenerator = new MaxLineNumberSqlGenerator();
        final AppDbConnection connection = DbConnectionContext.getConnection();
        final SqlPStatement statement = connection.prepareStatement(sqlGenerator.generateSql(config));
        ResultSetIterator rs = null;
        try {
            rs = statement.executeQuery();
            rs.next();
            final Long maxLineNumber = rs.getLong(1);
            return maxLineNumber != null ? maxLineNumber : 0L;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }
}

