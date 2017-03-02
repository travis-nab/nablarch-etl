package nablarch.etl;

import java.util.List;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.StepConfig;
import nablarch.etl.config.TruncateStepConfig;
import nablarch.etl.generator.TruncateSqlGenerator;
import nablarch.etl.generator.TruncateSqlGeneratorFactory;

import javax.batch.api.AbstractBatchlet;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * テーブルのデータをクリーニング(truncate)する{@link javax.batch.api.Batchlet}実装クラス。
 * <p/>
 * {@link TruncateStepConfig}で指定されたEntityクラスに対応するテーブルのデータをクリーニング(truncate)する。
 *
 * @author Hisaaki Shioiri
 */
@Named
@Dependent
public class TableCleaningBatchlet extends AbstractBatchlet {

    /** ETLの設定 */
    private final TruncateStepConfig stepConfig;

    /**
     * コンストラクタ。
     *
     * @param stepConfig ステップの設定
     */
    @Inject
    public TableCleaningBatchlet(@EtlConfig final StepConfig stepConfig) {
        this.stepConfig = (TruncateStepConfig) stepConfig;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 本処理では、TRUNCATEのSQL文を構築する際にステートメントを発行しているが、
     * RDBMS製品によっては、TRUNCATE文の発行はトランザクション内の最初のステートメントである必要があるため、
     * TRUNCATEのSQL文の構築後に明示的にトランザクションをロールバックしている。
     * <p/>
     * そのため、もしステップリスナ等で事前にデータベースへの更新等を行っている場合、
     * その処理は取り消されるため注意すること。
     */
    @Override
    public String process() throws Exception {
        final List<Class<?>> entities = stepConfig.getEntities();
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        final TruncateSqlGenerator sqlGenerator = TruncateSqlGeneratorFactory.create(connection);

        for (final Class<?> entity : entities) {
            final String sql = sqlGenerator.generateSql(entity);
            connection.rollback();

            final SqlPStatement statement = connection.prepareStatement(sql);
            statement.execute();
            connection.commit();
        }
        return "SUCCESS";
    }
}
