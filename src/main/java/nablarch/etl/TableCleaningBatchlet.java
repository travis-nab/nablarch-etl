package nablarch.etl;

import nablarch.common.dao.EntityUtil;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.StepConfig;
import nablarch.etl.config.TruncateStepConfig;

import javax.batch.api.AbstractBatchlet;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

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


    @Override
    public String process() throws Exception {

        final List<Class<?>> entities = stepConfig.getEntities();

        final AppDbConnection connection = DbConnectionContext.getConnection();
        for (Class<?> entity : entities) {
            final SqlPStatement statement = connection.prepareStatement(
                    "truncate table " + EntityUtil.getTableNameWithSchema(entity));
            statement.execute();
        }

        return "SUCCESS";
    }
}
