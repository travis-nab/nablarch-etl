package nablarch.etl.generator;

import nablarch.etl.config.DbToDbStepConfig;

/**
 * MERGE用のSQL文を生成するジェネレータクラス。
 * 
 * @author siosio
 */
public abstract class MergeSqlGenerator extends SqlGeneratorSupport<DbToDbStepConfig> {

    @Override
    public final String generateSql(final DbToDbStepConfig config) {
        verify(config.getBean());
        return generate(config);
    }

    /**
     * MERGE文を生成する。
     * @param config ステップの設定
     * @return 生成したMERGE文
     */
    public abstract String generate(final DbToDbStepConfig config);
}
