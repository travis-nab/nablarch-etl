package nablarch.etl.generator;

import javax.persistence.Entity;

import nablarch.core.util.annotation.Published;
import nablarch.etl.InvalidEtlConfigException;
import nablarch.etl.config.StepConfig;

/**
 * SQL文を生成するのをサポートする抽象クラス。
 *
 * @author Hisaaki Shioiri
 * @param <T> {@link StepConfig}のサブクラス
 */
@Published(tag = "architect")
public abstract class SqlGeneratorSupport<T extends StepConfig> {

    /**
     * 入力値の妥当性を検証する。
     * <p/>
     * 入力値のクラスに対して{@link Entity}
     *
     * @param clazz Entityクラス
     */
    protected static void verify(final Class<?> clazz) {
        if (clazz.getAnnotation(Entity.class) == null) {
            throw new InvalidEtlConfigException(clazz.getName() + " isn't a entity class.");
        }
    }

    /**
     * ステップの設定からデータを移送するためのSQL文を生成する。
     *
     * @param config ステップの設定
     * @return 生成したSQL文
     */
    public abstract String generateSql(T config);
}
