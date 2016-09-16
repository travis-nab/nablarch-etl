package nablarch.etl.config;

import java.util.List;

import nablarch.core.util.annotation.Published;

/**
 * truncateステップの設定を保持するクラス。
 *
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class TruncateStepConfig extends StepConfig {

    /** truncate対象のEntityのリスト */
    private List<Class<?>> entities;

    /**
     * 初期処理では特に何も行わない。
     */
    @Override
    protected void onInitialize() {
        // nop
    }

    /**
     * 削除対象のEntityリストをかえす。
     *
     * @return 削除対象のEntityリスト
     */
    public List<Class<?>> getEntities() {
        return entities;
    }

    /**
     * 削除対象のEntityリストを設定する。
     *
     * @param entities 削除対象のEntityリスト
     */
    public void setEntities(final List<Class<?>> entities) {
        this.entities = entities;
    }
}
