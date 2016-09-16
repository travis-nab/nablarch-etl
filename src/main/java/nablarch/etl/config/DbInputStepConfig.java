package nablarch.etl.config;

import nablarch.core.util.annotation.Published;

/**
 * DBを入力とするステップの設定を保持するクラス。
 * 
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public abstract class DbInputStepConfig extends StepConfig {

    /** Beanクラス */
    private Class<?> bean;

    /** SQL_ID */
    private String sqlId;

    /**
     * Beanクラスを取得する。
     * @return Beanクラス
     */
    public Class<?> getBean() {
        return bean;
    }

    /**
     * Beanクラスを設定する。
     * @param bean Beanクラス
     */
    public void setBean(Class<?> bean) {
        this.bean = bean;
    }

    /**
     * SQL_IDを取得する。
     * @return SQL_ID
     */
    public String getSqlId() {
        return sqlId;
    }

    /**
     * SQL_IDを設定する。
     * @param sqlId SQL_ID
     */
    public void setSqlId(String sqlId) {
        this.sqlId = sqlId;
    }
}
