package nablarch.etl.config;

import nablarch.core.util.annotation.Published;

/**
 * Validationを行う{@link nablarch.etl.ValidationBatchlet}用のステップ設定を保持するクラス。
 * <p/>
 * Validationステップには、以下の設定が必要となる。
 * <ul>
 * <li>ワークテーブルのEntity</li>
 * <li>エラーレコードを格納するテーブルのEntity(ワークテーブルのEntityを継承してテーブル名のみ変更したEntity)</li>
 * <li>Validationエラーがあった場合のモード(継続するかアボートするか)
 * デフォルト値は、アボート
 * </li>
 * <li>許容するエラーの件数。エラー数がこの数を超えた場合はJOBがアボートする</li>
 * </ul>
 *
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class ValidationStepConfig extends DbInputStepConfig {

    /** エラーテーブルのEntityクラス */
    private Class<?> errorEntity;

    /** モード */
    private Mode mode = Mode.ABORT;

    /** 許容するエラー数 */
    private Integer errorLimit;

    /**
     * 初期化処理では特に何もしない
     */
    @Override
    protected void onInitialize() {
    }

    /**
     * エラーテーブルのEntityクラスを取得する。
     *
     * @return エラーテーブルのEntityクラス
     */
    public Class<?> getErrorEntity() {
        return errorEntity;
    }

    /**
     * エラーテーブルのEntityクラスを設定する。
     *
     * @param errorEntity エラーテーブルのEntityクラス
     */
    public void setErrorTableEntity(final Class<?> errorEntity) {
        this.errorEntity = errorEntity;
    }

    /**
     * 処理の継続モードを取得する。
     *
     * @return 処理の継続モード
     */
    public Mode getMode() {
        return mode;
    }

    /**
     * 処理の継続モードを設定する。
     *
     * @param mode 処理の継続モード
     */
    public void setMode(final Mode mode) {
        this.mode = mode;
    }

    /**
     * 許容するエラー数を取得する。
     *
     * @return 許容するエラー数
     */
    public Integer getErrorLimit() {
        return errorLimit;
    }

    /**
     * 許容するエラー数を設定する。
     *
     * @param errorLimit 許容するエラー数
     */
    public void setErrorLimit(final Integer errorLimit) {
        this.errorLimit = errorLimit;
    }

    /**
     * Validationエラー発生時の処理継続モード
     */
    @Published(tag = "architect")
    public enum Mode {
        /** 継続 */
        CONTINUE,
        /** 終了 */
        ABORT
    }
}
