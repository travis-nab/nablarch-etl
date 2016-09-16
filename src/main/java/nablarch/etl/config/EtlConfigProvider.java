package nablarch.etl.config;

import javax.enterprise.inject.Produces;

import nablarch.core.repository.SystemRepository;

/**
 * ETLの設定を提供するクラス。
 * <p>
 * デフォルトでは、{@link JsonConfigLoader}を使用してETLの設定をロードする。
 * デフォルトのロード処理を変更したい場合は、{@link EtlConfigLoader}の実装クラスを
 * "etlConfigLoader"という名前でコンポーネント定義に設定して行う。
 * <p>
 * 本クラスのコンストラクタが呼ばれると、ETLの設定をロードし、初期化を行う。
 * 設定のロードは、JVMごとに1度しか行わない。
 * 
 * @author Kiyohito Itoh
 */
public final class EtlConfigProvider {

    /** デフォルトの{@link EtlConfigLoader} */
    private static final EtlConfigLoader DEFAULT_LOADER = new JsonConfigLoader();

    /** ETLの設定を初期化済みか否か */
    static boolean isInitialized = false;

    /** ETLの設定 */
    private static RootConfig config;

    /** インスタンス化防止 */
    private EtlConfigProvider() {
    }

    /**
     * ETLの設定を初期化済みかを判定する。
     * @return 初期化済みの場合は{@code true}
     */
    private static boolean isInitialized() {
        return isInitialized;
    }

    /**
     * ETLの設定をロードし、初期化を行う。
     */
    private static synchronized void initialize() {
        if (isInitialized()) {
            return;
        }
        config = getLoader().load();
        config.initialize();
        isInitialized = true;
    }

    /**
     * {@link EtlConfigLoader}を取得する。
     * <p>
     * "etlConfigLoader"という名前でリポジトリから取得する。
     * リポジトリに存在しない場合は{@link JsonConfigLoader}を返す。
     * @return {@link EtlConfigLoader}
     */
    private static EtlConfigLoader getLoader() {
        EtlConfigLoader loader = SystemRepository.get("etlConfigLoader");
        return loader != null ? loader : DEFAULT_LOADER;
    }

    /**
     * ETLの設定を取得する。
     * @return ETLの設定
     */
    @EtlConfig
    @Produces
    public static RootConfig getConfig() {
        if (!isInitialized()) {
            initialize();
        }
        return config;
    }
}
