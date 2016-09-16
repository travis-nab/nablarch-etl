package nablarch.etl.config;

import java.util.List;

import nablarch.core.db.statement.BasicSqlLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.annotation.Published;
import nablarch.etl.generator.InsertSqlGenerator;
import nablarch.etl.generator.OracleDirectPathInsertSqlGenerator;

/**
 * DBtoDBステップの設定を保持するクラス。
 * <p/>
 * 本クラスでは、{@link BasicSqlLoader}をリポジトリから取得し、
 * SQL_IDに対応するSQL文を取得する。
 * このため、本クラスを使用する場合、"sqlLoader"という名前で
 * {@link BasicSqlLoader}をコンポーネント定義に設定する必要がある。
 *
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public class DbToDbStepConfig extends DbInputStepConfig {

    /** MERGEのON句に指定するカラム名 */
    private List<String> mergeOnColumns;

    /** 1回のSQL実行で処理するサイズに関する設定 */
    private UpdateSize updateSize;

    /** SQL_IDに対応するSQL文 */
    private String sql;

    /** INSERTモード */
    private InsertMode insertMode = InsertMode.NORMAL;

    /**
     * MERGEのON句に指定するカラム名を取得する。
     *
     * @return MERGEのON句に指定するカラム名
     */
    public List<String> getMergeOnColumns() {
        return mergeOnColumns;
    }

    /**
     * MERGEのON句に指定するカラム名を設定する。
     *
     * @param mergeOnColumns MERGEのON句に指定するカラム名
     */
    public void setMergeOnColumns(List<String> mergeOnColumns) {
        this.mergeOnColumns = mergeOnColumns;
    }

    /**
     * 1回のSQL実行で処理するサイズに関する設定を取得する。
     *
     * @return 1回のSQL実行で処理するサイズに関する設定
     */
    public UpdateSize getUpdateSize() {
        return updateSize;
    }

    /**
     * 1回のSQL実行で処理するサイズに関する設定を設定する。
     *
     * @param updateSize 1回のSQL実行で処理するサイズに関する設定
     */
    public void setUpdateSize(UpdateSize updateSize) {
        this.updateSize = updateSize;
    }

    /**
     * INSERTモードを取得する。
     *
     * @return INSERTモード
     */
    public InsertMode getInsertMode() {
        return insertMode;
    }

    /**
     * INSERTモードを設定する。
     *
     * @param insertMode INSERTモード
     */
    public void setInsertMode(final InsertMode insertMode) {
        this.insertMode = insertMode;
    }

    /**
     * 初期化を行う。
     */
    @Override
    protected void onInitialize() {
        sql = loadSql();
    }

    /**
     * SQL_IDに対応するSQL文を取得する。
     *
     * @return SQL_IDに対応するSQL文
     */
    public String getSql() {
        return sql;
    }

    /**
     * SQL_IDに対応するSQL文をロードする。
     *
     * @return SQL_IDに対応するSQL文
     */
    private String loadSql() {

        final String sqlLoaderName = "sqlLoader";
        final BasicSqlLoader loader = SystemRepository.get(sqlLoaderName);

        if (loader == null) {
            throw new IllegalStateException(
                    String.format("BasicSqlLoader was not found. Using the name \"%s\", "
                                    + "please set BasicSqlLoader to component configuration.",
                            sqlLoaderName));
        }

        return loader.getValue(getBean().getName())
                .get(getSqlId());
    }

    /**
     * 1回のSQL実行で処理するサイズに関する設定を保持するクラス。
     *
     * @author Kiyohito Itoh
     */
    public static final class UpdateSize {

        /** 1回のSQL実行で処理するサイズ */
        private Integer size;

        /** データ取得元のBeanクラス */
        private Class<?> bean;

        /**
         * 1回のSQL実行で処理するサイズを取得する。
         *
         * @return 1回のSQL実行で処理するサイズ
         */
        public Integer getSize() {
            return size;
        }

        /**
         * 1回のSQL実行で処理するサイズを設定する。
         *
         * @param size 1回のSQL実行で処理するサイズ
         */
        public void setSize(Integer size) {
            this.size = size;
        }

        /**
         * データ取得元のBeanクラスを取得する。
         *
         * @return データ取得元のBeanクラス
         */
        public Class<?> getBean() {
            return bean;
        }

        /**
         * データ取得元のBeanクラスを設定する。
         *
         * @param bean データ取得元のBeanクラス
         */
        public void setBean(Class<?> bean) {
            this.bean = bean;
        }
    }

    /**
     * ロードステップのINSERTモード。
     */
    @Published(tag = "architect")
    public enum InsertMode {
        /** ノーマルモード */
        NORMAL {
            @Override
            public InsertSqlGenerator getInsertSqlGenerator() {
                return new InsertSqlGenerator();
            }
        },
        /** Oracleのダイレクトパスインサートモード */
        ORACLE_DIRECT_PATH {
            @Override
            public InsertSqlGenerator getInsertSqlGenerator() {
                return new OracleDirectPathInsertSqlGenerator();
            }
        };

        /**
         * INSERT文を生成する{@link InsertSqlGenerator}を取得する。
         *
         * @return INSERT文のGenerator
         */
        public abstract InsertSqlGenerator getInsertSqlGenerator();
    }
}
