package nablarch.etl.generator;

import nablarch.etl.config.DbToDbStepConfig;

/**
 * Oracleのダイレクトパスインサートを使用するinsert文を生成するクラス。
 *
 * @author Hisaaki Shioiri
 */
public class OracleDirectPathInsertSqlGenerator extends InsertSqlGenerator {

    /**
     * Oracleのダイレクトパスインサート用のヒントを設定したinsert into句を生成する。
     *
     * @param config ステップの設定(この実装では使用しない)
     * @return ダイレクトパスインサート用のヒント(appendヒント)を設定したinsert into句
     */
    @Override
    protected String generateInsertIntoClause(DbToDbStepConfig config) {
        return "insert /*+ APPEND */ into";
    }
}
