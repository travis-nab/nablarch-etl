package nablarch.etl.generator;

import nablarch.etl.config.DbToDbStepConfig;

/**
 * 入力リソース(SELECT)文から登録/更新を一括で行うSQLServerデータベース用のMERGE文を生成するクラス。
 * 
 * SQL Serverでは、MERGE文の末尾にセミコロン(;)が必要なため、
 * {@link OracleMergeSqlGenerator}で生成したSQL文の末尾にセミコロンを付加し返却する。
 *
 * @author Hisaaki Shioiri
 */
public class SqlServerMergeSqlGenerator extends MergeSqlGenerator{

    /** 移譲先 */
    private final MergeSqlGenerator delegatee = new OracleMergeSqlGenerator();

    @Override
    public String generate(final DbToDbStepConfig config) {
        return delegatee.generate(config) + ';';
    }
}
