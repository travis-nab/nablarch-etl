package nablarch.etl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.core.log.basic.LogLevel;
import nablarch.core.log.operation.OperationLogger;
import nablarch.core.message.MessageLevel;
import nablarch.core.message.MessageUtil;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.FileUtil;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.FileToDbStepConfig;
import nablarch.etl.config.PathConfig;
import nablarch.etl.config.StepConfig;

/**
 * SQL*Loaderを用いてCSVファイルのデータをワークテーブルに登録する{@link javax.batch.api.Batchlet}の実装クラス。
 *
 * @author Naoki Yamamoto
 */
@Named
@Dependent
public class SqlLoaderBatchlet extends AbstractBatchlet {

    /** {@link SystemRepository}に登録されているDB接続ユーザのキー */
    private static final String USER_KEY = "db.user";

    /** {@link SystemRepository}に登録されているDB接続パスワードのキー */
    private static final String PASSWORD_KEY = "db.password";

    /** {@link SystemRepository}に登録されているデータベース名のキー */
    private static final String DATABASE_NAME_KEY = "db.databaseName";

    /** {@link JobContext} */
    private final JobContext jobContext;

    /** {@link StepContext} */
    private final StepContext stepContext;

    /** ETLの設定 */
    private final FileToDbStepConfig stepConfig;

    /** 入力ファイルのベースパス */
    private final File inputFileBasePath;

    /** SQLLoaderに使用するコントロールファイルのベースパス */
    private final File sqlLoaderControlFileBasePath;

    /** SQLLoaderが出力するファイルのベースパス */
    private final File sqlLoaderOutputFileBasePath;

    /**
     * コンストラクタ。
     * @param jobContext JobContext
     * @param stepContext StepContext
     * @param stepConfig ステップの設定
     * @param inputFileBasePath 入力ファイルのあるディレクトリ
     * @param sqlLoaderControlFileBasePath SQL*Loaderのコントロールファイルが置かれたディレクトリ
     * @param sqlLoaderOutputFileBasePath SQL*Loaderが出力するファイルを置くディレクトリ
     */
    @Inject
    public SqlLoaderBatchlet(
            final JobContext jobContext,
            final StepContext stepContext,
            @EtlConfig final StepConfig stepConfig,
            @PathConfig(BasePath.INPUT) final File inputFileBasePath,
            @PathConfig(BasePath.SQLLOADER_CONTROL) final File sqlLoaderControlFileBasePath,
            @PathConfig(BasePath.SQLLOADER_OUTPUT) final File sqlLoaderOutputFileBasePath) {
        this.jobContext = jobContext;
        this.stepContext = stepContext;
        this.stepConfig = (FileToDbStepConfig) stepConfig;
        this.inputFileBasePath = inputFileBasePath;
        this.sqlLoaderControlFileBasePath = sqlLoaderControlFileBasePath;
        this.sqlLoaderOutputFileBasePath = sqlLoaderOutputFileBasePath;
    }


    /**
     * SQL*Loaderを実行してCSVファイルのデータをワークテーブルに一括登録する。
     *
     * @return 実行結果
     * @throws Exception 例外
     */
    @Override
    public String process() throws Exception {

        final String user = getUser();
        final String password = getPassword();
        final String databaseName = getDatabaseName();

        final String jobId = jobContext.getJobName();
        final String stepId = stepContext.getStepName();

        EtlUtil.verifyRequired(jobId, stepId, "fileName", stepConfig.getFileName());
        EtlUtil.verifyRequired(jobId, stepId, "bean", stepConfig.getBean());

        final String ctlFileName = stepConfig.getBean().getSimpleName();
        final String ctlFile = new File(sqlLoaderControlFileBasePath, ctlFileName + ".ctl").getPath();
        final File dataFile = new File(inputFileBasePath, stepConfig.getFileName());
        final String badFile = new File(sqlLoaderOutputFileBasePath, ctlFileName + ".bad").getPath();
        final String logFile = new File(sqlLoaderOutputFileBasePath, ctlFileName + ".log").getPath();

        if (!dataFile.isFile()) {
            OperationLogger.write(LogLevel.ERROR,
                    MessageUtil.createMessage(MessageLevel.ERROR, "nablarch.etl.input-file-not-found", dataFile.getAbsoluteFile()).formatMessage());
            return "FAILED";
        }

        SqlLoaderRunner runner = new SqlLoaderRunner(user, password, databaseName, ctlFile, dataFile.getPath(), badFile, logFile);
        runner.execute();

        if (!runner.isSuccess()) {
            throw new SqlLoaderFailedException(
                    "failed to execute SQL*Loader. controlFile = [" + ctlFile + ']');
        }
        return "SUCCESS";
    }

    /**
     * SQL*Loaderの実行に必要なDB接続ユーザ情報を{@link SystemRepository}より以下のキー名で取得する。
     * <ul>
     *     <li>db.user</li>
     * </ul>
     * ユーザの取得方法を変更したい場合は本メソッドをオーバーライドし、任意の処理を記述すること。
     *
     * @return ユーザ
     */
    protected String getUser() {
        return SystemRepository.getString(USER_KEY);
    }

    /**
     * SQL*Loaderの実行に必要なDB接続パスワード情報を{@link SystemRepository}より以下のキー名で取得する。
     * <ul>
     *     <li>db.password</li>
     * </ul>
     * パスワードの取得方法を変更したい場合は本メソッドをオーバーライドし、任意の処理を記述すること。
     *
     * @return パスワード
     */
    protected String getPassword() {
        return SystemRepository.getString(PASSWORD_KEY);
    }

    /**
     * SQL*Loaderの実行に必要なデータベース名を{@link SystemRepository}より以下のキー名で取得する。
     * <ul>
     *     <li>db.databaseName</li>
     * </ul>
     * データベース名の取得方法を変更したい場合は本メソッドをオーバーライドし、任意の処理を記述すること。
     *
     * @return データベース名
     */
    protected String getDatabaseName() {
        return SystemRepository.getString(DATABASE_NAME_KEY);
    }

    /**
     * SQL*Loaderを実行するクラス。
     *
     * @author Naoki Yamamoto
     */
    public static class SqlLoaderRunner {

        /** DB接続ユーザ名 */
        private final String userName;

        /** DB接続パスワード */
        private final String password;

        /** データベース名 */
        private final String databaseName;

        /** コントロールファイル */
        private final String ctrlFile;

        /** データファイル */
        private final String dataFile;

        /** BADファイル */
        private final String badFile;

        /** ログファイル */
        private final String logFile;

        /** SQL*Loaderの{@link Process} */
        private Process process;

        /**
         * コンストラクタ。
         *
         * @param userName DB接続ユーザ名
         * @param password DB接続パスワード
         * @param databaseName データベース名
         * @param ctrlFile コントロールファイル
         * @param dataFile データファイル
         * @param badFile BADファイル
         * @param logFile ログファイル
         */
        public SqlLoaderRunner(String userName, String password, String databaseName, String ctrlFile,
                               String dataFile, String badFile, String logFile) {
            this.userName = userName;
            this.password = password;
            this.databaseName = databaseName;
            this.ctrlFile = ctrlFile;
            this.dataFile = dataFile;
            this.badFile = badFile;
            this.logFile = logFile;
        }

        /**
         * SQL*Loaderを実行する。
         *
         * @throws IOException 入出力例外
         * @throws InterruptedException SQL*Loaderの終了待ちをしているスレッドが、他のスレッドによって割り込まれた場合
         */
        public void execute() throws IOException, InterruptedException {
            ProcessBuilder pb = new ProcessBuilder(
                    "sqlldr",
                    "USERID=" + userName + "/" + password + "@" + databaseName,
                    "CONTROL=" + ctrlFile,
                    "DATA=" + dataFile,
                    "BAD=" + badFile,
                    "LOG=" + logFile,
                    "SILENT=(HEADER,FEEDBACK)");
            pb.redirectErrorStream(true);
            process = pb.start();

            InputStream is = process.getInputStream();
            try {
                while (is.read() >= 0) {
                    // 標準出力の内容はログに出力しないが、
                    // バッファサイズが上限を超えた場合にプロセスが待ち状態になるのを防ぐため、
                    // プロセスが完了するまで標準出力の読み込みを行う。
                }
            } finally {
                FileUtil.closeQuietly(is);
            }

            process.waitFor();
        }

        /**
         * SQL*Loaderのプロセスが正常終了しているか否か。
         *
         * @return プロセスが正常終了していれば{@code true}
         */
        public boolean isSuccess() {
            return process.exitValue() == 0;
        }
    }
}
