package nablarch.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.BatchRuntimeException;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import nablarch.common.databind.ObjectMapper;
import nablarch.common.databind.ObjectMapperFactory;
import nablarch.core.log.basic.LogLevel;
import nablarch.core.log.operation.OperationLogger;
import nablarch.core.message.MessageLevel;
import nablarch.core.message.MessageUtil;
import nablarch.etl.config.DbToFileStepConfig;
import nablarch.etl.config.EtlConfig;
import nablarch.etl.config.PathConfig;
import nablarch.etl.config.StepConfig;

/**
 * ファイルにデータを書き込む{@link javax.batch.api.chunk.ItemWriter}の実装クラス。
 *
 * @author Kumiko Omi
 */
@Dependent
@Named
public class FileItemWriter extends AbstractItemWriter {

    /** {@link JobContext} */
    @Inject
    private JobContext jobContext;

    /** {@link StepContext} */
    @Inject
    private StepContext stepContext;

    /** ETLの設定 */
    @EtlConfig
    @Inject
    private StepConfig stepConfig;

    /** 出力ファイルのベースパス */
    @PathConfig(BasePath.OUTPUT)
    @Inject
    private File outputFileBasePath;

    /** Javaオブジェクトからデータに変換を行うマッパー */
    ObjectMapper<Object> mapper;

    @SuppressWarnings("unchecked")
    @Override
    public void open(Serializable checkpoint) throws Exception {

        final String jobId = jobContext.getJobName();
        final String stepId = stepContext.getStepName();

        final DbToFileStepConfig config = (DbToFileStepConfig) stepConfig;

        EtlUtil.verifyRequired(jobId, stepId, "bean", config.getBean());
        EtlUtil.verifyRequired(jobId, stepId, "fileName", config.getFileName());

        final File outputFile = new File(outputFileBasePath, config.getFileName());
        try {
            mapper = (ObjectMapper<Object>) ObjectMapperFactory.create(
                    config.getBean(), new FileOutputStream(outputFile));
        } catch (FileNotFoundException e) {
            final String message = MessageUtil.createMessage(
                    MessageLevel.ERROR, "nablarch.etl.invalid-output-file-path", outputFile.getAbsolutePath())
                                              .formatMessage();
            OperationLogger.write(LogLevel.ERROR, message);
            throw new BatchRuntimeException(message, e);
        }

        super.open(checkpoint);
    }

    @Override
    public void writeItems(List<Object> items) throws IOException {
        for (Object item : items) {
            mapper.write(item);
        }
    }

    @Override
    public void close() throws Exception {
        if (mapper != null) {
            mapper.close();
        }
    }
}
