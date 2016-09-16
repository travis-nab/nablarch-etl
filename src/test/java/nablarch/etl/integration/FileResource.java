package nablarch.etl.integration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.rules.ExternalResource;

/**
 * ファイルリソース。
 */
public class FileResource extends ExternalResource {

    private File root;
    private File inputFileBasePath;
    private File controlFileBasePath;

    @Override
    protected void before() throws Throwable {
        deleteBasePath();
        createBasePath();
    }

    private void createBasePath() throws IOException {
        root = new File("testdata");
        inputFileBasePath = new File(root, "input");
        controlFileBasePath = new File(root, "sqlloader/control");

        inputFileBasePath.mkdirs();
        new File(root, "output").mkdirs();
        controlFileBasePath.mkdirs();
        new File(root, "sqlloader/output").mkdirs();
    }

    @Override
    protected void after() {
        deleteBasePath();
    }

    private void deleteBasePath() {
        if (root != null) {
            recursiveDelete(root);
        }
    }

    private void recursiveDelete(final File file) {
        final File[] files= file.listFiles();
        if (files != null) {
            for (File each : files) {
                recursiveDelete(each);
            }
        }
        file.delete();
    }

    public void createInputFile(String fileName, String... lines) throws IOException {
        final File file = new File(inputFileBasePath, fileName);
        final BufferedWriter br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
        for (String line : lines) {
            br.write(line);
        }
        br.flush();
        br.close();
    }

    public void createControlFile(String fileName, String tableName, String columnNames) throws IOException {
        final String control = String.format(
                         "OPTIONS (SKIP=1)\n" +
                         "LOAD DATA\n" +
                         "CHARACTERSET UTF8\n" +
                         "TRUNCATE\n" +
                         "PRESERVE BLANKS\n" +
                         "INTO TABLE %s\n" +
                         "FIELDS\n" +
                         "TERMINATED BY ','\n" +
                         "OPTIONALLY ENCLOSED BY '\"'\n" +
                         "(\n" +
                         " line_number RECNUM," +
                         "  %s" +
                         ")", tableName, columnNames);
        final File file = new File(controlFileBasePath, fileName + ".ctl");
        final BufferedWriter br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
        br.write(control);
        br.flush();
        br.close();
    }
}
