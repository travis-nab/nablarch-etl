package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.sql.DatabaseMetaData;

import mockit.Expectations;
import mockit.Mocked;
import nablarch.core.db.connection.TransactionManagerConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * {@link TruncateSqlGeneratorFactory}のテストクラス。
 */
public class TruncateSqlGeneratorFactoryTest {

    @Mocked
    private TransactionManagerConnection connection;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreate() throws Exception {

        new Expectations() {{
            final DatabaseMetaData metaData = connection.getConnection().getMetaData();
            metaData.getURL();
            result = "jdbc:oracle:thin:@localhost:1521/xe";
        }};

        TruncateSqlGenerator generator = TruncateSqlGeneratorFactory.create(connection);
        assertThat(generator, allOf(
                instanceOf(TruncateSqlGenerator.class),
                not(instanceOf(Db2TruncateSqlGenerator.class))));
    }

    @Test
    public void testCreate_db2() throws Exception {

        new Expectations() {{
            final DatabaseMetaData metaData = connection.getConnection().getMetaData();
            metaData.getURL();
            result = "jdbc:db2://localhost:50000/sample";
        }};

        TruncateSqlGenerator generator = TruncateSqlGeneratorFactory.create(connection);
        assertThat(generator, instanceOf(Db2TruncateSqlGenerator.class));
    }

    @Test
    public void testCreate_url_null() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(is("failed to get connection url."));

        new Expectations() {{
            final DatabaseMetaData metaData = connection.getConnection().getMetaData();
            metaData.getURL();
            result = null;
        }};

        TruncateSqlGeneratorFactory.create(connection);
    }
}