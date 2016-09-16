package nablarch.etl;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@link Range}のテスト。
 */
public class RangeTest {
    /**
     * 一定間隔の範囲が正しく計算されること。
     */
    @Test
    public void testNext() {

        Range sut = new Range(3, 10L);

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(1L));
        assertThat(sut.to, is(3L));

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(4L));
        assertThat(sut.to, is(6L));

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(7L));
        assertThat(sut.to, is(9L));

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(10L));
        assertThat(sut.to, is(10L));

        assertThat(sut.next(), is(false)); // 最大値に達したので以降はfalse
        assertThat(sut.next(), is(false));
        assertThat(sut.next(), is(false));
    }
    /**
     * いろんな間隔と最大値の組み合わせでも、一定間隔の範囲が正しく計算されること。
     */
    @Test
    public void testNextWithSomeIntervalAndMax() {

        // interval == max
        Range sut = new Range(3, 3L);

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(1L));
        assertThat(sut.to, is(3L));

        assertThat(sut.next(), is(false));

        // interval > max
        sut = new Range(3, 1L);

        assertThat(sut.next(), is(true));
        assertThat(sut.from, is(1L));
        assertThat(sut.to, is(1L));

        assertThat(sut.next(), is(false));

        // max == 0
        sut = new Range(3, 0L);

        assertThat(sut.next(), is(false));
    }
}
