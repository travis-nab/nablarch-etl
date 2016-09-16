package nablarch.etl;

import nablarch.core.validation.ee.Digits;
import nablarch.core.validation.ee.DomainManager;
import nablarch.core.validation.ee.Length;
import nablarch.core.validation.ee.SystemChar;

/**
 * ドメインの定義
 */
public class ValidationDomain {

    @Length(max = 5)
    @SystemChar(charsetDef = "ひらがな")
    String name;

    @Digits(integer = 2, fraction = 0)
    String age;

    public static class ValidationDomainManager implements DomainManager<ValidationDomain> {

        @Override
        public Class<ValidationDomain> getDomainBean() {
            return ValidationDomain.class;
        }
    }
}
