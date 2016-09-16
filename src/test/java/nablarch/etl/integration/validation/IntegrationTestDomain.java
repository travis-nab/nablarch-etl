package nablarch.etl.integration.validation;

import nablarch.core.validation.ee.SystemChar;

/**
 *
 */
public class IntegrationTestDomain {

    @SystemChar(charsetDef = "数字")
    String userId;

}
