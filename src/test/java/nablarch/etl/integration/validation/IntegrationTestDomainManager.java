package nablarch.etl.integration.validation;

import nablarch.core.validation.ee.DomainManager;

public class IntegrationTestDomainManager implements DomainManager<IntegrationTestDomain> {

    @Override
    public Class<IntegrationTestDomain> getDomainBean() {
        return IntegrationTestDomain.class;
    }
}
