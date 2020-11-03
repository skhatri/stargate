package io.stargate.graphql.schema.fetchers.ddl;

import graphql.schema.GraphQLSchema;
import io.stargate.graphql.schema.GraphQlTestBase;
import io.stargate.graphql.schema.SchemaFactory;

public abstract class KeyspaceFetcherTestBase extends GraphQlTestBase {

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDdlSchema(persistence, authenticationService);
  }
}
