package io.stargate.graphql.schema.fetchers.dml;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService) {
    super(table, nameMapping, persistence, authenticationService);
  }

  @Override
  protected BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore) {
    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    return dataStore
        .queryBuilder()
        .delete()
        .from(table.keyspace(), table.name())
        .where(buildClause(table, environment))
        .ifs(buildConditions(table, environment.getArgument("ifCondition")))
        .ifExists(ifExists)
        .build()
        .bind();
  }
}
