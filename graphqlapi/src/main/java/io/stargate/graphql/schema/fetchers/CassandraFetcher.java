package io.stargate.graphql.schema.fetchers;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Base class for fetchers that access the Cassandra backend. It also handles authentication. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {

  protected final Persistence persistence;
  protected final AuthenticationService authenticationService;

  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final Parameters DEFAULT_PARAMETERS =
      Parameters.builder()
          .pageSize(DEFAULT_PAGE_SIZE)
          .consistencyLevel(DEFAULT_CONSISTENCY)
          .serialConsistencyLevel(DEFAULT_SERIAL_CONSISTENCY)
          .build();

  public CassandraFetcher(Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  public final ResultT get(DataFetchingEnvironment environment) throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();

    String token = httpAwareContext.getAuthToken();
    StoredCredentials storedCredentials = authenticationService.validateToken(token);

    Parameters parameters;
    Map<String, Object> options = environment.getArgument("options");
    if (options != null) {
      ImmutableParameters.Builder builder = Parameters.builder().from(DEFAULT_PARAMETERS);

      Object consistency = options.get("consistency");
      if (consistency != null) {
        builder.consistencyLevel(ConsistencyLevel.valueOf((String) consistency));
      }

      Object serialConsistency = options.get("serialConsistency");
      if (serialConsistency != null) {
        builder.serialConsistencyLevel(ConsistencyLevel.valueOf((String) serialConsistency));
      }

      Object pageSize = options.get("pageSize");
      if (pageSize != null) {
        builder.pageSize((Integer) pageSize);
      }

      Object pageState = options.get("pageState");
      if (pageState != null) {
        builder.pagingState(ByteBuffer.wrap(Base64.getDecoder().decode((String) pageState)));
      }

      parameters = builder.build();
    } else {
      parameters = DEFAULT_PARAMETERS;
    }

    DataStore dataStore =
        DataStore.create(persistence, storedCredentials.getRoleName(), parameters);
    return get(environment, dataStore);
  }

  protected abstract ResultT get(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception;
}
