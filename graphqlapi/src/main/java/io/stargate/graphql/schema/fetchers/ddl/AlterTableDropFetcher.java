/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.fetchers.ddl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableDropColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.List;

public class AlterTableDropFetcher extends DdlQueryFetcher {

  public AlterTableDropFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment)
      throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");

    HttpAwareContext httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();
    authorizationService.authorizeSchemaWrite(token, keyspaceName, tableName, Scope.ALTER);

    AlterTableStart start =
        SchemaBuilder.alterTable(
            CqlIdentifier.fromInternal(keyspaceName), CqlIdentifier.fromInternal(tableName));

    List<String> toDrop = dataFetchingEnvironment.getArgument("toDrop");
    if (toDrop.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("toDrop must contain at least one element");
    }
    AlterTableDropColumnEnd table = null;
    for (String column : toDrop) {
      if (table != null) {
        table = table.dropColumn(CqlIdentifier.fromInternal(column));
      } else {
        table = start.dropColumn(CqlIdentifier.fromInternal(column));
      }
    }
    return table.build().getQuery();
  }
}
