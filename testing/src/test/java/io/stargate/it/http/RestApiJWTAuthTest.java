package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.models.AuthProviderResponse;
import io.stargate.it.http.models.KeycloakCredential;
import io.stargate.it.http.models.KeycloakUser;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.web.models.Changeset;
import io.stargate.web.models.ColumnModel;
import io.stargate.web.models.Filter;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.Keyspace;
import io.stargate.web.models.Query;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.RowAdd;
import io.stargate.web.models.RowResponse;
import io.stargate.web.models.RowUpdate;
import io.stargate.web.models.Rows;
import io.stargate.web.models.RowsResponse;
import io.stargate.web.models.SuccessResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.Headers.Builder;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'web_user' WITH PASSWORD = 'web_user' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS store.shopping_cart (userid text, item_count int, last_update_timestamp timestamp, PRIMARY KEY (userid, last_update_timestamp));",
      "INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(toDate(now())))",
      "INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(toDate(now())))",
    })
public class RestApiJWTAuthTest extends BaseOsgiIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(RestApiJWTAuthTest.class);

  private String keyspaceName = "store";
  private String tableName = "shopping_cart";

  private static String authToken;
  private String host;
  private static String keycloakHost;
  private static GenericContainer keycloakContainer;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) throws IOException {
    initKeycloakContainer();

    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthJwtService");
    builder.putSystemProperties(
        "stargate.auth.jwt_provider_url",
        String.format("%s/auth/realms/stargate/protocol/openid-connect/certs", keycloakHost));
  }

  @AfterAll
  public static void teardown() {
    keycloakContainer.stop();
  }

  private static void initKeycloakContainer() throws IOException {
    int keycloakPort = 4444;
    keycloakContainer =
        new GenericContainer("quay.io/keycloak/keycloak:11.0.2")
            .withExposedPorts(keycloakPort)
            .withEnv("KEYCLOAK_USER", "admin")
            .withEnv("KEYCLOAK_PASSWORD", "admin")
            .withEnv("KEYCLOAK_IMPORT", "/tmp/realm.json")
            .withClasspathResourceMapping(
                "stargate-realm.json", "/tmp/realm.json", BindMode.READ_ONLY)
            .withCommand("-Djboss.http.port=" + keycloakPort)
            .waitingFor(Wait.forHttp("/auth/realms/master"));

    keycloakContainer.start();

    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger).withPrefix("keycloak");
    Map<String, String> mdcCopy = MDC.getCopyOfContextMap();
    if (mdcCopy != null) {
      logConsumer.withMdc(mdcCopy);
    }

    keycloakContainer.followOutput(logConsumer);

    keycloakHost =
        "http://"
            + keycloakContainer.getContainerIpAddress()
            + ":"
            + +keycloakContainer.getMappedPort(keycloakPort);

    setupKeycloakUsers();
  }

  private static void setupKeycloakUsers() throws IOException {
    String body =
        RestUtils.generateJwt(
            keycloakHost + "/auth/realms/master/protocol/openid-connect/token",
            "admin",
            "admin",
            "admin-cli",
            HttpStatus.SC_OK);

    AuthProviderResponse authTokenResponse =
        objectMapper.readValue(body, AuthProviderResponse.class);
    String adminAuthToken = authTokenResponse.getAccessToken();
    assertThat(adminAuthToken).isNotNull();

    KeycloakCredential keycloakCredential =
        new KeycloakCredential("password", "testuser1", "false");
    Map<String, List<String>> attributes = new HashMap<>();
    attributes.put("userid", Collections.singletonList("9876"));
    attributes.put("role", Collections.singletonList("web_user"));
    KeycloakUser keycloakUser =
        new KeycloakUser(
            "testuser1", true, true, attributes, Collections.singletonList(keycloakCredential));

    RestUtils.postWithHeader(
        new Builder().add("Authorization", "bearer " + adminAuthToken).build(),
        keycloakHost + "/auth/admin/realms/stargate/users",
        objectMapper.writeValueAsString(keycloakUser),
        HttpStatus.SC_CREATED);
  }

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    //    host = "http://127.0.0.1";

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.generateJwt(
            keycloakHost + "/auth/realms/stargate/protocol/openid-connect/token",
            "testuser1",
            "testuser1",
            "user-service",
            HttpStatus.SC_OK);

    AuthProviderResponse authTokenResponse =
        objectMapper.readValue(body, AuthProviderResponse.class);
    authToken = authTokenResponse.getAccessToken();
    assertThat(authToken).isNotNull();
  }

  @Test
  public void getKeyspacesv2() throws IOException {
    String body =
        RestUtils.get(
            authToken, String.format("%s:8082/v2/schemas/keyspaces", host), HttpStatus.SC_OK);

    ResponseWrapper response = objectMapper.readValue(body, ResponseWrapper.class);
    List<Keyspace> keyspaces =
        objectMapper.convertValue(response.getData(), new TypeReference<List<Keyspace>>() {});
    assertThat(keyspaces)
        .anySatisfy(
            value ->
                assertThat(value).isEqualToComparingFieldByField(new Keyspace("system", null)));
  }

  @Test
  public void getKeyspacesV1() throws IOException {
    String body =
        RestUtils.get(authToken, String.format("%s:8082/v1/keyspaces", host), HttpStatus.SC_OK);

    List<String> keyspaces = objectMapper.readValue(body, new TypeReference<List<String>>() {});
    assertThat(keyspaces)
        .containsAnyOf(
            "system", "system_auth", "system_distributed", "system_schema", "system_traces");
  }

  @Test
  public void getAllRowsV1() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspaceName, tableName),
            HttpStatus.SC_OK);

    Rows rows = objectMapper.readValue(body, new TypeReference<Rows>() {});
    assertThat(rows.getCount()).isGreaterThan(0);

    for (Map<String, Object> row : rows.getRows()) {
      assertThat(row.get("userid")).isEqualTo("9876");
      assertThat((int) row.get("item_count")).isGreaterThan(0);
      assertThat(row.get("last_update_timestamp")).isNotNull();
    }
  }

  @Test
  public void getRowV1() throws IOException {
    String body = getRowV1(tableName, "");

    Rows rows = objectMapper.readValue(body, new TypeReference<Rows>() {});
    assertThat(rows.getCount()).isGreaterThan(0);

    for (Map<String, Object> row : rows.getRows()) {
      assertThat(row.get("userid")).isEqualTo("9876");
      assertThat((int) row.get("item_count")).isGreaterThan(0);
      assertThat(row.get("last_update_timestamp")).isNotNull();
    }
  }

  @Test
  public void getRowV1NotAuthorized() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows/1234", host, keyspaceName, tableName),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void getRowsV2() throws IOException {
    String body =
        RestUtils.get(
            authToken,
            String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "9876"),
            HttpStatus.SC_OK);

    GetResponseWrapper getResponseWrapper = objectMapper.readValue(body, GetResponseWrapper.class);
    List<Map<String, Object>> data =
        objectMapper.convertValue(
            getResponseWrapper.getData(), new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("userid")).isEqualTo("9876");
    assertThat(data.get(0).get("item_count")).isEqualTo(2);
    assertThat(data.get(0).get("last_update_timestamp")).isNotNull();
  }

  @Test
  public void getRowsV2NotAuthorized() throws IOException {
    RestUtils.get(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, "1234"),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void updateRowV1() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();

    addRowV1(rowIdentifier, updateTimestamp, "10");

    RowResponse rowResponse =
        objectMapper.readValue(
            getRowV1(tableName, rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8")),
            new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("userid")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("item_count")).isEqualTo(10);
    assertThat(rowResponse.getRows().get(0).get("last_update_timestamp")).isNotNull();

    RowUpdate rowUpdate = new RowUpdate();
    Changeset itemCountChange = new Changeset();
    itemCountChange.setColumn("item_count");
    itemCountChange.setValue("8");
    rowUpdate.setChangeset(Collections.singletonList(itemCountChange));

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
                host,
                keyspaceName,
                tableName,
                rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8")),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    SuccessResponse successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();

    rowResponse =
        objectMapper.readValue(
            getRowV1(tableName, rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8")),
            new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("userid")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("item_count")).isEqualTo(8);
  }

  @Test
  public void updateRowV1NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    RowUpdate rowUpdate = new RowUpdate();
    Changeset itemCountChange = new Changeset();
    itemCountChange.setColumn("item_count");
    itemCountChange.setValue("8");
    rowUpdate.setChangeset(Collections.singletonList(itemCountChange));

    RestUtils.put(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
            host,
            keyspaceName,
            tableName,
            rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8")),
        objectMapper.writeValueAsString(rowUpdate),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void addRowV1NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    List<ColumnModel> columns = new ArrayList<>();

    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("userid");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel itemCountColumn = new ColumnModel();
    itemCountColumn.setName("item_count");
    itemCountColumn.setValue("0");
    columns.add(itemCountColumn);

    ColumnModel updateTimestampColumn = new ColumnModel();
    updateTimestampColumn.setName("last_update_timestamp");
    updateTimestampColumn.setValue(updateTimestamp);
    columns.add(updateTimestampColumn);

    RowAdd rowAdd = new RowAdd();
    rowAdd.setColumns(columns);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(rowAdd),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void queryRowV1() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();

    addRowV1(rowIdentifier, updateTimestamp, "20");

    Query query = new Query();
    query.setColumnNames(Arrays.asList("userid", "item_count", "last_update_timestamp"));

    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("userid");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList(rowIdentifier));
    filters.add(filter);

    filter = new Filter();
    filter.setColumnName("last_update_timestamp");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList(updateTimestamp));
    filters.add(filter);

    query.setFilters(filters);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(query),
            HttpStatus.SC_OK);

    RowResponse rowResponse = objectMapper.readValue(body, new TypeReference<RowResponse>() {});
    assertThat(rowResponse.getCount()).isEqualTo(1);
    assertThat(rowResponse.getRows().get(0).get("userid")).isEqualTo(rowIdentifier);
    assertThat(rowResponse.getRows().get(0).get("item_count")).isEqualTo(20);
    assertThat(rowResponse.getRows().get(0).get("last_update_timestamp"))
        .isEqualTo(updateTimestamp);
  }

  @Test
  public void queryRowV1NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    Query query = new Query();
    query.setColumnNames(Arrays.asList("userid", "item_count", "last_update_timestamp"));

    List<Filter> filters = new ArrayList<>();

    Filter filter = new Filter();
    filter.setColumnName("userid");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList(rowIdentifier));
    filters.add(filter);

    filter = new Filter();
    filter.setColumnName("last_update_timestamp");
    filter.setOperator(Filter.Operator.eq);
    filter.setValue(Collections.singletonList(updateTimestamp));
    filters.add(filter);

    query.setFilters(filters);

    RestUtils.post(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/query", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(query),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void deleteRowV1() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();

    addRowV1(rowIdentifier, updateTimestamp, "30");

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
            host,
            keyspaceName,
            tableName,
            (rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8"))),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void deleteRowV1NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
            host,
            keyspaceName,
            tableName,
            (rowIdentifier + ";" + URLEncoder.encode(updateTimestamp, "UTF-8"))),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void updateRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "88");

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("item_count", "27");

    String body =
        RestUtils.put(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s/%s/%s",
                host,
                keyspaceName,
                tableName,
                rowIdentifier,
                URLEncoder.encode(updateTimestamp, "UTF-8")),
            objectMapper.writeValueAsString(rowUpdate),
            HttpStatus.SC_OK);

    ResponseWrapper responseWrapper = objectMapper.readValue(body, ResponseWrapper.class);
    Map<String, String> data = objectMapper.convertValue(responseWrapper.getData(), Map.class);

    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Test
  public void updateRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("userid", rowIdentifier);
    rowUpdate.put("item_count", "27");

    RestUtils.put(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s", host, keyspaceName, tableName, rowIdentifier),
        objectMapper.writeValueAsString(rowUpdate),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void addRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();
    Map<String, String> row = new HashMap<>();
    row.put("userid", rowIdentifier);
    row.put("item_count", "0");
    row.put("last_update_timestamp", updateTimestamp);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void queryRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "99");

    String whereClause =
        String.format(
            "{\"userid\":{\"$eq\":\"%s\"},\"last_update_timestamp\":{\"$eq\":\"%s\"}}",
            rowIdentifier, updateTimestamp);
    String body =
        RestUtils.get(
            authToken,
            String.format(
                "%s:8082/v2/keyspaces/%s/%s?where=%s&raw=true",
                host, keyspaceName, tableName, whereClause),
            HttpStatus.SC_OK);

    List<Map<String, Object>> data =
        objectMapper.readValue(body, new TypeReference<List<Map<String, Object>>>() {});
    assertThat(data.get(0).get("userid")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("item_count")).isEqualTo(99);
  }

  @Test
  public void queryRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    String whereClause =
        String.format(
            "{\"userid\":{\"$eq\":\"%s\"},\"last_update_timestamp\":{\"$eq\":\"%s\"}}",
            rowIdentifier, updateTimestamp);
    RestUtils.get(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s?where=%s&raw=true",
            host, keyspaceName, tableName, whereClause),
        HttpStatus.SC_UNAUTHORIZED);
  }

  @Test
  public void deleteRowV2() throws IOException {
    String rowIdentifier = "9876";
    String updateTimestamp = Instant.now().toString();
    addRowV2(rowIdentifier, updateTimestamp, "88");

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s/%s",
            host,
            keyspaceName,
            tableName,
            rowIdentifier,
            URLEncoder.encode(updateTimestamp, "UTF-8")),
        HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void deleteRowV2NotAuthorized() throws IOException {
    String rowIdentifier = "1234";
    String updateTimestamp = Instant.now().toString();

    RestUtils.delete(
        authToken,
        String.format(
            "%s:8082/v2/keyspaces/%s/%s/%s/%s",
            host,
            keyspaceName,
            tableName,
            rowIdentifier,
            URLEncoder.encode(updateTimestamp, "UTF-8")),
        HttpStatus.SC_UNAUTHORIZED);
  }

  private void addRowV2(String rowIdentifier, String updateTimestamp, String itemCount)
      throws IOException {
    Map<String, String> row = new HashMap<>();
    row.put("userid", rowIdentifier);
    row.put("item_count", itemCount);
    row.put("last_update_timestamp", updateTimestamp);

    RestUtils.post(
        authToken,
        String.format("%s:8082/v2/keyspaces/%s/%s", host, keyspaceName, tableName),
        objectMapper.writeValueAsString(row),
        HttpStatus.SC_CREATED);
  }

  private String getRowV1(String tableName, String rowIdentifier) throws IOException {
    return RestUtils.get(
        authToken,
        String.format(
            "%s:8082/v1/keyspaces/%s/tables/%s/rows/%s",
            host, keyspaceName, tableName, rowIdentifier),
        HttpStatus.SC_OK);
  }

  private void addRowV1(String rowIdentifier, String updateTimestamp, String itemCount)
      throws IOException {
    List<ColumnModel> columns = new ArrayList<>();

    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("userid");
    idColumn.setValue(rowIdentifier);
    columns.add(idColumn);

    ColumnModel itemCountColumn = new ColumnModel();
    itemCountColumn.setName("item_count");
    itemCountColumn.setValue(itemCount);
    columns.add(itemCountColumn);

    ColumnModel updateTimestampColumn = new ColumnModel();
    updateTimestampColumn.setName("last_update_timestamp");
    updateTimestampColumn.setValue(updateTimestamp);
    columns.add(updateTimestampColumn);

    RowAdd rowAdd = new RowAdd();
    rowAdd.setColumns(columns);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, keyspaceName, tableName),
            objectMapper.writeValueAsString(rowAdd),
            HttpStatus.SC_CREATED);

    RowsResponse rowsResponse = objectMapper.readValue(body, new TypeReference<RowsResponse>() {});
    assertThat(rowsResponse.getRowsModified()).isEqualTo(1);
    assertThat(rowsResponse.getSuccess()).isTrue();
  }
}
