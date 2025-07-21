/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.getWorkerNodes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.testng.Assert.assertEquals;

public class TestPrestoNativeDynamicCatalog
{
    static void runCatalogRegister(String endpoint, String jsonPayload, String catalogName)
    {
        try {
            URL url = new URL(endpoint + "/v1/catalog/" + catalogName);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");

            // Send JSON body.
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Read and validate response.
            int responseCode = connection.getResponseCode();
            System.out.println(connection.getResponseMessage());
            InputStream stream = (responseCode >= 200 && responseCode < 300)
                    ? connection.getInputStream()
                    : connection.getErrorStream();

            try (BufferedReader in = new BufferedReader(new InputStreamReader(stream))) {
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                System.out.println("Response Body: " + response);

                if (responseCode != 200) {
                    throw new RuntimeException("Request failed: " + responseCode + " " + response);
                }

                assertEquals(response.toString(), "Registered catalog: " + catalogName);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to call catalog registration", e);
        }
    }

    @Test
    public void testDynamicCatalog() throws Exception
    {
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder().build();
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder().setWorkerCount(1).build();
        queryRunner.getCoordinator().createCatalog("hive2", "hive");
        List<String> endpoints = getWorkerNodes(queryRunner).stream().map(InternalNode::getInternalUri).map(URI::toString).collect(Collectors.toList());

        // Attempt to run a query on a catalog that does not exist on the workers.
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM hive2.tpch.customer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sortedCandidates is null or empty for ModularHashingNodeProvider");

        // Attempt to register a catalog without a connector
        String missingConnectorJsonPayload = "{"
                + "\"invalid\": \"hive\""
                + "}";

        Throwable thrown = catchThrowable(() -> runCatalogRegister(endpoints.get(0), missingConnectorJsonPayload, "hive2"));
        assertThat(thrown.getCause())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Missing configuration property connector.name");

        // Valid config
        String jsonPayload = "{"
                + "\"connector.name\": \"hive\""
                + "}";

        // Attempt to register a duplicate catalog
        thrown = catchThrowable(() -> runCatalogRegister(endpoints.get(0), missingConnectorJsonPayload, "hive"));
        assertThat(thrown.getCause())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Catalog ['hive'] is already present");

        endpoints.forEach((endpoint) -> runCatalogRegister(endpoint, jsonPayload, "hive2"));

        // Leave time for Presto refreshNodes to be called.
        Thread.sleep(15000);

        queryRunner.execute("SELECT * FROM hive2.tpch.customer");

        queryRunner.close();
    }

    @Test(dependsOnMethods = "testDynamicCatalog")
    public void testDynamicCatalogPath() throws Exception
    {
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder().build();
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();

        Path dir = Paths.get("/tmp", TestPrestoNativeDynamicCatalog.class.getSimpleName());
        Files.createDirectories(dir);
        Path tempDirectoryPath = Files.createTempDirectory(dir, "dynamic");
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder().setWorkerCount(1).setDynamicCatalogPath(tempDirectoryPath.toString()).build();
        queryRunner.getCoordinator().createCatalog("hive2", "hive");
        List<String> endpoints = getWorkerNodes(queryRunner).stream().map(InternalNode::getInternalUri).map(URI::toString).collect(Collectors.toList());

        // Attempt to run a query on a catalog that does not exist on the workers.
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM hive2.tpch.customer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sortedCandidates is null or empty for ModularHashingNodeProvider");

        String jsonPayload = "{"
                + "\"connector.name\": \"hive\""
                + "}";

        endpoints.forEach((endpoint) -> runCatalogRegister(endpoint, jsonPayload, "hive2"));

        // Leave time for Presto refreshNodes to be called.
        Thread.sleep(15000);

        queryRunner.execute("SELECT * FROM hive2.tpch.customer");

        queryRunner.close();

        // Simulate a restart by starting a second query runner with the same dynamic-catalog-path
        DistributedQueryRunner queryRunnerCopy = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder().setWorkerCount(1).setDynamicCatalogPath(tempDirectoryPath.toString()).build();
        queryRunnerCopy.getCoordinator().createCatalog("hive2", "hive");

        // Catalog has persisted in dynamic-catalog-path so we shouldn't have to dynamically register it again.
        queryRunnerCopy.execute("SELECT * FROM hive2.tpch.customer");
    }
}
