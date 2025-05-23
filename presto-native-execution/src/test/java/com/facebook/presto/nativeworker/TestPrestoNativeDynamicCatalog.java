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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ExternalWorker;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.getWorkerNodes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestPrestoNativeDynamicCatalog
{
    static void writeNewCatalog(ExternalWorker worker) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(worker.workingPath + "/catalog/hive2.properties"))) {
            writer.write("connector.name=hive");
        } catch (IOException e) {
            throw new RuntimeException("Unable to write to file");
        }
    }

    static void runCatalogRegister(String endpoint) {
        try {
            String catalogName = "hive2";
            URL url = new URL(endpoint + "/v1/catalog/register?catalog=" + catalogName);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Accept", "text/plain");

            int responseCode = connection.getResponseCode();
            assertEquals(responseCode, 200);

            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                assertEquals(response.toString(), "Successfully registered catalog: " + catalogName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to call catalog registration");
        }
    }

    @Test
    public void testDynamicCatalog() throws Exception
    {
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(false);
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunner(false, false, false, false);
        List<ExternalWorker> workers = queryRunner.getExternalWorkers();
        List<String> endpoints = getWorkerNodes(queryRunner).stream().map(InternalNode::getInternalUri).map(URI::toString).collect(Collectors.toList());

        // Attempt to run a query on a catalog that does not exist on the workers.
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM hive2.tpch.customer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sortedCandidates is null or empty for ModularHashingNodeProvider");

        // Add hive2 catalog to each worker nodes' catalog directory.
        workers.forEach(TestPrestoNativeDynamicCatalog::writeNewCatalog);

        // Attempt to run a query on the new catalog we created but have not run the dynamic registration on.
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM hive2.tpch.customer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sortedCandidates is null or empty for ModularHashingNodeProvider");

        endpoints.forEach(TestPrestoNativeDynamicCatalog::runCatalogRegister);

        // Leave time for Presto refreshNodes to be called.
        Thread.sleep(10000);

        queryRunner.execute("SELECT * FROM hive2.tpch.customer");

        queryRunner.close();
    }
}
