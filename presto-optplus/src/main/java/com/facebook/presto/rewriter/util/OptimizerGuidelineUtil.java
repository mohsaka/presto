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
package com.facebook.presto.rewriter.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.ibm.db2.jcc.DB2ConnectionPoolDataSource;
import com.ibm.db2.jcc.dbpool.DB2ConnectionPool;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.rewriter.util.GovernanceErrorCode.DB2_CONNECTION_FAILURE;
import static com.facebook.presto.rewriter.util.GovernanceErrorCode.DB2_MISCONFIGURATION;
import static com.google.common.collect.Maps.fromProperties;

public class OptimizerGuidelineUtil
{
    private static final Logger log = Logger.get(OptimizerGuidelineUtil.class);
    public static final String OAAS_URL = "wxd_optimizer_url";
    private static final String TRUSTSTORE_KEY = "http-server.https.truststore.key";
    static DB2ConnectionPool optimizerConnPool;
    private static DocumentBuilderFactory dbf = createDbf();
    private static final File CONFIGURATION_FILE = Paths.get(System.getProperty("config", "defaultConfigPath")).toFile();

    private OptimizerGuidelineUtil()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static DocumentBuilderFactory createDbf()
    {
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            // Use this if the JAXP parser accepts it
            documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            // AND add the following to enforce limits on what the parser is allowed to do
            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            documentBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            documentBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            documentBuilderFactory.setXIncludeAware(false);
            documentBuilderFactory.setExpandEntityReferences(false);
            return documentBuilderFactory;
        }
        catch (ParserConfigurationException e) {
            log.error(e, "OPT+ Exception in creating DocumentBuilderFactory for optimizer guideline");
        }
        return null;
    }

    public static DocumentBuilderFactory getDbf()
    {
        return dbf;
    }

    public static void initOptimizerConnectionPool(String db2Url, boolean showOptimizedQuery)
    {
        String oaasUrl = Optional.ofNullable(System.getenv(OAAS_URL)).orElse(db2Url);
        DB2ConnectionPoolDataSource dataSource = new DB2ConnectionPoolDataSource();
        try {
            String connectionString = oaasUrl.replaceAll("^\"|\"$", "");
            if (showOptimizedQuery) {
                log.debug("OPT+ connect pool connection string %s", connectionString
                        .replaceFirst("password=[^;]+", "password=[REDACTED]"));
            }
            String withoutPrefix = connectionString.substring("jdbc:db2://".length());
            String[] parts = withoutPrefix.split(":");
            String host = parts[0];
            String[] hostAndDb = parts[1].split("/");
            int port = Integer.parseInt(hostAndDb[0]);
            String database = hostAndDb[1];

            Map<String, String> props = parseJdbcConnectionString(parts[2]);
            //check if this can be added as config
            dataSource.setMaxStatements(30);
            dataSource.setServerName(host);
            dataSource.setPortNumber(port);
            dataSource.setDatabaseName(database);
            log.debug("Setting host to : %s, port to : %d, database to : %s", host, port, database);

            dataSource.setDriverType(4);
            if (props.containsKey("user")) {
                String user = props.get("user");
                log.debug("User is set to : %s", user);
                dataSource.setUser(user);
            }
            if (props.containsKey("password")) {
                String password = props.get("password");
                log.debug("Password is set");
                if (password.isBlank()) {
                    log.warn("db2 password is set as empty");
                }
                dataSource.setPassword(password);
            }
            if (props.containsKey("sslConnection")) {
                Boolean sslConnection = Boolean.valueOf(props.get("sslConnection"));
                log.debug("SSL Connection is set to : %s", sslConnection);
                dataSource.setSslConnection(sslConnection);
            }
            if (props.containsKey("securityMechanism")) {
                Short securityMechanism = Short.valueOf(props.get("securityMechanism"));
                log.debug("Security Mechanism is set to : %s", securityMechanism);
                dataSource.setSecurityMechanism(securityMechanism);
            }
            if (props.containsKey("sslCertLocation")) {
                String sslCertLocation = props.get("sslCertLocation");
                log.debug("SSL Certificate Location is set to : %s", sslCertLocation);
                dataSource.setSslCertLocation(sslCertLocation);
            }
            if (props.containsKey("sslTrustStoreLocation")) {
                // For Saas we get truststore in jks
                dataSource.setSslTrustStoreType("JKS");
                String sslTrustStoreLocation = props.get("sslTrustStoreLocation");
                log.debug("SSL Trust Store Location is set to : %s", sslTrustStoreLocation);
                dataSource.setSslTrustStoreLocation(sslTrustStoreLocation);
                Map<String, String> configProperties = loadProperties(CONFIGURATION_FILE);
                String trustStorePassword = configProperties.get(TRUSTSTORE_KEY);
                if (trustStorePassword.isBlank()) {
                    log.warn("SSL Trust Store key is set as empty");
                }
                dataSource.setSslTrustStorePassword(trustStorePassword);
            }
            DB2ConnectionPool pool = new DB2ConnectionPool(dataSource);
            log.debug("Optimizer connection pool value before init : " + OptimizerGuidelineUtil.optimizerConnPool);
            //check if this can be added as config
            pool.setMaxPoolSize(30);
            OptimizerGuidelineUtil.optimizerConnPool = pool;
            log.debug("Optimizer connection pool value after init : " + OptimizerGuidelineUtil.optimizerConnPool);
        }
        catch (SQLException | IOException e) {
            OptimizerGuidelineUtil.optimizerConnPool = null;
            log.error("Failed to initialize wxd query optimizer connection pool %s", e.toString());
            throw new PrestoException(DB2_CONNECTION_FAILURE, "Failed to initialize wxd query optimizer connection pool", e);
        }
    }

    public static DB2ConnectionPool getOptimizerConnectionPool()
    {
        if (optimizerConnPool == null) {
            log.error("Optimizer connection pool is null ");
        }
        log.info("OPT+ connection pool stats - %s", optimizerConnPool.getPoolStats());
        return optimizerConnPool;
    }

    public static Connection getConnection()
            throws SQLException
    {
        if (optimizerConnPool == null) {
            log.error("Optimizer connection pool is null ");
        }
        log.info("OPT+ connection pool stats - %s", optimizerConnPool.getPoolStats());
        return optimizerConnPool.getConnection();
    }

    private static Map<String, String> parseJdbcConnectionString(String jdbcConnectionString)
    {
        Map<String, String> map = new HashMap<>();
        // Regular expression pattern to extract key-value pairs
        String[] pairs = jdbcConnectionString.split(";");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length == 2) {
                map.put(keyValue[0], keyValue[1]);
            }
        }
        return map;
    }

    public static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        catch (Exception e) {
            return ImmutableMap.of();
        }
        return fromProperties(properties);
    }

    public static void validate(String dbUrl)
            throws Exception
    {
        if (dbUrl.contains("user:") || dbUrl.contains("password:")) {
            throw new PrestoException(DB2_MISCONFIGURATION, "Connection URL validation check should not contain user or password: ");
        }
    }

    public static String runOptGuideline(
            String query,
            String coordinatorHost,
            String coordinatorPort,
            String user,
            String pass,
            boolean enableSSL,
            String trustStorePath,
            String trustStorePassword,
            boolean showOptimizedQuery)
    {
        long startTimeTotal = System.currentTimeMillis();
        String prestoServer = System.getenv("PRESTO_SERVER");
        Connection conn = null;
        String dbUrl;
        if (prestoServer != null && !"".equalsIgnoreCase(prestoServer)) {
            dbUrl = "jdbc:presto://" + prestoServer;
            if (showOptimizedQuery) {
                log.debug("OPT+ optguidelines URL - %s", dbUrl);
            }
        }
        else {
            dbUrl = "jdbc:presto://" + coordinatorHost + ":" + coordinatorPort;
            if (showOptimizedQuery) {
                log.debug("OPT+ optguidelines URL - %s", dbUrl);
            }
        }
        try {
            // Class.forName("com.facebook.presto.jdbc.PrestoDriver");
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", pass);
            if (enableSSL) {
                properties.setProperty("SSL", "true");
                properties.setProperty("SSLTrustStorePath", trustStorePath);
                Map<String, String> configProperties = loadProperties(CONFIGURATION_FILE);
                properties.setProperty("SSLTrustStorePassword", Optional.ofNullable(configProperties.get(TRUSTSTORE_KEY)).orElse(trustStorePassword));
            }
            validate(dbUrl);
            conn = DriverManager.getConnection(dbUrl, properties);
            String optGuideLines = null;
            String execqryString;
            String[] paramMarkers = null;
            StringBuilder sb = new StringBuilder(query);

            optGuideLines = sb.substring(query.indexOf("<OPTGUIDELINES>"), sb.indexOf("</OPTGUIDELINES>") + 16);
            query = sb.delete(query.indexOf("/*<OPTGUIDELINES>"), query.indexOf("</OPTGUIDELINES>*/") + 18).toString();
            // OptGuideLines parsing
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document xdoc = builder.parse(new InputSource(new StringReader(optGuideLines)));

            NodeList nodeList;
            Node node;
            Element element;
            String fqries = null;

            // extract execquery "execute qry using ..." to get all *named* paramMarkers for
            // query
            execqryString = ((Element) xdoc.getElementsByTagName("queryexec").item(0)).getTextContent().trim();
            if (showOptimizedQuery) {
                log.debug("OPT+ optguidelines execqry_string  : %s ", execqryString);
            }
            paramMarkers = execqryString.split(",");
            paramMarkers[0] = paramMarkers[0].substring(paramMarkers[0].lastIndexOf(' ') + 1);
            for (int i = 0; i < paramMarkers.length; i++) {
                paramMarkers[i] = paramMarkers[i].trim();
            }
            paramMarkers[paramMarkers.length - 1] = (paramMarkers[paramMarkers.length - 1]).split(";")[0];

            // extract list of filter queries
            nodeList = xdoc.getElementsByTagName("filterparmqueries").item(0).getChildNodes();
            for (int i = 0; i < nodeList.getLength(); i++) {
                node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    element = (Element) node;
                    if (element.getNodeName().equals("query")) {
                        fqries = element.getTextContent().trim();
                    }
                }
            }
            // execute filter queries to retrieve filter values and store in Hash Map
            Statement fqStmt = conn.createStatement();
            ResultSet frs;
            ResultSetMetaData frsm;
            ArrayList<String> result = new ArrayList<String>();
            HashMap<String, String> results = new HashMap<String, String>();
            fqries = "--optguideline\n" + fqries;
            if (showOptimizedQuery) {
                log.debug("OPT+ optguidelines intermediate query  : %s", fqries);
            }
            long startTime = System.currentTimeMillis();
            frs = fqStmt.executeQuery(fqries);
            frsm = frs.getMetaData();
            while (frs.next()) {
                for (int i = 1; i <= frsm.getColumnCount(); i++) {
                    // add values if no value is there yet, and keep last non null value
                    if (!results.containsKey(frsm.getColumnLabel(i)) || !(frs.getString(i) == null)) {
                        results.put(frsm.getColumnLabel(i), frs.getString(i));
                    }
                }
            }
            log.info("OPT+ optguidelines Time taken for presto to execute secondary query %d millisec", (System.currentTimeMillis() - startTime));
            // "fill" filter values into parameter markers of prepared query
            for (int i = 0; i < paramMarkers.length; i++) {
                String replaceMarker = (results.get(paramMarkers[i]) == null) ? "NULL" : results.get(paramMarkers[i]).toString();
                if (showOptimizedQuery) {
                    log.debug("OPT+ optguidelines replace %d Marker with : %s", i, replaceMarker);
                }
                query = query.replaceFirst("\\?", replaceMarker);
            }
            if (showOptimizedQuery) {
                log.debug("OPT+ optguidelines updated query is : %s", query);
            }
            conn.close();
            log.info("OPT+ optguidelines Total Time taken for optguidlines %d millisec", (System.currentTimeMillis() - startTimeTotal));
            return query;
        }
        catch (Exception e) {
            log.error("Exception in running optimizer guideline");
            if (showOptimizedQuery) {
                log.error(e, "Error");
            }
            return null;
        }
    }
}
