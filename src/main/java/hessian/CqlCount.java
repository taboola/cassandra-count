/*
 * Copyright 2015 Brian Hess
 *
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

package hessian;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CqlCount {
    private final String version = "0.0.6";
    private final String minToken = "-9223372036854775808";
    private final String maxToken = "9223372036854775807";
    private String host = null;
    private int port = 9042;
    private String username = null;
    private String password = null;
    private String truststorePath = null;
    private String truststorePwd = null;
    private String keystorePath = null;
    private String keystorePwd = null;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    private Cluster cluster = null;
    private Session session = null;
    private String beginTokenString = null;
    private String endTokenString = null;
    private int numSplits = 0;
    private int numFutures = 100;
    private int readTimeout = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;
    private int connectTimeout = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
    private String keyspaceName = null;
    private String tableName = null;
    private long splitSize = 2 * 1024 * 1024;
    private int debug = 0;

    private List<Token> beginTokens;
    private List<Token> endTokens;

    // -host recob101.us.taboolasyndication.com -keyspace trc -table recommendationsv3 -readTimeout 1200000 -connectTimeout 500000 -debug 2 -numSplits 143370
    public static void main(String[] args)
            throws IOException,
            KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException,
            CertificateException, KeyManagementException {
        CqlCount cc = new CqlCount();
        boolean success = cc.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    private String usage() {
        StringBuilder usage = new StringBuilder("version: ").append(version).append("\n");
        usage.append("Usage: -host <ipaddress> -keyspace <ks> -table <tableName> [OPTIONS]\n");
        usage.append("OPTIONS:\n");
        usage.append("  -configFile <filename>         File with configuration options [none]\n");
        usage.append("  -port <portNumber>             CQL Port Number [9042]\n");
        usage.append("  -user <username>               Cassandra username [none]\n");
        usage.append("  -pw <password>                 Password for user [none]\n");
        usage.append("  -ssl-truststore-path <path>    Path to SSL truststore [none]\n");
        usage.append("  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]\n");
        usage.append("  -ssl-keystore-path <path>      Path to SSL keystore [none]\n");
        usage.append("  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]\n");
        usage.append("  -consistencyLevel <CL>         Consistency level [LOCAL_ONE]\n");
        usage.append("  -beginToken <tokenString>      Begin token [none]\n");
        usage.append("  -endToken <tokenString>        End token [none]\n");
        usage.append("  -numFutures <numfutures>       Number of futures [100]\n");
        usage.append("  -numSplits <numsplits>         Number of total splits (0 for <number of tokens>, -1 for size-related generated splits) [number of tokens]\n");
        usage.append("  -splitSize <splitSize>         Split size in MBs [2]\n");
        usage.append("  -readTimeout <readTimeout>     read timeout in millisecond [12000]\n");
        usage.append("  -connectTimeout <connectTimeout> connect timeout in millisecond [5000]\n");
        usage.append("  -debug <0|1|2>                 Print debug messages [0]\n");
        return usage.toString();
    }

    private boolean validateArgs() {
        if ((null == username) && (null != password)) {
            System.err.println("If you supply the password, you must supply the username");
            return false;
        }
        if ((null != username) && (null == password)) {
            System.err.println("If you supply the username, you must supply the password");
            return false;
        }
        if ((null == truststorePath) && (null != truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-pwd, you must supply the ssl-truststore-path");
            return false;
        }
        if ((null != truststorePath) && (null == truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-path, you must supply the ssl-truststore-pwd");
            return false;
        }
        if ((null == keystorePath) && (null != keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-pwd, you must supply the ssl-keystore-path");
            return false;
        }
        if ((null != keystorePath) && (null == keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-path, you must supply the ssl-keystore-pwd");
            return false;
        }
        File tfile = null;
        if (null != truststorePath) {
            tfile = new File(truststorePath);
            if (!tfile.isFile()) {
                System.err.println("truststore file must be a file");
                return false;
            }
        }
        if (null != keystorePath) {
            tfile = new File(keystorePath);
            if (!tfile.isFile()) {
                System.err.println("keystore file must be a file");
                return false;
            }
        }
        if ((null != beginTokenString) && (null == endTokenString)) {
            System.err.println("If you supply the beginToken then you need to specify the endToken");
            return false;
        }
        if ((null == beginTokenString) && (null != endTokenString)) {
            System.err.println("If you supply the endToken then you need to specify the beginToken");
            return false;
        }
        if (numFutures < 1) {
            System.err.println("numFutures must be positive");
            return false;
        }
        //if (numSplits < 1) {
        //  System.err.println("numSplits must be positive");
        //  return false;
        //}
        if (splitSize < 0) {
            System.err.println("splitSize must be positive");
            return false;
        }

        if ((2 < debug) || (0 > debug)) {
            System.err.println("Debug options are 0, 1, 2 (in increasing verbosity)");
            return false;
        }

        return true;
    }

    private boolean processConfigFile(String fname, Map<String, String> amap)
            throws IOException {
        File cFile = new File(fname);
        if (!cFile.isFile()) {
            System.err.println("Configuration File must be a file");
            return false;
        }

        BufferedReader cReader = new BufferedReader(new FileReader(cFile));
        String line;
        while ((line = cReader.readLine()) != null) {
            String[] fields = line.trim().split("\\s+");
            if (2 != fields.length) {
                System.err.println("Bad line in config file: " + line);
                return false;
            }
            amap.computeIfAbsent(fields[0], k -> fields[1]);
        }
        return true;
    }

    private boolean parseArgs(String[] args)
            throws IOException {
        String tkey;
        if (args.length == 0) {
            System.err.println("No arguments specified");
            return false;
        }
        if (0 != args.length % 2)
            return false;

        Map<String, String> amap = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            amap.put(args[i], args[i + 1]);
        }

        if (null != (tkey = amap.remove("-configFile")))
            if (!processConfigFile(tkey, amap))
                return false;

        host = amap.remove("-host");
        if (null == host) { // host is required
            System.err.println("Must provide a host");
            return false;
        }

        keyspaceName = amap.remove("-keyspace");
        if (null == keyspaceName) { // keyspace is required
            System.err.println("Must provide a keyspace name");
            return false;
        }

        tableName = amap.remove("-table");
        if (null == tableName) { // table is required
            System.err.println("Must provide a table name");
            return false;
        }

        if (null != (tkey = amap.remove("-port"))) port = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-user"))) username = tkey;
        if (null != (tkey = amap.remove("-pw"))) password = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-path"))) truststorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-pwd"))) truststorePwd = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-path"))) keystorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-pwd"))) keystorePwd = tkey;
        if (null != (tkey = amap.remove("-consistencyLevel"))) consistencyLevel = ConsistencyLevel.valueOf(tkey);
        if (null != (tkey = amap.remove("-numFutures"))) numFutures = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-numSplits"))) numSplits = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-splitSize"))) splitSize = Long.parseLong(tkey) * 1024 * 1024;
        if (null != (tkey = amap.remove("-beginToken"))) beginTokenString = tkey;
        if (null != (tkey = amap.remove("-endToken"))) endTokenString = tkey;
        if (null != (tkey = amap.remove("-debug"))) debug = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-readTimeout"))) readTimeout = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-connectTimeout"))) connectTimeout = Integer.parseInt(tkey);

        if (!amap.isEmpty()) {
            for (String k : amap.keySet())
                System.err.println("Unrecognized option: " + k);
            return false;
        }
        return validateArgs();
    }

    private SSLOptions createSSLOptions()
            throws KeyStoreException, IOException, NoSuchAlgorithmException,
            KeyManagementException, CertificateException, UnrecoverableKeyException {
        TrustManagerFactory tmf = null;
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load(new FileInputStream(new File(truststorePath)),
                truststorePwd.toCharArray());
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);

        KeyManagerFactory kmf = null;
        if (null != keystorePath) {
            KeyStore kks = KeyStore.getInstance("JKS");
            kks.load(new FileInputStream(new File(keystorePath)),
                    keystorePwd.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(kks, keystorePwd.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null ? kmf.getKeyManagers() : null,
                tmf != null ? tmf.getTrustManagers() : null,
                new SecureRandom());

        return JdkSSLOptions.builder().withSSLContext(sslContext).build(); //SSLOptions.DEFAULT_SSL_CIPHER_SUITES);
    }

    private void debugPrint(String str, boolean crlf, int level) {
        if (debug >= level)
            System.err.print(str + (crlf ? "\n" : ""));
    }

    private void setup()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        // Connect to Cassandra
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout).setConnectTimeoutMillis(connectTimeout))
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
        if (null != username)
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        if (null != truststorePath)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());

        cluster = clusterBuilder.build();
        if (null == cluster) {
            throw new IOException("Could not create cluster");
        }
        session = cluster.connect();
    }

    private void cleanup() {
        if (null != session)
            session.close();
        if (null != cluster)
            cluster.close();
    }

    private void determineSplits() {
        beginTokens = new ArrayList<>();
        endTokens = new ArrayList<>();
        Metadata m = cluster.getMetadata();
        if (null != beginTokenString) {
            BigInteger begin;
            BigInteger end;
            BigInteger delta;
            begin = new BigInteger(beginTokenString);
            end = new BigInteger(endTokenString);
            delta = end.subtract(begin).divide(new BigInteger(String.valueOf(numSplits)));
            for (int mype = 0; mype < numSplits; mype++) {
                if (mype < numSplits - 1) {
                    beginTokens.add(m.newToken(begin.add(delta.multiply(new BigInteger(String.valueOf(mype)))).toString()));
                    endTokens.add(m.newToken(begin.add(delta.multiply(new BigInteger(String.valueOf(mype + 1)))).toString()));
                } else {
                    beginTokens.add(m.newToken(begin.add(delta.multiply(new BigInteger(String.valueOf(numSplits - 1)))).toString()));
                    endTokens.add(m.newToken(end.toString()));
                }
            }

        } else {
            Set<TokenRange> inranges = m.getTokenRanges();
            Set<TokenRange> ranges = new HashSet<>();
            if (0 == numSplits) {
                debugPrint("Getting token ranges as number of splits", true, 2);
                numSplits = m.getTokenRanges().size();
            }
            if (numSplits > 0) {
                debugPrint("Splitting into " + numSplits + " splits", true, 2);
                for (TokenRange tr : inranges) {
                    Token start = tr.getStart();
                    Token end = tr.getEnd();
                    if (0 < start.compareTo(end)) {
                        ranges.add(m.newTokenRange(start, m.newToken(maxToken)));
                        ranges.add(m.newTokenRange(m.newToken(minToken), end));
                    } else {
                        ranges.add(tr);
                    }
                }
                int numRanges = ranges.size();
                numSplits = Math.max(numRanges * 10, numSplits);
                int numSplitsPerRange = numSplits / numRanges;
                debugPrint("Splitting " + numRanges + " ranges each into " + numSplitsPerRange + " splits", true, 2);
                if (numSplitsPerRange < 1)
                    numSplitsPerRange = 1;

                for (TokenRange r : ranges) {
                    List<TokenRange> splits = r.splitEvenly(numSplitsPerRange);
                    for (TokenRange s : splits) {
                        beginTokens.add(s.getStart());
                        endTokens.add(s.getEnd());
                    }
                }
                debugPrint("Total ranges: " + beginTokens.size(), true, 1);
            } else {
                List<Row> rows = session.execute("SELECT range_start, range_end, mean_partition_size, partitions_count FROM system.size_estimates WHERE keyspace_name='" + keyspaceName + "' AND table_name = '" + tableName + "'").all();
                Long maxToken = Long.MAX_VALUE;
                Long minToken = Long.MIN_VALUE;
                debugPrint("Splitting by size: " + splitSize, true, 2);
                for (Row r : rows) {
                    long stlong;
                    long enlong;
                    if (rows.size() == 1) {
                        stlong = minToken;
                        enlong = maxToken;
                    } else {
                        String st = r.getString("range_start");
                        String en = r.getString("range_end");
                        stlong = Long.parseLong(st);
                        enlong = Long.parseLong(en);
                    }
                    long mps = r.getLong("mean_partition_size");
                    long pc = r.getLong("partitions_count");
                    long nsplit = (long) (((double) mps * (double) pc) / (double) splitSize);
                    if (nsplit < 1)
                        nsplit = 1;
                    debugPrint("Splitting (" + stlong + "," + enlong + "] into " + nsplit + " splits", true, 2);
                    long delta = (enlong / nsplit) - (stlong / nsplit);
                    for (long i = 0; i < nsplit; i++) {
                        beginTokens.add(m.newToken(String.valueOf(stlong + (i * delta))));
                        debugPrint("  (" + (stlong + (i * delta)), false, 2);
                        if (i < nsplit - 1) {
                            endTokens.add(m.newToken(String.valueOf(stlong + ((i + 1) * delta))));
                            debugPrint(", " + (stlong + ((i + 1) * delta)) + "]", true, 2);
                        } else {
                            endTokens.add(m.newToken(String.valueOf(enlong)));
                            debugPrint(", " + enlong + "]", true, 2);
                        }
                    }
                }
                debugPrint("Total ranges: " + beginTokens.size(), true, 1);
            }
        }
    }

    private PreparedStatement prepareStatement() {
        List<ColumnMetadata> partkeys = cluster.getMetadata().getKeyspace(keyspaceName).getTable(tableName).getPartitionKey();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ");
        sb.append(keyspaceName).append(".").append(tableName);
        sb.append(" WHERE Token(");
        sb.append(partkeys.get(0).getName());
        for (int i = 1; i < partkeys.size(); i++)
            sb.append(", ").append(partkeys.get(i).getName());
        sb.append(") > ? AND Token(");
        sb.append(partkeys.get(0).getName());
        for (int i = 1; i < partkeys.size(); i++)
            sb.append(",").append(partkeys.get(i).getName());
        sb.append(") <= ?");

        debugPrint("Query: " + sb.toString(), true, 2);

        return session.prepare(sb.toString()).setConsistencyLevel(consistencyLevel);
    }

    public boolean run(String[] args)
            throws IOException,
            KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        if (!parseArgs(args)) {
            System.err.println("Bad arguments");
            System.err.println(usage());
            return false;
        }
        debugPrint("Version: " + version, true, 2);

        // Setup
        setup();

        // Determine splits
        determineSplits();

        // Prepare Statement
        PreparedStatement ps = prepareStatement();
        List<ResultSetFuture> flist = new ArrayList<>();
        int fsize = 0;
        long count = 0;
        Row r;

        // Loop over splits
        debugPrint("Running over " + beginTokens.size() + " tokens", true, 2);
        for (int i = 0; i < beginTokens.size(); i++) {
            //   Bind Split
            debugPrint("Executing: " + beginTokens.get(i) + "  " + endTokens.get(i), true, 2);
            BoundStatement bs = ps.bind(beginTokens.get(i),
                    endTokens.get(i));
            //   Execute query
            ResultSetFuture rsf = session.executeAsync(bs);
            flist.add(rsf);
            fsize++;
            if (fsize >= numFutures) {
                debugPrint("Waiting for " + flist.size() + " futures after token #" + i + "/" + beginTokens.size(), true, 2);
                for (ResultSetFuture rs : flist) {
                    try {
                        r = rs.getUninterruptibly(readTimeout, TimeUnit.MILLISECONDS).one();
                    } catch (Exception rte) {
                        System.err.println("An " + rte.getClass().getSimpleName() + " occurred. Try increasing -numSplits or reducing -splitSize. " + rte.getMessage());
                        rte.printStackTrace();
                        cleanup();
                        return false;
                    }
                    count += r.getLong(0);
                }
                flist.clear();
                fsize = 0;
            }
        }
        if (fsize > 0) {
            debugPrint("Waiting for " + flist.size() + " left futures", true, 2);
            for (ResultSetFuture rs : flist) {
                try {
                    r = rs.getUninterruptibly(readTimeout, TimeUnit.MILLISECONDS).one();
                } catch (Exception rte) {
                    System.err.println("An " + rte.getClass().getSimpleName() + " occurred. Try increasing -numSplits or reducing -splitSize. " + rte.getMessage());
                    rte.printStackTrace();
                    cleanup();
                    return false;
                }
                count += r.getLong(0);
            }
            flist.clear();
            fsize = 0;
        }

        System.out.println(keyspaceName + "." + tableName + ": " + count);

        cleanup();
        return true;
    }
}
