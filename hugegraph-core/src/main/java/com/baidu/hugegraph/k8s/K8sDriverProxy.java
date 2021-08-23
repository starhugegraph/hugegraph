package com.baidu.hugegraph.k8s;

import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class K8sDriverProxy {

    private static final Logger LOG = Log.logger(K8sDriverProxy.class);

    private static final String USER_DIR = System.getProperty("user.dir");

    private static boolean K8S_API_ENABLED = false;

    private static String NAMESPACE = "";
    private static String CUBE_CONFIG_PATH = "";
    private static String HUGEGRAPH_URL = "";
    private static String ENABLE_INTERNAL_ALGORITHM = "";
    private static String INTERNAL_ALGORITHM_IMAGE_URL = "";

    protected HugeConfig config;
    protected KubernetesDriver driver;

    static {
        OptionSpace.register("computer-driver",
                "com.baidu.hugegraph.computer.driver.config" +
                        ".ComputerOptions");
        OptionSpace.register("computer-k8s-driver",
                "com.baidu.hugegraph.computer.k8s.config" +
                        ".KubeDriverOptions");
        OptionSpace.register("computer-k8s-spec",
                "com.baidu.hugegraph.computer.k8s.config" +
                        ".KubeSpecOptions");
    }

    public static void setCubeConfig(String namespace,
                                     String cubeConfigPath,
                                     String hugegraphUrl,
                                     String enableInternalAlgorithm,
                                     String internalAlgorithmImageUrl) throws IOException {
        File kubeConfigFile = new File(USER_DIR + "/" + cubeConfigPath);
        if (!kubeConfigFile.exists() || StringUtils.isEmpty(hugegraphUrl)) {
            throw new IOException("[K8s API] k8s config fail");
        }

        K8S_API_ENABLED = true;
        NAMESPACE = namespace;
        CUBE_CONFIG_PATH = USER_DIR + "/" + cubeConfigPath;
        HUGEGRAPH_URL = hugegraphUrl;
        ENABLE_INTERNAL_ALGORITHM = enableInternalAlgorithm;
        INTERNAL_ALGORITHM_IMAGE_URL = internalAlgorithmImageUrl;
    }

    public static boolean isK8sApiEnabled() {
        return K8S_API_ENABLED;
    }

    public K8sDriverProxy(String partitionsCount, String internalAlgorithm, String paramsClass) {
        try {
            if (!K8sDriverProxy.K8S_API_ENABLED) {
                throw new UnsupportedOperationException("The k8s api not enabled.");
            }

            LOG.info("partitionsCount:" + partitionsCount + "\tinternalAlgorithm:" + internalAlgorithm +
                    "\tparamsClass:" + paramsClass);
            this.initConfig(partitionsCount, internalAlgorithm, paramsClass);
            this.initKubernetesDriver();
        } catch (Throwable throwable) {
            LOG.error("Failed to start K8sDriverProxy ", throwable);
        }
    }

    /**
     * options.put("k8s.internal_algorithm_image_url", "hugegraph/hugegraph-computer-based-algorithm:beta1");
     * options.put("k8s.internal_algorithm", "[pagerank, test]");
     * options.put("algorithm.params_class", "com.baidu.hugegraph.computer.algorithm.rank.pagerank.PageRankParams");
     *
     * options.put("k8s.internal_algorithm", "[pagerank, test]");
     * options.put("algorithm.params_class", "com.baidu.hugegraph.computer.algorithm.rank.pagerank.PageRankParams");
     *
     * @param partitionsCount
     */
    protected void initConfig(String partitionsCount,
                              String internalAlgorithm,
                              String paramsClass) {
        HashMap<String, String> options = new HashMap<>();

        // from configuration
        options.put("k8s.namespace", K8sDriverProxy.NAMESPACE);
        options.put("k8s.kube_config", K8sDriverProxy.CUBE_CONFIG_PATH);
        options.put("hugegraph.url", K8sDriverProxy.HUGEGRAPH_URL);
        options.put("k8s.enable_internal_algorithm", K8sDriverProxy.ENABLE_INTERNAL_ALGORITHM);
        options.put("k8s.internal_algorithm_image_url", K8sDriverProxy.INTERNAL_ALGORITHM_IMAGE_URL);

        // from rest api params
        options.put("job.partitions_count", partitionsCount);   // partitionsCount >= worker_instances
        options.put("k8s.internal_algorithm", internalAlgorithm);
        options.put("algorithm.params_class", paramsClass);
        LOG.info("config options: " + options.toString());
        MapConfiguration mapConfig = new MapConfiguration(options);
        this.config = new HugeConfig(mapConfig);
    }

    protected void initKubernetesDriver() {
        this.driver = new KubernetesDriver(this.config);
    }

    public KubernetesDriver getKubernetesDriver() {
        return this.driver;
    }

    public void close() {
        this.driver.close();
    }
}
