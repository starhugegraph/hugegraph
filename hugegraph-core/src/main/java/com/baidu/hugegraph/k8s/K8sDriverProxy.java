package com.baidu.hugegraph.k8s;

import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;

import java.util.HashMap;

public class K8sDriverProxy {

    private static final Logger LOG = Log.logger(K8sDriverProxy.class);

    private static String CUBE_CONFIG_PATH = "";

    private static boolean K8S_SUPPORT = false;

    protected String namespace = "common";

    protected HugeConfig config;

    protected KubernetesDriver driver;

    protected static final String IMAGE_REPOSITORY_URL =
            "czcoder/hugegraph-computer-test";

    public static void setCubeConfig(String cubeConfigPath) {
        CUBE_CONFIG_PATH = cubeConfigPath;
        K8S_SUPPORT = true;
    }

    public static boolean supportK8s() {
        return K8S_SUPPORT;
    }

    public K8sDriverProxy() {
        try {
            // String kubeconfigFilename = getKubeconfigFilename();
            // File file = new File(kubeconfigFilename);
            // Assert.assertTrue(file.exists());

            // this.namespace = "common";
            // System.setProperty("k8s.watch_namespace", Constants.ALL_NAMESPACE);

            this.initConfig();
            this.initKubernetesDriver();
        } catch (Throwable throwable) {
            LOG.error("Failed to start K8sDriverProxy ", throwable);
        }

    }

    protected void initConfig() {
        HashMap<String, String> options = new HashMap<>();
        options.put("job.id", KubeUtil.genJobId("PageRank"));
        options.put("job.workers_count", "1");

        //options.put("algorithm.result_class",
        //        LongValue.class.getName());
        options.put("algorithm.params_class",
                "com.baidu.hugegraph.computer.core.config.Null");
        options.put("job.partitions_count",
                "1000");
        options.put("bsp.etcd_endpoints",
                "http://abc:8098");
        options.put("hugegraph.url",
                "http://127.0.0.1:8080");
        options.put("k8s.namespace",
                this.namespace);
        options.put("k8s.log4j_xml_path",
                "conf/log4j2-test.xml");
        options.put("k8s.enable_internal_algorithm",
                "false");
        options.put("k8s.image_repository_url",
                IMAGE_REPOSITORY_URL);
        options.put("k8s.image_repository_username",
                "hugegraph");
        options.put("k8s.image_repository_password",
                "hugegraph");
        options.put("k8s.internal_algorithm_image_url",
                IMAGE_REPOSITORY_URL + ":PageRank-latest");
        options.put("k8s.pull_policy", "IfNotPresent");
        options.put("k8s.jvm_options", "-Dlog4j2.debug=true");
        options.put("k8s.master_command", "[/bin/sh, -c]");
        options.put("k8s.worker_command", "[/bin/sh, -c]");
        options.put("k8s.master_args", "[echo master]");
        options.put("k8s.worker_args", "[echo worker]");
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
