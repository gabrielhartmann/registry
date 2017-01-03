package registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import registry.api.ApiServer;

/**
 * The main entry point for launching a registry and an HTTP server exposing it.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String portStr = System.getenv("PORT0");

        if (portStr == null) {
            LOGGER.error("envvar PORT0 is not set.");
        }

        int port = Integer.valueOf(portStr);
        Registry registry = new Registry("master.mesos:2181");
        new ApiServer(port, registry).start();
    }
}
