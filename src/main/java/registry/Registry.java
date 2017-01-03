package registry;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.retry.RetryForever;
import org.apache.mesos.Protos;
import org.apache.mesos.curator.CuratorPersister;
import org.apache.mesos.dcos.DcosConstants;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import registry.state.CuratorReadOnlyStateStore;
import registry.state.ReadOnlyStateStore;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The registry provides read-only access to the StateStores of DC/OS services developed with the DC/OS Service SDK.
 */
public class Registry {
    static final int RETRY_INTERVAL_MS = 5000;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CuratorPersister curatorPersister;
    private final Map<String, ReadOnlyStateStore> registry;
    private final String zkConnectionString;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    public Registry(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
        this.curatorPersister = new CuratorPersister(zkConnectionString, new RetryForever(RETRY_INTERVAL_MS));
        this.registry = new HashMap<>();
        startRefreshing();
    }

    public void refresh() throws Exception {
        Collection<String> serviceRoots = getServiceRoots();

        for (String serviceRoot : serviceRoots) {
            String serviceName = getServiceName(serviceRoot);
            ReadOnlyStateStore readOnlyStateStore = new CuratorReadOnlyStateStore(serviceName, zkConnectionString);

            try {
                Optional<Protos.FrameworkID> frameworkIDOptional = readOnlyStateStore.fetchFrameworkId();
                if (frameworkIDOptional.isPresent()) {
                    registry.put(serviceName, readOnlyStateStore);
                } else {
                    logger.warn("FrameworkID not yet present for: {}", serviceName);
                }
            } catch (StateStoreException e) {
                logger.error("Failed to fetch FrameworkID for: {}", serviceName);
            }
        }
    }

    public Collection<String> getServiceNames() {
        return registry.keySet();
    }

    public ReadOnlyStateStore getStore(String serviceName) {
        return registry.get(serviceName);
    }

    public Map<String, String> getTaskHosts(String serviceName) throws TaskException {
        ReadOnlyStateStore stateStore = getStore(serviceName);
        if (stateStore == null) {
            return Collections.emptyMap();
        }

        Map<String, String> taskHosts = new HashMap<>();
        for (Protos.TaskInfo taskInfo : stateStore.fetchTasks()) {
            taskHosts.put(taskInfo.getName(), TaskUtils.getHostname(taskInfo));
        }

        return taskHosts;
    }

    private String getServiceName(String serviceRoot) {
        return serviceRoot.substring(DcosConstants.SERVICE_ROOT_PATH_PREFIX.length()-1);
    }

    private void startRefreshing() {
        scheduledExecutorService.schedule(() -> {
                    try {
                        logger.info("Refreshing...");
                        refresh();
                    } catch (Exception e) {
                        logger.error("Failed to refresh.", e);
                    }
                },
                RETRY_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    Collection<String> getServiceRoots() throws Exception {
        return curatorPersister.getChildren("/").stream()
                .filter(name -> name.startsWith(DcosConstants.SERVICE_ROOT_PATH_PREFIX.substring(1)))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    void close() {
        curatorPersister.close();
    }
}
