package registry;

import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.DcosConstants;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.testing.CuratorTestUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import registry.api.ApiServer;
import registry.state.ReadOnlyStateStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test the Registry.
 */
public class RegistryTest {
    private static final FrameworkID FRAMEWORK_ID = FrameworkID.newBuilder().setValue("test-framework-id").build();
    private static final String serviceName = "test-service-name";
    private static TestingServer testZk;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private StateStore store;
    private Registry registry;

    @BeforeClass
    public static void beforeAll() throws Exception {
        testZk = new TestingServer();
    }

    @Before
    public void beforeEach() throws Exception {
        CuratorTestUtils.clear(testZk);
        registry = new Registry(testZk.getConnectString());
    }

    @After
    public void afterEach() {
        if (store != null) {
            ((CuratorStateStore) store).closeForTesting();
        }

        if (registry != null) {
            registry.close();
        }
    }

    @Test
    public void testGetNoServiceRoots() throws Exception {
        assertTrue(registry.getServiceRoots().isEmpty());
    }

    @Test
    public void testGetOneServiceRoot() throws Exception {
        String serviceRoot = (DcosConstants.SERVICE_ROOT_PATH_PREFIX + serviceName).substring(1);
        store = new CuratorStateStore(serviceName, testZk.getConnectString());
        store.storeFrameworkId(FRAMEWORK_ID);
        assertEquals(serviceRoot, registry.getServiceRoots().stream().findFirst().get());
    }

    @Test
    public void testGetMultipleServiceRoots() throws Exception {
        CuratorStateStore storeB = null;
        try {
            String serviceRootA = (DcosConstants.SERVICE_ROOT_PATH_PREFIX + serviceName).substring(1);
            store = new CuratorStateStore(serviceName, testZk.getConnectString());
            store.storeFrameworkId(FRAMEWORK_ID);

            String frameworkNameB = "bar";
            String serviceRootB = (DcosConstants.SERVICE_ROOT_PATH_PREFIX + frameworkNameB).substring(1);
            storeB = new CuratorStateStore(frameworkNameB, testZk.getConnectString());
            storeB.storeFrameworkId(FRAMEWORK_ID);

            List<String> serviceNames = new ArrayList<>(registry.getServiceRoots());
            assertEquals(serviceRootA, serviceNames.get(1));
            assertEquals(serviceRootB, serviceNames.get(0));
        } finally {
            if (storeB != null) {
                storeB.closeForTesting();
            }
        }

    }

    @Test
    public void testGetNoServiceNames() {
        assertTrue(registry.getServiceNames().isEmpty());
    }

    @Test
    public void testGetOneServiceName() throws Exception {
        testGetOneServiceRoot();
        registry.refresh();
        assertEquals(1, registry.getServiceNames().size());
        assertEquals(serviceName, registry.getServiceNames().stream().findFirst().get());
    }

    @Test
    public void testAutoRefresh() throws Exception {
        testGetOneServiceRoot();
        Thread.sleep(Registry.RETRY_INTERVAL_MS + 1000);
        assertEquals(1, registry.getServiceNames().size());
        assertEquals(serviceName, registry.getServiceNames().stream().findFirst().get());
    }

    @Test
    public void testGetNoStore() throws Exception {
        ReadOnlyStateStore readOnlyStateStore = registry.getStore(serviceName);
        assertNull(readOnlyStateStore);
    }

    @Test
    public void testGetStore() throws Exception {
        testGetOneServiceRoot();
        registry.refresh();
        ReadOnlyStateStore readOnlyStateStore = registry.getStore(serviceName);
        assertNotNull(readOnlyStateStore);
    }

    @Test
    public void testGetNoTaskHosts() throws Exception {
        testGetOneServiceRoot();
        registry.refresh();
        assertTrue(registry.getTaskHosts(serviceName).isEmpty());
    }

    @Test
    public void testGetOneTaskHost() throws Exception {
        store = new CuratorStateStore(serviceName, testZk.getConnectString());
        store.storeFrameworkId(FRAMEWORK_ID);
        store.storeTasks(Arrays.asList(getTestTaskInfo()));
        registry.refresh();
        assertEquals(1, registry.getTaskHosts(serviceName).size());
        assertEquals("task-name", registry.getTaskHosts(serviceName).keySet().stream().findFirst().get());
        assertEquals("hostname", registry.getTaskHosts(serviceName).values().stream().findFirst().get());
    }

    private Protos.TaskInfo getTestTaskInfo() {
        return Protos.TaskInfo.newBuilder()
                .setName("task-name")
                .setTaskId(Protos.TaskID.newBuilder().setValue("task-id"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"))
                .setLabels(Protos.Labels.newBuilder()
                        .addLabels(Protos.Label.newBuilder()
                                .setKey("offer_hostname")
                                .setValue("hostname")))
                .build();
    }
}
