package registry;

import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.DcosConstants;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.testing.CuratorTestUtils;
import org.junit.*;
import registry.state.ReadOnlyStateStore;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the Registry.
 */
public class RegistryTest {
    private static final FrameworkID FRAMEWORK_ID = FrameworkID.newBuilder().setValue("test-framework-id").build();
    private static final String serviceName = "test-service-name";
    private static TestingServer testZk;
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
        Assert.assertNull(readOnlyStateStore);
    }

    @Test
    public void testGetStore() throws Exception {
        testGetOneServiceRoot();
        registry.refresh();
        ReadOnlyStateStore readOnlyStateStore = registry.getStore(serviceName);
        Assert.assertNotNull(readOnlyStateStore);
    }
}
