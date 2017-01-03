package registry.state;

import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.testing.CuratorTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test the ReadOnlyStateStore.
 */
public class ReadOnlyStateStoreTest {
    private static final FrameworkID FRAMEWORK_ID = FrameworkID.newBuilder().setValue("test-framework-id").build();
    private static final String ROOT_ZK_PATH = "/test-root-path";
    private static TestingServer testZk;
    private StateStore store;
    private CuratorReadOnlyStateStore readOnlyStateStore;

    @BeforeClass
    public static void beforeAll() throws Exception {
        testZk = new TestingServer();
    }

    @Before
    public void beforeEach() throws Exception {
        CuratorTestUtils.clear(testZk);
        store = new CuratorStateStore(ROOT_ZK_PATH, testZk.getConnectString());
        readOnlyStateStore = new CuratorReadOnlyStateStore(ROOT_ZK_PATH, testZk.getConnectString());
    }

    @After
    public void afterEach() {
        ((CuratorStateStore) store).closeForTesting();
    }

    @Test
    public void testStoreFetchFrameworkId() throws Exception {
        store.storeFrameworkId(FRAMEWORK_ID);
        assertEquals(FRAMEWORK_ID, readOnlyStateStore.fetchFrameworkId().get());
    }
}
