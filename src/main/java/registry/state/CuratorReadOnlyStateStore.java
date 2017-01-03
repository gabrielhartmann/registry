package registry.state;

import org.apache.mesos.Protos;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;

import java.util.Collection;
import java.util.Optional;

/**
 * The ReadOnly
 */
public class CuratorReadOnlyStateStore implements ReadOnlyStateStore {
    private final StateStore stateStore;

    public CuratorReadOnlyStateStore(String frameworkName) {
        stateStore = new CuratorStateStore(frameworkName);
    }

    public CuratorReadOnlyStateStore(String frameworkName, String connectionString) {
        stateStore = new CuratorStateStore(frameworkName, connectionString);
    }

    @Override
    public Optional<Protos.FrameworkID> fetchFrameworkId() throws StateStoreException {
        return stateStore.fetchFrameworkId();
    }

    @Override
    public Collection<String> fetchTaskNames() throws StateStoreException {
        return stateStore.fetchTaskNames();
    }

    @Override
    public Collection<Protos.TaskInfo> fetchTasks() throws StateStoreException {
        return stateStore.fetchTasks();
    }

    @Override
    public Optional<Protos.TaskInfo> fetchTask(String taskName) throws StateStoreException {
        return stateStore.fetchTask(taskName);
    }

    @Override
    public Collection<Protos.TaskStatus> fetchStatuses() throws StateStoreException {
        return fetchStatuses();
    }

    @Override
    public Optional<Protos.TaskStatus> fetchStatus(String taskName) throws StateStoreException {
        return fetchStatus(taskName);
    }

    @Override
    public byte[] fetchProperty(String key) throws StateStoreException {
        return fetchProperty(key);
    }

    @Override
    public Collection<String> fetchPropertyKeys() throws StateStoreException {
        return fetchPropertyKeys();
    }

    @Override
    public boolean isSuppressed() throws StateStoreException {
        return isSuppressed();
    }
}
