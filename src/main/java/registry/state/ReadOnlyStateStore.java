package registry.state;

import org.apache.mesos.Protos;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;

import java.util.Collection;
import java.util.Optional;

/**
 * This interface is a subset of the {@link StateStore} interface allowing, readonly operations.
 */
public interface ReadOnlyStateStore {
    Optional<Protos.FrameworkID> fetchFrameworkId() throws StateStoreException;

    Collection<String> fetchTaskNames() throws StateStoreException;

    Collection<Protos.TaskInfo> fetchTasks() throws StateStoreException;

    Optional<Protos.TaskInfo> fetchTask(String taskName) throws StateStoreException;

    Collection<Protos.TaskStatus> fetchStatuses() throws StateStoreException;

    Optional<Protos.TaskStatus> fetchStatus(String taskName) throws StateStoreException;

    byte[] fetchProperty(String key) throws StateStoreException;

    Collection<String> fetchPropertyKeys() throws StateStoreException;

    boolean isSuppressed() throws StateStoreException;
}
