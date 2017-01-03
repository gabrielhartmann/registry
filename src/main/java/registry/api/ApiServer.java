package registry.api;

import com.google.gson.Gson;
import org.apache.mesos.api.JettyApiServer;
import org.apache.mesos.offer.TaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import registry.Registry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;

/**
 * The ApiServer serves REST endpoints for discovery of service meta-data.
 */
public class ApiServer {
    private final JettyApiServer apiServer;
    public ApiServer(int port, Registry registry) {
        RegistryResource registryResource = new RegistryResource(registry);
        apiServer = new JettyApiServer(port, Arrays.asList(registryResource));
    }

    public void start() throws Exception {
        apiServer.start();
    }

    public void stop() throws Exception {
        apiServer.stop();
    }

    @Path("/v1/registry")
    public static class RegistryResource {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final Registry registry;

        public RegistryResource(Registry registry) {
            this.registry = registry;
        }

        /**
         * Produces a listing of all services known to the registry.
         */
        @GET
        public Response getServices() {
            return Response.ok(
                    new Gson().toJson(registry.getServiceNames()),
                    MediaType.APPLICATION_JSON)
                    .build();
        }

        /**
         * Produces a listing of taskName:hostName map of all tasks for a given service.
         */
        @GET
        @Path("/{serviceName}")
        public Response getService(@PathParam("serviceName") String serviceName) {
            try {
                return Response.ok(
                        new Gson().toJson(registry.getTaskHosts(serviceName)),
                        MediaType.APPLICATION_JSON)
                        .build();
            } catch (TaskException e) {
                logger.error("Failed to retrieve meta-data for: {}", serviceName);
                return Response.serverError().build();
            }
        }
    }
}
