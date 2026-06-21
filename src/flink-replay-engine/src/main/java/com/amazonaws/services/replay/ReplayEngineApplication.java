package com.amazonaws.services.replay;

import com.amazonaws.services.replay.api.ReplayResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplayEngineApplication {
    private static final Logger logger = LoggerFactory.getLogger(ReplayEngineApplication.class);

    private static final int DEFAULT_PORT = 8080;
    private Server server;
    private final int port;

    public ReplayEngineApplication(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.classnames",
                ReplayResource.class.getCanonicalName()
        );
        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.packages",
                "com.amazonaws.services.replay.api"
        );

        server.start();
        logger.info("Replay Engine Server started on port {}", port);
        logger.info("API base URL: http://localhost:{}/api/v1/replay", port);
    }

    public void stop() throws Exception {
        if (server != null) {
            server.stop();
            logger.info("Replay Engine Server stopped");
        }
    }

    public void join() throws InterruptedException {
        if (server != null) {
            server.join();
        }
    }

    public static void main(String[] args) {
        try {
            int port = DEFAULT_PORT;
            if (args.length > 0) {
                try {
                    port = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid port argument {}, using default {}", args[0], DEFAULT_PORT);
                }
            }

            String stateDir = "./state";
            if (args.length > 1) {
                stateDir = args[1];
            }
            System.setProperty("replay.state.dir", stateDir);

            ReplayEngineApplication app = new ReplayEngineApplication(port);
            app.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook triggered");
                try {
                    app.stop();
                } catch (Exception e) {
                    logger.error("Error during shutdown", e);
                }
            }));

            app.join();

        } catch (Exception e) {
            logger.error("Failed to start Replay Engine", e);
            System.exit(1);
        }
    }
}
