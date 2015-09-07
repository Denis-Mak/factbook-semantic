package it.factbook.semantic;

import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class SemanticSearchServer{
    private static final Logger log = LoggerFactory.getLogger(SemanticSearchServer.class);

    private int listenPort = 9406;

    private boolean isStopped = false;

    private ServerSocket serverSocket;

    private Map<Integer, JavaRDD<SemanticVector>> allVectors;

    private CassandraConnector cassandraConnector;

    public SemanticSearchServer(int port, Map<Integer, JavaRDD<SemanticVector>> allVectors,
                                CassandraConnector cassandraConnector){
        this.listenPort = port;
        this.allVectors = allVectors;
        this.cassandraConnector = cassandraConnector;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(listenPort);
        } catch (IOException e) {
            log.error("Exception caught when trying to listen on port "
                    + " or listening for a connection", e);
        }

        while (!isStopped) {
            try {
                Socket clientSocket = serverSocket.accept();
                new Thread(
                        new SemanticSearchWorker(clientSocket, allVectors, cassandraConnector)
                ).start();
            } catch (IOException e) {
                if (isStopped) {
                    log.error("Semantic search server is stopped");
                } else {
                    log.error("Error accepting client connections", e);
                }
            }
        }
        log.info("Server stopped");
    }

    public synchronized boolean isStopped() {
        return this.isStopped;
    }

    public synchronized void stop() {
        this.isStopped = true;
        try {
            if (this.serverSocket != null) this.serverSocket.close();
        } catch (IOException e) {
            log.error("Error stopping server", e);
        }
        log.info("Semantic search server stopped");
    }

}