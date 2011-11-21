package edu.berkeley.xtrace.server.sampling;

import edu.berkeley.xtrace.XTraceSampling;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.IllegalBlockingModeException;
import org.apache.log4j.Logger;

/**
 *
 * @author Raja Sambasivan
 */
public class SamplingPercentageServer implements Runnable {

    Logger logger = null;
    /**
     * Constructor
     */
    public SamplingPercentageServer() {
         logger = Logger.getLogger(
            "edu.berkeley.xtrace.server.sampling.SamplingPercentageServer");
    }

    public void StartServer() {
        Thread t = new Thread(this, "SamplingServer");
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        DataInputStream input = null;
        int samplingRate = 0;

        try {
            serverSocket = new ServerSocket(Constants.port);
        } catch (Exception e) {
            logger.fatal("Couldn't open socket.  " + e);
            assert(false);
        }
        
        while (true) {
            try {
                // Wait until client connects to port
                Socket client = serverSocket.accept();
                input = new DataInputStream(
                        new BufferedInputStream(client.getInputStream()));
                samplingRate = input.readInt();
                client.close();
                input.close();
            } catch (Exception e) {
                logger. error("Couldn't get client data.  "  + e);
            }
            /* Read sampling rate and set it */
            assert (samplingRate >= 0 && samplingRate <= 100);
            logger.info("Setting sampling rate to: " + samplingRate);
            XTraceSampling.setSamplingPercentage(samplingRate);
        }
    }
}