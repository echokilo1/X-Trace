package edu.berkeley.xtrace.samplingserver;

import edu.berkeley.xtrace.XTraceSampling;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 *
 * @author Raja Sambasivan
 */
public class SamplingPercentageServer implements Runnable {

    private static final Log logger =
            LogFactory.getLog(SamplingPercentageServer.class);
    
    /**
     * Constructor
     */
    public SamplingPercentageServer() {}

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