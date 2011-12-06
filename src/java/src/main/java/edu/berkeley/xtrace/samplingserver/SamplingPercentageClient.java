package edu.berkeley.xtrace.samplingserver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 *
 * @author rajasambasivan
 */
public class SamplingPercentageClient {
    
    private static final Logger logger = 
            Logger.getLogger(SamplingPercentageClient.class);

    /**
     * Default constructor
     */
    public SamplingPercentageClient() {}

    /**
     * Sends the sampling percentage specified to the XTraceSamplingServer
     * 
     * @param server: The server to which to send data
     * @param port: The port to use
     * @param samplingPercentage: the sampling percentage
     */
    public void sendData(String server, int port, int samplingPercentage) {
        try {
            Socket client = new Socket(server, port);
            DataOutputStream socketOut = new DataOutputStream(
                    client.getOutputStream());
            socketOut.writeInt(samplingPercentage);
            socketOut.close(); 
            client.close();
        } catch (UnknownHostException e) {
            logger.error("Unknown host: " + server  + ".  "  + e);
        } catch (IOException e) {
            logger.error("Couldn't write data to socket."  
                    + server + ".  port: " + port  + ".  " + e);
        }
    }

    /**
     * Gets the sampling percentage from the command line and passes it to 
     * XTraceSamplingPercentageClient.sendData();
     * 
     * @param args: A String array specifying the hostname and the sampling rate
     */
    public static void main(String[] args) {
        int value = 0;
        assert(args.length == 2);        
        String host = args[0];
        
        BasicConfigurator.configure();

        try {
            value = Integer.parseInt(args[1]);            
        } catch (NumberFormatException e) {
            /* Log something */
            System.exit(1);
        }
        assert (value >= 0 && value <= 100);
        SamplingPercentageClient client 
                = new SamplingPercentageClient();
        client.sendData(host, Constants.port, value);
    }
}