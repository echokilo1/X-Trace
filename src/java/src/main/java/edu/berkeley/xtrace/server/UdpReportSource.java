package edu.berkeley.xtrace.server;

import java.io.IOException;
import java.io.*;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import edu.berkeley.xtrace.XTraceException;

public class UdpReportSource implements ReportSource {
	private static final Logger LOG = Logger.getLogger(UdpReportSource.class);

	private BlockingQueue<String> q;
	private DatagramSocket socket;
  private long droppedReports = 0;
  private long totalReports = 0;
  private long reportsPerPeriod = 0;
  private long startTime = 0;
  private static final long FIVE_SECONDS = 5000000000L;

	public void initialize() throws XTraceException {
		
		String udpSource = System.getProperty("xtrace.udpsource", "127.0.0.1:7831");
		
		/*InetAddress localAddr;
		try {
			localAddr = InetAddress.getByName(udpSource.split(":")[0]);
		} catch (UnknownHostException e) {
			throw new XTraceException("Unknown host: " + udpSource.split(":")[0], e);
		}*/
		int localPort = Integer.parseInt(udpSource.split(":")[1]);
		try {
			socket = new DatagramSocket(localPort);
		} catch (SocketException e) {
			throw new XTraceException("Unable to open socket", e);
		}

		//LOG.info("UDPReportSource initialized on " + localAddr + ":" + localPort);
	}

	public void setReportQueue(BlockingQueue<String> q) {
		this.q = q;
	}

	public void shutdown() {
		if (socket != null)
			socket.close();
	}

	public void run() {
		LOG.info("UDPReportSource listening for packets");
		startTime = System.nanoTime();
		while (true) {
      long currentTime = System.nanoTime();
      if (currentTime - startTime > FIVE_SECONDS) {
        //System.out.println((((double)(reportsPerPeriod))/((double)((currentTime - startTime) / 1000000000L))) + "packets per second");
        System.out.println(reportsPerPeriod/(((double)(currentTime - startTime)) / 1000000000L) + " reports per second");
        reportsPerPeriod = 0;
        startTime = System.nanoTime();
      }
			byte[] buf = new byte[8192];
			DatagramPacket p = new DatagramPacket(buf, buf.length);
		    try {
				socket.receive(p);
			} catch (IOException e) {
				LOG.warn("Unable to receive report", e);
			}
      //System.out.println("Received packet from " + p.getSocketAddress());
			
			//LOG.debug("Received Report");
			
		    //try {
            while(true) {
              try {
                ByteArrayInputStream arr = new ByteArrayInputStream(p.getData());
                DataInputStream stream = new DataInputStream(arr);
                while(true) {
                  try {
                    int length = stream.readInt();
                    //System.out.println("length of report = " + length);
                    if (length <= 0)
                      break;
                    byte[] msg = new byte[length];
                    stream.read(msg);
                    totalReports++;
                    reportsPerPeriod++;
                    if (totalReports % 10000 == 0)
                      System.out.println("total number of reports = " + totalReports);
                    String report = new String(msg, 0, msg.length, "UTF-8");
                    //System.out.println(report);
                    boolean test = q.offer(report);
                    if (!test) {
                      droppedReports++;
                      if (droppedReports % 100 == 0)
                        System.out.println(droppedReports + " have been dropped out of a total of " + totalReports);
                    }
                  } catch (EOFException e) {
                    break;
                  }
                }
				        //q.put(new String(p.getData(), 0, p.getLength(), "UTF-8"));
                break;
              } catch (Exception e) {
                continue;
              }
            }
				//q.offer(new String(p.getData(), 0, p.getLength(), "UTF-8"));
			/*} catch (UnsupportedEncodingException e) {
				LOG.warn("UTF-8 not available", e);
			}*/
		}
	}
}
