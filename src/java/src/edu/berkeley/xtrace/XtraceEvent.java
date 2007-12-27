package edu.berkeley.xtrace;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import edu.berkeley.xtrace.reporting.Report;
import edu.berkeley.xtrace.reporting.ReportingContext;

/**
 * An <code>XtraceEvent</code> makes propagating X-Trace metadata easier.
 * An application writer can initialize a new <code>XtraceEvent</code> when
 * a task begins (usually as the result of a new request arriving).
 * Each X-Trace metadata extracted from the request (or requests, if the
 * task is made up of concurrent requests) is added to this context
 * via the <code>addEdge</code> method.  Additionally, information
 * in the form of key-value pairs can be supplied as well.
 * <p>
 * The metadata that should be propagated into any new requests
 * is obtained via the <code>getNewMetadata</code> method.  Lastly,
 * a report obtained via <code>getNewReport</code> should be sent
 * to the report context, which will forward it to the reporting
 * infrastructure.
 * 
 * @author George Porter <gporter@cs.berkeley.edu>
 */
public class XtraceEvent {
	
	/** 
	 * Thread-local random number generator, seeded with machine name
	 * as well as current time. 
	 *
	 **/
	private static ThreadLocal<Random> random
		= new ThreadLocal<Random>() {
		@Override
		protected Random initialValue() {
			// TODO: use process ID too?
			try {
				return new Random(++threadId
						+ System.nanoTime()
						+ Thread.currentThread().getId()
						+ InetAddress.getLocalHost().getHostName().hashCode() );
			} catch (UnknownHostException e) {
				// Failed to get local host name; just use the other pieces
				return new Random(++threadId
						+ System.nanoTime()
						+ Thread.currentThread().getId());
			}
		}
	};
	private static volatile long threadId = 0;
	
	private Report report;
	private byte[] myOpId;

	/**
	 * Initialize a new XtraceEvent.  This should be done for each
	 * request or task processed by this node.
	 *
	 */
	public XtraceEvent(int opIdLength) {
		report = new Report();
		myOpId = new byte[opIdLength];
		random.get().nextBytes(myOpId);
	}
	
	public void addEdge(Metadata xtr) {
		Metadata newmd = new Metadata(xtr);
		newmd.setOpId(myOpId);
		
		report.put("X-Trace", newmd.toString(), false);
		report.put("Edge", xtr.getOpIdString());
	}

	public void put(String key, String value) {
		report.put(key, value);
	}
	
	public Metadata getNewMetadata() {
		Metadata xtr = report.getMetadata();
		Metadata xtr2;
		
		/* If we don't know the task id, return an
		 * invalid metadata
		 */
		if (xtr == null) {
			return new Metadata();
		}
		
		xtr2 = new Metadata(xtr);
		xtr2.setOpId(myOpId);
		
		return xtr2;
	}
	
	/**
	 * Set the event's metadata to a given value (used for tasks with no edges).
	 */
	public void setMetadata(Metadata xtr) {
		// TODO: the following line isn't defensive.  Fix by copying the
		// bytes, rather than just the reference.
		myOpId = xtr.getOpId();
		report.put("X-Trace", xtr.toString(), false);
	}
	
	/**
	 * Add a timestamp property with the current time.
	 */
	private void setTimestamp() {
		long time = System.currentTimeMillis();
		String value = String.format("%d.%03d", time/1000, time%1000);
		report.put("Timestamp", value, false);
	}
	
	public Report getNewReport() {
	    setTimestamp();
		return report;
	}
	
	public void sendReport() {
	    setTimestamp();
		ReportingContext.getReportCtx().sendReport(report);
	}
}
