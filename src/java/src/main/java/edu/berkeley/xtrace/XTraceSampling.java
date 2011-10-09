package edu.berkeley.xtrace;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Provides an API for storing and retrieving the current sampling rate
 *
 * Usage:
 * <ul>
 *  <li>  Use <code>SetSamplingPercentage(int percentage)</code> to set the sampling percentage
 *  <li>  Use <code>GetSamplingPercentage()</code> to retrieve the sampling rate
 * </ul>
 *
 * @author Raja Sambasivan
 */
public class XTraceSampling {
    /** Sample 10% of all incoming tasks (requests) by default */
    private static int samplingPercentage = 10;

    /** A counter of threads that have accessed this class */
    private static volatile long threadId = 0;

    /** 
     * Instantiate a new random number generator with an unique sead for use by
     * all events that happen in this thread.  
     */
    private static ThreadLocal<Random> random
        = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
			int processId = ManagementFactory.getRuntimeMXBean().getName().hashCode();
			try {
				return new Random(++threadId
                                  + processId
                                  + System.nanoTime()
                                  + Thread.currentThread().getId()
                                  + InetAddress.getLocalHost().getHostName().hashCode() );
			} catch (UnknownHostException e) {
				// Failed to get local host name; just use the other pieces
				return new Random(++threadId
                                  + processId
                                  + System.nanoTime()
                                  + Thread.currentThread().getId());
			}
        }
    };

    /**
     * Constructor
     */
    public XTraceSampling() {}

    /**
     * Set the sampling percentage
     * @param percentage: The sampling percentage
     */
    public static void setSamplingPercentage(int percentage) {
        assert(percentage >= 0 && percentage <= 100);
        samplingPercentage = percentage;
    }

    /**
     * Get the sampling percentage
     * @return the sampling percentage
     */
    public static int getSamplingPercentage() {
        return samplingPercentage;
    }

    /**
     * Get a decision on whether or not to sample a new event
     * @return true if event should be sampled, false otherwise
     */
    public static boolean shouldSample() {
        return random.get().nextInt(101) <= samplingPercentage;
    }

    /**
     * Get a long value from the random number generator
     * @return the next value in the sequence from the random number generator
     */
    public static long getRandomLong() {
        return random.get().nextLong();
    }        
}

