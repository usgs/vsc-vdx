package gov.usgs.volcanoes.vdx.in;

import java.util.*;

/**
 * A class that manages thread timing for periodic polling.
 */
abstract public class Poller extends Thread
{
	/** convenience variable, the number of milliseconds in a day */
    public static final int ONE_DAY = 24 * 60 * 60 * 1000;
    
    /** the number of milliseconds between polls */
    protected long interval;
    
    /** the number of milliseconds until the next poll.  If you want to start polling at a 
     * certain time then do regular interval polling after that time you can use this variable */
    protected long nextInterval;

	/** whether or not this thread is current polling */
    protected boolean stopped;
    
    /** a flag to let the thread know that it should die */
    private boolean killThread;
    
    /** Creates a new stopped, alive Poller and starts the Thread.  This does NOT start the polling
     * it just gets the Thread started; you must call startPolling to start the polling. */
    public Poller()
    {
        super();
        stopped = true;
        killThread = false;
        this.setName("Poller");
        start();
    }
    
    /** This function is called every time the polling time comes around.
     * @param filename name of file to process
     */
    abstract public void process(String filename);
    
    /** Kills the Poller.
     */
    public void kill()
    {
        killThread = true;
        interrupt();
    }
    
    /** Stops polling but doesn't kill the Poller.  Polling can then be resumed. */
    public void stopPolling()
    {
        stopped = true;
        interrupt();
    }
    
    /** Starts the polling. */
    public void startPolling()
    {
        stopped = false;
    }
    
    /** The Thread's run() method.  This takes care of the polling timing and the calling
     * of the poll() method. */
    public void run()
    {
        while (true)
        {
            if (!stopped)
            {
                try { Thread.sleep(nextInterval); } catch (InterruptedException ex) {}
                nextInterval = interval;
                if (!stopped && !killThread)
                    process("");
            }
            else
            {
                try { Thread.sleep(100); } catch (InterruptedException ex) {}
            }
    
            if (killThread)
                return;
        }
    }
    
    /** Convenience method for setting the time until the next polling based on a specific time.
     * @param hr the hour
     * @param min the minute 
     * @param sec the second
     * @param ms the millisecond
     */
    public void setNextAsTime(int hr, int min, int sec, int ms)
    {
        Calendar cal = Calendar.getInstance();
        Date now = new Date();
        cal.setTime(now);
        cal.set(Calendar.HOUR_OF_DAY, hr);
        cal.set(Calendar.MINUTE, min);
        cal.set(Calendar.SECOND, sec);
        cal.set(Calendar.MILLISECOND, ms);
        if (cal.getTime().getTime() <= now.getTime()) 
            cal.add(Calendar.DAY_OF_YEAR, 1);
        nextInterval = cal.getTime().getTime() - now.getTime();   
    }
}
