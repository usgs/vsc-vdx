package gov.usgs.vdx.in.conn;


import java.io.*;
import java.net.*;
import java.text.*;

/**
 * An extension of IPConnection for dealing with FreeWave communication over IP.
 *
 * @author Dan Cervelli, Ralf Krug
 * @version 1.2
 */
public class FreewaveIPConnection extends IPConnection
{
	/** number of seconds before a timeout waiting for an OK */
	private static final int WAIT4OK_TIMEOUT = 2;
	
	/** number of milliseconds to wait before a data timeout */
	private long receiveTimeout;
	
	/** whether or not the connection is in CCSAIL mode */
	private boolean CCSAILMode;
	
	/** a number format for properly dialing the radio phone number */
	private DecimalFormat radioNumberFormatter;
	
	/** Creates a new FreewaveIPConnection given the specific IP address, port, and 
	 * data timeout
	 * @param i the IP address
	 * @param p the port
	 * @param rto the data timeout
	 */
	public FreewaveIPConnection(String i, int p, long rto)
	{
		super(i, p);
		radioNumberFormatter = new DecimalFormat ("#######");
		receiveTimeout = rto;
	}
	
	/** Connects to a FreeWave.
	 * @param repeaterEntry the repeater entry number in the FreeWave callbook
	 * @param radioNumber the radio phone number
	 * @param timeout the timeout in milliseconds (-1 == none)
	 * @throws Exception various exceptions can be thrown with different messages depending on the outcome
	 */
	public void connect(int repeaterEntry, int radioNumber, int timeout) throws Exception
	{
		try
		{
			open();
		}
		catch (UnknownHostException e)
		{
			throw new Exception("Unknown host: " + ip + ":" + port);
		}
		catch (IOException e)
		{
			throw new Exception("Couldn't open socket input/output streams");
		}
		 
		setRepeater(repeaterEntry);
		call(radioNumber, timeout);
		CCSAILMode = true;
	}
	
	/** Disconnects from the FreeWave.
	 */
	public void disconnect()
	{
		close();
		CCSAILMode = false;
	}
	
	/** Calls the FreeWave.
	 * @param radioNumber the radio phone number
	 * @param establishConnectionTimeout the timeout (ms)
	 */
	private void call(int radioNumber, int establishConnectionTimeout) throws Exception
	{
		String cmd = "ATD" + radioNumberFormatter.format(radioNumber);

		writeString(cmd);
		wait4OK(); // throws FreewaveConnectionException if no 'OK' answer
		wait4Connect (establishConnectionTimeout);  // throws FreewaveConnectionException if no 'CONNECT' answer
	}
	
	/** Sets the repeater path.
	 * @param repeaterEntry the repeater entry in the FreeWave phone book
	 */
	private void setRepeater(int repeaterEntry) throws Exception
	{
		String cmd = "ATXC" + (char)('0' + repeaterEntry);
		writeString(cmd);
		wait4OK();
	}
	
	/** Waits for an OK from the FreeWave.
	 * @throws Exception if there was a problem
	 */
	private void wait4OK() throws Exception
	{
		String msg = getMsg ((long)WAIT4OK_TIMEOUT * 1000L); // throws SerialConnectionException if timeout

		if (0 != msg.indexOf ("OK"))
			throw new Exception ("'OK' expected but '" + msg + "' received");

		//sometimes "CONNECT" comes immediately after "OK"
		int idx = msg.indexOf ("CONNECT");
		if (-1 != idx)
			putMsg (msg.substring (idx)); // put the rest back to the message queue
	}
	
	/** Waits for a CONNECT from the FreeWave.
	 * @param establishConnectionTimeout the timeout (ms) (-1 == none)
	 * @throws Exception if there was a problem
	 */
	private void wait4Connect (int establishConnectionTimeout) throws Exception
	{
		// throws SerialConnectionException if timeout
		String msg = getMsg ((-1 == establishConnectionTimeout) ? -1L : ((long)establishConnectionTimeout));

		if (0 != msg.indexOf ("CONNECT"))
			throw new Exception ("'CONNECT' expected but '" + msg + "' received");
	}
	
	/**
	 * Waits <code>timeout</code>sec for a message.
	 * Gets a message from the internal message queue
	 * <p>
	 * In case <code>CCSAILMode</code> is high, this method behaves different:
	 * It waits for a complete CCSAIL-String.
	 * The CCSAIL string is complete, when the last char of a string is 0x03.
	 * The CCSAIL string might arrive in one or more single messages.
	 * Additionally the CD line is watched to see if the connection broke
	 *
	 * @param timeout the time (in milliseconds) to wait till a message is put into the queue
	 *		if timeout == -1, then no timeout, but wait for user interaction
	 * @exception Exception in case of timeout or broken connection.
	 *
	 * @return the oldest message from the queue
	 */
	protected String getMsg (long timeout) throws Exception
	{
		if (!open)
			throw new Exception("Connection not open.");
			
		long start = System.currentTimeMillis();
		long end   = start + timeout;
		long now   = start;
		long delay = 10; //receiveTimeout;

		if ((timeout > 0) && (timeout < delay)) 
			delay = timeout;

		StringBuffer sb = new StringBuffer();
		while ( (now < end) || (-1L == timeout) )
		{
			if (!lockQueue)
			{
				if (!msgQueue.isEmpty())
				{
					//msg += (String)msgQueue.firstElement();
					sb.append(msgQueue.firstElement());
					msgQueue.removeElementAt (0);

					// In CCSAIL mode the last char of a message must be (char)3
					if (!CCSAILMode || ((char)3 == sb.charAt (sb.length() - 1)) )
						return sb.toString();
				}
			}

			try {
				Thread.sleep (delay);
			} catch (InterruptedException e) {

			}

			now = System.currentTimeMillis();

		}

		String txt = "Timeout while waiting for data.";
		if (sb.length() > 0)
		{
			txt += " Already received: ";
			txt += sb.toString();
		}
		throw new Exception (txt);
	}

	/** Puts a message into the message queue.
	 * @param msg the message
	 * @return success state
	 */	
	protected boolean putMsg (String msg)
	{
		if (!open) return false;

		if (lockQueue) // is a new message arriving?
		{
			// wait till the end of the receive timeout
			try
			{
				Thread.sleep(receiveTimeout);
			}
			catch (InterruptedException e) {}

			// if the queue is still locked, something is going wrong...
			if (lockQueue) return false;
		}

		lockQueue = true;
		msgQueue.insertElementAt(new String (msg), 0);
		lockQueue = false;

		return true;
	}
}