package gov.usgs.vdx.in;

import java.net.*;
import java.io.*;
import java.util.*;

/**
 * A class for handling String communication over an IP socket.
 *
 * @author Dan Cervelli
 * @version 1.2
 */
public class IPConnection extends Thread
{
	/** the IP address of the socket */
	protected String ip;
	
	/** the port number of the socket */
	protected int port;
	
	/** the actual socket for communication */
	private Socket socket;
	
	/** an output stream for the socket */
	private PrintWriter out;
	
	/** an input stream for the socket */
	private InputStreamReader in;
	
	/** a character buffer for reading from the socket */	
	private char buffer[];
	/** the size of the character buffer */
	private static final int BUFFER_SIZE = 2048;
	
	/** whether or not to echo incoming bytes from the socket to standard out */
	private boolean echo;
	
	/** whether or not to stop the thread from executing (used when quitting) */
	protected boolean stopThread = false;
	
	/** whether or not the socket is open */
	protected boolean open;
	
	/** an ordered message queue */
	protected Vector msgQueue;
	
	/** a mechanism to lock the queue.  Also used by subclasses. */
	protected boolean lockQueue;
	
	/** Creates an unopened IPConnection to the specified IP address and port.
	 * @param i the IP address
	 * @param p the port
	 */
	public IPConnection(String i, int p) 
	{
		super("IPConnection Thread");
		ip = i;
		port = p;
		buffer = new char[BUFFER_SIZE];
		msgQueue = new Vector(100);
		echo = false;
	}
	
	/** Sets whether or not to echo received bytes to standard out
	 * @param b the echo state
	 */
	public void setEcho(boolean b)
	{
		echo = b;
	}
	
	/** Opens the socket for communication.
	 * @throws UnknownHostException if the host (IP) can't be found
	 * @throws IOException if the socket fails to open
	 * @return whether or not the operation was successful
	 */
	public boolean open() throws UnknownHostException, IOException
	{
		boolean result = false;
		socket = new Socket(ip, port);
		out = new PrintWriter(socket.getOutputStream());
		in = new InputStreamReader(socket.getInputStream());
		result = true;
		open = true;
		stopThread = false;
		if (!this.isAlive())
			start();
		
		return result;
	}
	
	/** Closes the connection and stops waiting for bytes.
	 */
	public void close()
	{
		try
		{
			if (open)
			{
				open = false;
				stopThread = true;
				out.close();
				in.close();
				socket.close();
				msgQueue.removeAllElements();
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/** Gets the open state of this connection.
	 * @return the open state
	 */
	public boolean isOpen()
	{
		return open;
	}
	
	/** Thread run() implementation.  Just constantly trys reading bytes from the socket.
	 */
	public void run()
	{
		while (true)
		{
			try
			{
				if (open && !stopThread)
				{
					int count = in.read(buffer);
					receiveData(buffer, count);
				}
				else
					Thread.sleep(300);
			}
			catch (SocketException sockEx)
			{
				// this occurs whenever the socket closes, this is normal
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	/** Places received bytes in a message queue.
	 * @param data the received bytes
	 * @param count the number of bytes (note count != data.length)
	 */
	private synchronized void receiveData(char[] data, int count)
	{
		lockQueue = true;
		String msg = new String(data, 0, count);
		msgQueue.add(msg);
		if (echo)
			System.out.print(msg);
		lockQueue = false;
	}
	
	/** Gets the first message in the queue.
	 * @return the message
	 */
	public synchronized String getMessage()
	{
		String msg = null;
		if (msgQueue.size() != 0)
		{
			msg = (String)msgQueue.elementAt(0);
			msgQueue.removeElementAt(0);
		}
		return msg;
	}

	/** Writes a character to the socket.
	 * @param c the character
	 */	
	public void writeChar(char c)
	{
		out.print(c);
		out.flush();
	}

	/** Writes a string to the socket.
	 * @param s the string
	 */
	public void writeString(String s)
	{
		int len = s.length();
		int idx = 0;
		while (idx < len)
			out.write ((int)s.charAt(idx++));
		
		out.flush();
	}
}
