package gov.usgs.vdx.in;

public class LilyIPConnection extends IPConnection {
	
	/** Creates a new LilyIPConnection given the specific IP address, port, and 
	 * data timeout
	 * @param i the IP address
	 * @param p the port
	 */
	public LilyIPConnection(String i, int p) {
		super(i, p);
	}
	
	/**
	 * Waits <code>timeout</code>sec for a message.
	 * Gets a message from the internal message queue
	 * <p>
	 *
	 * @param timeout the time (in milliseconds) to wait till a message is put into the queue
	 *		if timeout == -1, then no timeout, but wait for user interaction
	 * @exception Exception in case of timeout or broken connection.
	 *
	 * @return the oldest message from the queue
	 */
	protected String getMsg (long timeout) throws Exception {
			
		long start = System.currentTimeMillis();
		long end   = start + timeout;
		long now   = start;
		long delay = 10; //receiveTimeout;

		if ((timeout > 0) && (timeout < delay)) 
			delay = timeout;

		StringBuffer sb = new StringBuffer();
		while ( (now < end) || (-1L == timeout) ) {
			if (!lockQueue) {
				if (!msgQueue.isEmpty()) {
					sb.append(msgQueue.firstElement());
					msgQueue.removeElementAt (0);

					// the last char of a message must be <cr><lf>
					if (((char)'$' == sb.charAt(0)) && ((char)'\n' == sb.charAt(sb.length() - 1)))
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

}
