package gov.usgs.vdx.server;

import gov.usgs.net.Command;
import gov.usgs.net.NetTools;
import gov.usgs.util.Util;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.logging.Level;

/**
 * <p>
 * Base abstract class to represent command performed by server.
 * All concrete command should derived from this class.
 * </p>
 * @author Dan Cervelli
 */
abstract public class BaseCommand implements Command
{
	protected NetTools netTools;
	protected ServerHandler handler;
	
	protected Map<String, String> inParams;
	protected Map<String, String> outParams;
	
	/**
	 * Constructor
	 * @param sh server handler
	 * @param nt net tools
	 */
	public BaseCommand(ServerHandler sh, NetTools nt)
	{
		handler = sh;
		netTools = nt;
	}

	/**
	 * Throw and log error
	 * @param msg error message
	 * @param cmd command
	 * @param channel socket channel
	 */
	public void sendError(String msg, String cmd, SocketChannel channel)
	{
		netTools.writeString("error: " + msg + "\n", channel);
		handler.log(Level.FINE, String.format("[%s] error: %s", cmd, msg), channel);
	}
	
	/**
	 * Parse command and comstruct parameters map
	 * @param cmd command to parse
	 */
	// TODO: allow \s for semicolons?
	public void parseParams(String cmd)
	{
		int ci = cmd.indexOf(":");
		if (ci == -1)
			return;
		String paramString = cmd.substring(ci + 1);
		inParams = Util.stringToMap(paramString);
	}
	
	/**
	 * all VDX commands are a single line
	 * @return true if commands span more than a one line
	 */
	public boolean isMultiLine() {
	    return false;
	}
}
