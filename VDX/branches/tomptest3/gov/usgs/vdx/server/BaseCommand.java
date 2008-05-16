package gov.usgs.vdx.server;

import gov.usgs.net.Command;
import gov.usgs.net.NetTools;
import gov.usgs.util.Util;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
abstract public class BaseCommand implements Command
{
	protected NetTools netTools;
	protected ServerHandler handler;
	
	protected Map<String, String> inParams;
	protected Map<String, String> outParams;
	
	public BaseCommand(ServerHandler sh, NetTools nt)
	{
		handler = sh;
		netTools = nt;
	}

	public void sendError(String msg, String cmd, SocketChannel channel)
	{
		netTools.writeString("error: " + msg + "\n", channel);
		handler.log(Level.FINE, String.format("[%s] error: %s", cmd, msg), channel);
	}
	
	// TODO: allow \s for semicolons?
	public void parseParams(String cmd)
	{
		int ci = cmd.indexOf(":");
		if (ci == -1)
			return;
		String paramString = cmd.substring(ci + 1);
		inParams = Util.stringToMap(paramString);
	}
}
