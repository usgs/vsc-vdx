package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.util.Util;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
abstract public class RequestResult
{
	protected boolean error;
	protected Map<String, String> parameters;
	
	public RequestResult()
	{
		parameters = new HashMap<String, String>();
		error = false;
	}
	
	public Map<String, String> getParameters()
	{
		return parameters;
	}
	
	public void set(String k, String v)
	{
		parameters.put(k, v);
	}
	
	public void setError(boolean error){
		this.error = error;
	}
			
	
	public void prepare()
	{}
	
	public void writeHeader(NetTools netTools, SocketChannel channel)
	{
		String resp = error ? "error: " : "ok: " + Util.mapToString(parameters) + "\n";
		netTools.writeString(resp, channel);
	}
	
	abstract protected void writeBody(NetTools netTools, SocketChannel channel);
}
