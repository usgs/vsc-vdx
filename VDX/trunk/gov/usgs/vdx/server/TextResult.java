package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;

import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class TextResult extends RequestResult
{
	protected List<String> strings;
	
	public TextResult(List<String> s)
	{
		super();
		strings = s;
	}
	
	public void add(String s)
	{
		strings.add(s);
	}
	
	public void prepare()
	{
		set("lines", Integer.toString(strings.size()));
	}
	
	public List<String> getStrings()
	{
		return strings;
	}
	
	protected void writeBody(NetTools netTools, SocketChannel channel)
	{
		for (String s : strings)
			netTools.writeString(s + "\n", channel);
	}

}
