package gov.usgs.volcanoes.vdx.server;

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
	
	/**
	 * Constructor
	 * @param s list of strings for result
	 */
	public TextResult(List<String> s)
	{
		super();
		strings = s;
	}
	
	/**
	 * Add a string to result
	 * @param s string to add
	 */
	public void add(String s)
	{
		strings.add(s);
	}
	
	/**
	 * Get result ready for writing
	 */
	public void prepare()
	{
		set("lines", Integer.toString(strings.size()));
	}
	
	/**
	 * Yield the contents
	 * @return list of strings
	 */
	public List<String> getStrings()
	{
		return strings;
	}
	
	/**
	 * Write data
	 * @param netTools tools to use for writing
	 * @param channel channel to write to
	 */
	protected void writeBody(NetTools netTools, SocketChannel channel)
	{
		for (String s : strings)
			netTools.writeString(s + "\n", channel);
	}

}
