package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.util.CodeTimer;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.DataSourceDescriptor;
import gov.usgs.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.logging.Level;

/**
 * Comand to retrieve data. 
 * Contains 'source' parameter do determine data source,
 * and parameters set to determine requested data.
 *  
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/08/28 19:11:44  dcervelli
 * Fixes for new CodeTimer.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class GetDataCommand extends BaseCommand
{
	/**
	 * Constructor
	 */
	public GetDataCommand(ServerHandler sh, NetTools nt)
	{
		super(sh, nt);
	}
	
	/**
	 * Perform command actions, write result to channel
	 */
	public void doCommand(Object info, SocketChannel channel)
	{
		CodeTimer ct = new CodeTimer("send");
		parseParams((String)info);
		String source = inParams.get("source");
		if (source == null)
		{
			sendError("source not specified", "getdata", channel);
			return;
		}
		DataSourceHandler dsh = handler.getDataSourceHandler();
		DataSourceDescriptor dsd = dsh.getDataSourceDescriptor(inParams.get("source"));
		DataSource ds = dsd.getDataSource();
		RequestResult result = ds.getData(inParams);
		dsd.putDataSource();
		
		if (result != null)
		{
			result.set("type", ds.getType());
			result.prepare();
			result.writeHeader(netTools, channel);
			result.writeBody(netTools, channel);
			ct.stop(false);
			handler.log(Level.FINE, String.format("%s (%1.2f ms): [%s]", inParams.get("source"), ct.getRunTimeMillis(), info), channel);
		}
		else
		{
			netTools.writeString("error: no data\n", channel);
			handler.log(Level.FINE, "[getdata] returned nothing", channel);
		}
	}
}
