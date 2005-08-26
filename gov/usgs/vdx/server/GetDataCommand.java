package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.util.CodeTimer;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.DataSourceDescriptor;
import gov.usgs.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class GetDataCommand extends BaseCommand
{
	public GetDataCommand(ServerHandler sh, NetTools nt)
	{
		super(sh, nt);
	}
	
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
		
		if (result != null)
		{
			result.set("type", ds.getType());
			result.prepare();
			result.writeHeader(netTools, channel);
			result.writeBody(netTools, channel);
			ct.stop(false);
			handler.log(Level.FINE, String.format("[getdata] %s (%d ms)", inParams.get("source"), ct.getRunTime()), channel);
		}
		else
		{
			netTools.writeString("error: no data\n", channel);
			handler.log(Level.FINE, "[getdata] returned nothing", channel);
		}
	}
}
