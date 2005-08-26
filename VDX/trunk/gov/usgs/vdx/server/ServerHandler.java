package gov.usgs.vdx.server;

import gov.usgs.net.CommandHandler;
import gov.usgs.net.NetTools;
import gov.usgs.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class ServerHandler extends CommandHandler
{
	private static int instances = 0;
	private VDX vdx;
	private DataSourceHandler dataSourceHandler;

	public ServerHandler(VDX s)
	{
		super(s, "VDX/ServerHandler-" + instances++);
		
		vdx = s;
		netTools = new NetTools(vdx.getLogger());
		dataSourceHandler = new DataSourceHandler(vdx.getLogger());
		setupCommandHandlers();
	}

	protected void setupCommandHandlers()
	{
		addCommand("version", new BaseCommand(this, netTools)
				{
					public void doCommand(Object info, SocketChannel channel)
					{
						parseParams((String)info);
						netTools.writeString("version=1.0.0\n", channel);
						vdx.log(Level.FINE, "version", channel);
					}
				});
		
		addCommand("menu", new MenuCommand(this, netTools));
		addCommand("getdata", new GetDataCommand(this, netTools));
	}
	
	public DataSourceHandler getDataSourceHandler()
	{
		return dataSourceHandler;
	}
	
	public void log(Level level, String msg, SocketChannel channel)
	{
		vdx.log(level, msg, channel);
	}
}
