package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.DataSourceDescriptor;
import gov.usgs.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.logging.Level;

/**
 * Comand to retrieve menu description data. 
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class MenuCommand extends BaseCommand
{
	/**
	 * Constructor
	 */
	public MenuCommand(ServerHandler sh, NetTools nt)
	{
		super(sh, nt);
	}

	/**
	 * Perform command actions, write result to channel
	 */
	public void doCommand(Object info, SocketChannel channel)
	{
		handler.log(Level.FINE, "[menu]", channel);
		parseParams((String)info);
		DataSourceHandler dsh = handler.getDataSourceHandler();
		List<DataSourceDescriptor> dsds = dsh.getDataSources();
		for (DataSourceDescriptor dsd : dsds)
		{
			DataSource ds = dsd.getDataSource();
			netTools.writeString("source=" + dsd.getName() + "; description=" + dsd.getDescription() + "; " +
					"type=" + ds.getType() + ";", channel);
			
//			List<String> extendedTypes = ds.getExtendedTypes();
//			if (extendedTypes != null)
//			{
//				netTools.writeString(" extendedTypes=", channel);
//				for (int i = 0; i < extendedTypes.size(); i++)
//				{
//					netTools.writeString(extendedTypes.get(i), channel);
//					if (i != extendedTypes.size() - 1)
//						netTools.writeString(",", channel);
//				}
//			}
			netTools.writeString("\n", channel);
		}
	}
}
