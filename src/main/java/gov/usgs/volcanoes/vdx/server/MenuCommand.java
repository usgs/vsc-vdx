package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.legacy.net.NetTools;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.data.DataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.log4j.Level;

/**
 * Comand to retrieve menu description data.
 *
 * @author Dan Cervelli
 */
public class MenuCommand extends BaseCommand {

  /**
   * Constructor.
   *
   * @param sh server handler
   * @param nt net tools
   */
  public MenuCommand(ServerHandler sh, NetTools nt) {
    super(sh, nt);
  }

  /**
   * Perform command actions, write result to channel.
   *
   * @param info params
   * @param channel where to write to
   */
  public void doCommand(Object info, SocketChannel channel) {
    handler.log(Level.DEBUG, "[menu]", channel);
    parseParams((String) info);
    DataSourceHandler dsh = handler.getDataSourceHandler();
    List<DataSourceDescriptor> dsds = dsh.getDataSources();
    for (DataSourceDescriptor dsd : dsds) {
      DataSource ds = dsd.getDataSource();
      netTools.writeString(
          "source=" + dsd.getClassName() + "; description=" + dsd.getDescription() + "; "
              + "type=" + ds.getType() + ";", channel);

      netTools.writeString("\n", channel);
    }
  }
}
