package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.legacy.net.CommandHandler;
import gov.usgs.volcanoes.core.legacy.net.NetTools;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Level;

/**
 * Extended CommandHandler, a class that handles server requests on a separate thread. Designed to
 * handle VDX requests.
 *
 * @author Dan Cervelli
 */
public class ServerHandler extends CommandHandler {

  private static AtomicInteger instances = new AtomicInteger(0);
  private VDX vdx;
  private DataSourceHandler dataSourceHandler;
  private NetTools netTools;

  /**
   * Constructor.
   *
   * @param s VDX server
   */
  public ServerHandler(VDX s) {
    super(s, "VDX/ServerHandler-" + instances.getAndIncrement());
    vdx = s;
    netTools = new NetTools();
    dataSourceHandler = new DataSourceHandler(vdx.getDbDriver(), vdx.getDbUrl(), vdx.getPrefix());
    setupCommandHandlers();
  }

  /**
   * Fill possible commands for VDX server.
   */
  protected void setupCommandHandlers() {
    addCommand("version", new BaseCommand(this, netTools) {
      public void doCommand(Object info, SocketChannel channel) {
        parseParams((String) info);
        netTools.writeString("version=1.0.0\n", channel);
        vdx.log(Level.DEBUG, "version", channel);
      }
    });

    addCommand("menu", new MenuCommand(this, netTools));
    addCommand("getdata", new GetDataCommand(this, netTools));
  }

  /**
   * Yield data source handler.
   *
   * @return data source handler computed from vdx parameters
   */
  public DataSourceHandler getDataSourceHandler() {
    return dataSourceHandler;
  }

  /**
   * Write log message.
   *
   * @param level level of message
   * @param msg message text
   * @param channel where to write to
   */
  public void log(Level level, String msg, SocketChannel channel) {
    vdx.log(level, msg, channel);
  }
}
