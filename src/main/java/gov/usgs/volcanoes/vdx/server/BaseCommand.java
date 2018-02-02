package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.legacy.net.Command;
import gov.usgs.volcanoes.core.legacy.net.NetTools;
import gov.usgs.volcanoes.core.util.StringUtils;

import java.nio.channels.SocketChannel;
import java.util.Map;

import org.apache.log4j.Level;

/**
 * <p> Base abstract class to represent command performed by server. All concrete command should
 * derived from this class. </p> $Log: not supported by cvs2svn $
 *
 * @author Dan Cervelli
 */
public abstract class BaseCommand implements Command {

  protected NetTools netTools;
  protected ServerHandler handler;

  protected Map<String, String> inParams;
  protected Map<String, String> outParams;

  /**
   * Constructor.
   *
   * @param sh server handler
   * @param nt net tools
   */
  public BaseCommand(ServerHandler sh, NetTools nt) {
    handler = sh;
    netTools = nt;
  }

  /**
   * Throw and log error.
   *
   * @param msg error message
   * @param cmd command
   * @param channel socket channel
   */
  public void sendError(String msg, String cmd, SocketChannel channel) {
    netTools.writeString("error: " + msg + "\n", channel);
    handler.log(Level.DEBUG, String.format("[%s] error: %s", cmd, msg), channel);
  }

  /**
   * Parse command and comstruct parameters map.
   *
   * @param cmd command to parse
   */
  // TODO: allow \s for semicolons?
  public void parseParams(String cmd) {
    int ci = cmd.indexOf(":");
    if (ci == -1) {
      return;
    }
    String paramString = cmd.substring(ci + 1);
    inParams = StringUtils.stringToMap(paramString);
  }
}
