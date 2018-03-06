package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.CodeTimer;
import gov.usgs.volcanoes.core.legacy.net.NetTools;
import gov.usgs.volcanoes.vdx.ExportConfig;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.data.DataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.apache.log4j.Level;

/**
 * Comand to retrieve data. Contains 'source' parameter do determine data source, and parameters set
 * to determine requested data.
 *
 * @author Dan Cervelli
 */
public class GetDataCommand extends BaseCommand {

  /**
   * Constructor.
   *
   * @param sh server handler
   * @param nt net tools
   */
  public GetDataCommand(ServerHandler sh, NetTools nt) {
    super(sh, nt);
  }

  /**
   * Perform command actions, write result to channel.
   *
   * @param info params
   * @param channel where to write to
   */
  public void doCommand(Object info, SocketChannel channel) {
    CodeTimer ct = new CodeTimer("send");
    parseParams((String) info);
    String source = inParams.get("source");
    if (source == null) {
      sendError("source not specified", "getdata", channel);
      return;
    }
    DataSourceHandler dsh = handler.getDataSourceHandler();
    String resultType;
    RequestResult result;
    String action = inParams.get("action");
    if (action != null && action.equals("exportinfo")) {
      ExportConfig ec = dsh.getExportConfig(source);
      resultType = action;
      if (ec == null || !ec.isClosed()) {
        int ncl = Integer.parseInt(inParams.get("numCommentLines"));
        ArrayList<String> args = new ArrayList<String>(ncl + 4);
        args.add(inParams.get("exportable"));
        args.add(inParams.get("width.0"));
        args.add(inParams.get("width.1"));
        //args.add( ""+ncl );
        for (int i = 1; i <= ncl; i++) {
          args.add(inParams.get("cmt." + i));
        }
        ExportConfig newEc = new ExportConfig(args);
        if (ec == null) {
          ec = newEc;
          dsh.putExportConfig(source, ec);
        } else {
          ec.underride(newEc);
        }
        ec.setClosed();
      }
      result = new TextResult(ec.toStringList());
    } else {
      DataSourceDescriptor dsd = dsh.getDataSourceDescriptor(inParams.get("source"));
      DataSource ds = dsd.getDataSource();

      result = ds.getData(inParams);
      dsd.putDataSource();
      resultType = ds.getType();
    }
    if (result != null) {
      result.set("type", resultType);
      result.prepare();
      result.writeHeader(netTools, channel);
      result.writeBody(netTools, channel);
      ct.stop();
      handler.log(Level.DEBUG,
          String.format("%s (%1.2f ms): [%s]", inParams.get("source"), ct.getRunTimeMillis(), info),
          channel);
    } else {
      netTools.writeString("error: no data\n", channel);
      handler.log(Level.DEBUG, "[getdata] returned nothing", channel);
    }
  }
}
