package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.legacy.net.NetTools;
import gov.usgs.volcanoes.core.util.StringUtils;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * $Log: not supported by cvs2svn $.
 *
 * @author Dan Cervelli
 */
public abstract class RequestResult {

  protected boolean error;
  protected Map<String, String> parameters;

  /**
   * Constructor.
   */
  public RequestResult() {
    parameters = new HashMap<String, String>();
    error = false;
  }

  /**
   * Yield parameters.
   *
   * @return map of parameter names to values
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * Set paramater k to v.
   *
   * @param k name of parameter
   * @param v value of paramater
   */
  public void set(String k, String v) {
    parameters.put(k, v);
  }

  /**
   * Set error.
   *
   * @param error true if error, false otherwise
   */
  public void setError(boolean error) {
    this.error = error;
  }

  /**
   * Get result ready for writing.
   */
  public void prepare() {
  }

  /**
   * Write header.
   *
   * @param netTools tools to use for writing
   * @param channel channel to write to
   */
  public void writeHeader(NetTools netTools, SocketChannel channel) {
    String resp = error ? "error: " : "ok: " + StringUtils.mapToString(parameters) + "\n";
    netTools.writeString(resp, channel);
  }

  /**
   * Write data.
   *
   * @param netTools tools to use for writing
   * @param channel channel to write to
   */
  protected abstract void writeBody(NetTools netTools, SocketChannel channel);
}
