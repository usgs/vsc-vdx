package gov.usgs.volcanoes.vdx.in.hw;

import java.util.Date;

/**
 * Esc8832DataPacket defines an object that contains a data channel, date and value.
 *
 * @author lantolik
 */
class Esc8832DataPacket {

  public String dataChannel;
  public Date dataDate;
  public Double dataValue;

  public Esc8832DataPacket(String dataChannel, Date dataDate, Double dataValue) {
    this.dataChannel = dataChannel;
    this.dataDate = dataDate;
    this.dataValue = dataValue;
  }

  public String toString() {
    return this.dataChannel + "/" + this.dataDate + "/" + this.dataValue;
  }
}
