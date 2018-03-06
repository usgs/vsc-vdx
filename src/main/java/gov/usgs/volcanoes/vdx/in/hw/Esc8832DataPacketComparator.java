package gov.usgs.volcanoes.vdx.in.hw;

import java.util.Comparator;

/**
 * Esc8832DataPacketComparator compares two packets, first by date then by channel.
 *
 * @author lantolik
 */
class Esc8832DataPacketComparator implements Comparator<Object> {

  public int compare(Object a, Object b) {
    int dateCompare = ((Esc8832DataPacket) a).dataDate.compareTo(((Esc8832DataPacket) b).dataDate);
    if (dateCompare != 0) {
      return dateCompare;
    } else {
      return ((Esc8832DataPacket) a).dataChannel.compareTo(((Esc8832DataPacket) b).dataChannel);
    }
  }
}

