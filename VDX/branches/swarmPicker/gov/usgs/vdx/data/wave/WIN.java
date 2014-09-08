/*
 * This code is derived from:
 * 
 * The TauP Toolkit: Flexible Seismic Travel-Time and Raypath Utilities. Copyright (C) 1998-2000 University of South
 * Carolina
 * 
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 * 
 * The current version can be found at <A HREF="www.seis.sc.edu">http://www.seis.sc.edu</A>
 * 
 * Bug reports and comments should be directed to H. Philip Crotwell, crotwell@seis.sc.edu or Tom Owens,
 * owens@seis.sc.edu
 */

package gov.usgs.vdx.data.wave;

import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.logging.Logger;


/**
 * A class to read WIN files, adapted from Fissures code, via WIN.java
 * 
 * @author Joel Shellman
 */
public class WIN {
    protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.wave.WIN");


    public static class ChannelData {
        public int packetSize;
        public int year;
        public int month;
        public int day;
        public int hour;
        public int minute;
        public int second;
        public int channel_num;
        public int data_size;
        public float sampling_rate;
        public List<Integer> in_buf;

        public ChannelData() {}

        public ChannelData(ChannelData copy) {
            this.packetSize = copy.packetSize;
            this.year = copy.year;
            this.month = copy.month;
            this.day = copy.day;
            this.hour = copy.hour;
            this.minute = copy.minute;
            this.second = copy.second;
            this.channel_num = copy.channel_num;
            this.data_size = copy.data_size;
            this.sampling_rate = copy.sampling_rate;
        }
    }

    private Map<Integer, List<ChannelData>> channelMap = new TreeMap<Integer, List<ChannelData>>();
    public static TimeZone timeZoneValue;
    public static boolean useBatch;
    public static boolean isWIN;

    /**
     * reads the WIN file specified by the filename.
     * 
     * @throws IOException
     *             if not found or some read error
     */
    public int read(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(filename);
        BufferedInputStream buf = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(buf);
        while (dis.available() != 0) {
            ChannelData cur = new ChannelData();
            readHeader(cur, dis);
            readData(cur, dis);
        }
        dis.close();
        return channelMap.size();
    }


    private ByteBuffer converter2 = ByteBuffer.wrap(new byte[2]);
    private ByteBuffer converter4 = ByteBuffer.wrap(new byte[4]);
    private TimeZone timeZone;

    static int intFromSingleByte(byte b) {
        return b;
    }

    private int intFromFourBytes(byte[] bites) {
        converter4.clear();
        converter4.mark();
        converter4.put(bites);
        converter4.rewind();
        return converter4.getInt();
    }

    private int intFromThreeBytes(byte[] bites) {
        byte pad = (byte)((bites[0] < 0) ? -1 : 0);
        byte[] padded = new byte[] { pad, bites[0], bites[1], bites[2] };
        return intFromFourBytes(padded);
    }

    private short shortFromTwoBytes(byte[] bites) {
        converter2.clear();
        converter2.mark();
        converter2.put(bites);
        converter2.rewind();
        return converter2.getShort();
    }

    private static int decodeBcd(byte[] b) {
        StringBuffer buf = new StringBuffer(b.length * 2);
        for (int i = 0; i < b.length; ++i) {
            buf.append((char)(((b[i] & 0xf0) >> 4) + '0'));
            if ((i != b.length) && ((b[i] & 0xf) != 0x0A)) buf.append((char)((b[i] & 0x0f) + '0'));
        }
        return Integer.parseInt(buf.toString());
    }

    /**
     * reads the header from the given stream.
     * 
     * @param dis
     *            DataInputStream to read WIN from
     * @throws FileNotFoundException
     *             if the file cannot be found
     * @throws IOException
     *             if it isn't a WIN file
     */
    public void readHeader(ChannelData c, DataInputStream dis) throws FileNotFoundException, IOException {
        // read first 4 byte: file size
        byte[] fourBytes = new byte[4];
        byte[] oneByte = new byte[1];

        dis.readFully(fourBytes);
        c.packetSize = intFromFourBytes(fourBytes);

        // read next 6 bytes: yy mm dd hh mi ss
        dis.readFully(oneByte);
        c.year = 2000 + decodeBcd(oneByte);

        dis.readFully(oneByte);
        c.month = decodeBcd(oneByte);

        dis.readFully(oneByte);
        c.day = decodeBcd(oneByte);

        dis.readFully(oneByte);
        c.hour = decodeBcd(oneByte);

        dis.readFully(oneByte);
        c.minute = decodeBcd(oneByte);

        dis.readFully(oneByte);
        c.second = decodeBcd(oneByte);
    }

    /**
     * read the data portion of WIN format from the given stream
     * 
     * @param dis
     *            DataInputStream to read WIN from
     * @throws IOException
     *             if it isn't a WIN file
     */
    public void readData(final ChannelData header, final DataInputStream dis) throws IOException {
        int bytesRead = 10;

        do {
            ChannelData c = new ChannelData(header);
            c.in_buf = new ArrayList<Integer>();
            byte[] oneByte = new byte[1];
            dis.readFully(oneByte);

            dis.readFully(oneByte);
            c.channel_num = intFromSingleByte(oneByte[0]);
            
            dis.readFully(oneByte);
            byte sampleRateUpperBits = (byte)(oneByte[0] & 0xF);
            c.data_size = intFromSingleByte(oneByte[0]) >> 4;

            dis.readFully(oneByte);
            c.sampling_rate = intFromSingleByte(oneByte[0]) + (sampleRateUpperBits << 4);

            byte[] fourBytes = new byte[4];
            dis.readFully(fourBytes);
            int accum = intFromFourBytes(fourBytes);

            c.in_buf.add(accum);

            float[] d = new float[(int)c.sampling_rate - 1];

            bytesRead += 8;
            if (c.data_size == 0) {
                for (int ix = 0; ix < ((int)c.sampling_rate - 1); ix++) {
                    accum += dis.readByte();
                    c.in_buf.add(accum);

                }
            } else if (c.data_size == 1) {
                for (int ix = 0; ix < ((int)c.sampling_rate - 1); ix++) {
                    accum += dis.readByte();
                    c.in_buf.add(accum);
                    bytesRead++;
                }
            } else if (c.data_size == 2) {
                byte[] twoBytes = new byte[2];
                for (int ix = 0; ix < ((int)c.sampling_rate - 1); ix++) {
                    dis.readFully(twoBytes);
                    accum += shortFromTwoBytes(twoBytes);
                    d[ix] = accum;
                    c.in_buf.add(accum);
                    bytesRead += 2;
                }
            } else if (c.data_size == 3) {
                byte[] threeBytes = new byte[3];
                for (int ix = 0; ix < ((int)c.sampling_rate - 1); ix++) {
                    dis.readFully(threeBytes);
                    accum += intFromThreeBytes(threeBytes);
                    d[ix] = accum;
                    c.in_buf.add(accum);
                    bytesRead += 3;
                }
            } else if (c.data_size == 4) {
                for (int ix = 0; ix < ((int)c.sampling_rate - 1); ix++) {
                    dis.readFully(fourBytes);
                    accum += intFromFourBytes(fourBytes);

                    d[ix] = accum;
                    c.in_buf.add(accum);
                    bytesRead += 4;
                }
            }
            List<ChannelData> list = channelMap.get(c.channel_num);
            if (list == null) {
            	list = new ArrayList<ChannelData>();
                channelMap.put(c.channel_num, list);
            }
            list.add(c);
        } while (bytesRead < header.packetSize);
    }

    /**
     * Create Wave object from WIN data
     * 
     * @return wave created
     */
    public Wave[] toWave() {
        Wave[] waves = new Wave[channelMap.size()];
        int i=0;
        for (List<ChannelData> channels : channelMap.values()) {
        	List<Wave> subParts = new ArrayList<Wave>(channels.size());
        	for (ChannelData c : channels) {
        		subParts.add(toWave(c));
        	}
        	waves[i++] = Wave.join(subParts, false);
        }
        return waves;
    }

    private Wave toWave(ChannelData c) {
        Wave sw = new Wave();
        sw.setStartTime(Util.dateToJ2K(getStartTime(c)));
        sw.setSamplingRate(c.sampling_rate);
        sw.buffer = new int[c.in_buf.size()];
        for (int j = 0; j < c.in_buf.size(); j++) {
            sw.buffer[j] = Math.round(c.in_buf.get(j));
        }
        return sw;
    }
    
    /**
     * Get start time of data
     * 
     * @return start time of data
     */
    public Date getStartTime(ChannelData c) {
        /*timeZone = "GMT";
        if (timeZoneValue < 0) {
            timeZone += Integer.toString(timeZoneValue) + ":00";
        } else if (timeZoneValue > 0) {
            timeZone += "+" + timeZoneValue + ":00";
        }*/
        timeZone = timeZoneValue;
        Calendar cal = Calendar.getInstance(timeZone);
        cal.setTimeInMillis(0);
        cal.set(Calendar.YEAR, c.year);
        cal.set(Calendar.MONTH, c.month - 1);
        cal.set(Calendar.DAY_OF_MONTH, c.day);
        cal.set(Calendar.HOUR_OF_DAY, c.hour);
        cal.set(Calendar.MINUTE, c.minute);
        cal.set(Calendar.SECOND, c.second);
//        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        return cal.getTime();
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    // /** just for testing. Reads the filename given as the argument,
    // * writes out some header variables and then
    // * writes it back out as "outWINfile".
    // * @param args command line args
    // */
    // public static void main(String[] args)
    // {
    // WIN data = new WIN();
    //
    // if (args.length != 1)
    // {
    // System.out.println("Usage: java gov.usgs.vdx.data.wave.WIN WINsourcefile ");
    // //return;
    // }
    //
    // try
    // {
    // data.read(args[0]);
    // data.printHeader();
    // System.out.println(Util.formatDate(data.getStartTime()));
    // System.out.println(data.getSamplingRate());
    // Wave sw = data.toWave();
    // System.out.println(sw);
    // }
    // catch (FileNotFoundException e)
    // {
    // System.out.println("File " + args[0] + " doesn't exist.");
    // }
    // catch (IOException e)
    // {
    // System.out.println("IOException: " + e.getMessage());
    // }
    // }
}