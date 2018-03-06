package gov.usgs.volcanoes.vdx.in.tilt;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

import gov.usgs.volcanoes.core.data.GenericDataMatrix;
import gov.usgs.volcanoes.vdx.data.tilt.SQLTiltDataSource;
import gov.usgs.volcanoes.vdx.data.tilt.ptx.PtxRecord;
import gov.usgs.volcanoes.vdx.db.VDXDatabase;

import java.io.DataInputStream;
import java.io.FileInputStream;

/**
 * Program to import Pinnacle series 5000 tiltmeter PTX files.
 *
 * @author Dan Cervelli
 */
public class ImportPtx {

  /**
   * Main method, expect file names to proceed in the command line.
   */
  public static void main(String[] args) throws Exception {
    SQLTiltDataSource dataSource = new SQLTiltDataSource();
    VDXDatabase database = VDXDatabase.getVDXDatabase("vdx.config");
    String channel = args[1];
    // TODO: work out new initialization
    // dataSource.setDatabase(database);
    // dataSource.setName(args[0]);

    byte[] buffer = new byte[1024];
    boolean done = false;
    // long lastTime = 0;
    for (int j = 2; j < args.length; j++) {
      System.out.println("file: " + args[j]);
      done = false;
      DataInputStream dis = new DataInputStream(new FileInputStream(args[j]));
      while (!done) {

        try {
          int read = 0;
          while (read < 1024) {
            read += dis.read(buffer, read, 1024 - read);
          }

          PtxRecord ptx = new PtxRecord(buffer);
          double[][] data = ptx.getImportData();
          for (int i = 0; i < data.length; i++) {
            DoubleMatrix2D dm = DoubleFactory2D.dense.make(1, 5);
            dm.setQuick(0, 0, data[i][0]);
            dm.setQuick(0, 1, data[i][1]);
            dm.setQuick(0, 2, data[i][2]);
            dm.setQuick(0, 3, data[i][3]);
            dm.setQuick(0, 4, data[i][4]);
            String[] columnNames = {"j2ksec", "xTilt", "yTilt", "instVolt", "holeTemp"};
            GenericDataMatrix gdm = new GenericDataMatrix(dm);
            gdm.setColumnNames(columnNames);
            dataSource.defaultInsertData(channel, gdm, dataSource.getTranslationsFlag(),
                dataSource.getRanksFlag(), 1);
          }
        } catch (Exception e) {
          done = true;
        }
      }
      dis.close();
    }
  }
}
