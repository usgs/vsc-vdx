package gov.usgs.vdx.in.gps;

import java.io.*;
import java.util.Scanner;

class UnavcoPosToSQL {
    public static void main(String[] args) throws IOException {
        Scanner s = null;
        String code;
        double t;
        double[] sigma;
        sigma = new double[6];
        int j;
        if (args.length != 3) {
                System.out.println("Usage: java UnavcoPosToSQL source-name rankID posFile");
                return;
        }

        try {
                s = new Scanner(new BufferedReader(new FileReader(args[2])));

                s.nextLine(); s.nextLine();     s.next(); s.next();
            
                code = s.next();
            
                s.nextLine(); s.nextLine(); s.nextLine(); s.nextLine(); s.nextLine(); s.nextLine(); s.next(); s.next(); s.next(); s.next();

                System.out.printf("INSERT IGNORE INTO channels (code,lat,lon,height) VALUES (\"%s\",%s,%s,%s);\n",code,s.next(),s.next(),s.next());
                System.out.printf("SELECT cid FROM channels WHERE code=\"%s\" INTO @channelID;\n",code);
                System.out.printf("UPDATE channels SET lon=lon-360 WHERE code=\"%s\" AND lon>180;\n",code);
                s.nextLine();
                while(s.hasNext()) {
                        s.next(); s.next();
                        t = (s.nextDouble()-51545)*86400;
                        System.out.printf("INSERT IGNORE INTO sources (name,j2ksec0,j2ksec1,rid) VALUES (\"%s\",%f,%f,%s);\n",args[0],t,t+86400,args[1]);
                        System.out.printf("SELECT sid FROM sources WHERE name=\"%s\" AND j2ksec0=%f AND j2ksec1=%f AND rid=%s INTO @sourceID;\n",args[0],t,t+86400,args[1]);
                        System.out.printf("INSERT INTO solutions VALUES (@sourceID,@channelID,%s,%s,%s,",s.next(),s.next(),s.next());
                        for (j=0;j<6;j++) {
                                sigma[j] = s.nextDouble();
                        }
                        System.out.printf("%.17e,%.17e,%.17e,%.17e,%.17e,%.17e);\n",sigma[0]*sigma[0],sigma[1]*sigma[1],sigma[2]*sigma[2],sigma[3]*sigma[0]*sigma[1],sigma[4]*sigma[0]*sigma[2],sigma[5]*sigma[1]*sigma[2]);
                        s.nextLine();

                }
        } finally {
            if (s != null) {
                s.close();
            }
        }
    }
}