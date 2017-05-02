# VDX
---
Valve Data Exchange (VDX) is collection of Java command-line applications and services developed for the intended purposes of inserting volcano-monitoring data into and pulling said data out of a database. While a VDX installation is often paired with a Valve3 web application installation, it's not necessary.

## Installation
---
1. Start by getting the MySQL instance ready for VDX use:  
    ```
    mysql> grant all on `v3\_%`.* to vdx@'localhost' identified by 'vdx';
    Query OK, 0 rows affected, 2 warnings (0.00 sec)
    ```
    Feel free to use whichever user & host combination that works for your setup. Just make sure that your VDX.config (see below) is configured correctly.
2. Now setup VDX:
   1. Build or otherwise obtain the vdx-bin.tar.gz archive.
   2. Extract the archive to the location you want to install it.
   3. Rename the various dist-* files in the base directory to remove the 'dist-'.
   4. Configure VDX.config with the correct host and user/password combo to connect to the database:  
        ```
        vdx.url=jdbc:mysql://127.0.0.1/?user=vdx&password=vdx
        ```
   5. Create the root database:
        ```
        $ cd ~/vdx
        $ java -cp lib/vdx.jar gov.usgs.volcanoes.vdx.db.VDXDatabase -c VDX.config -a createvdx
        ```
        That's it! The system is now setup to import data via the various importers (see below) or install Valve as the front-end.
3. In order to run the VDX service, you can use the supplied shell script or run the Java command yourself:
    ```
    $ java -cp lib/vdx.jar -Xmx512M gov.usgs.volcanoes.vdx.server.VDX --noinput > log/vdx.log 2>&1 &
    ```
If everything works correctly, the log file should be free of errors and VDX should be listening on whichever port was configured in the VDX.config file.

## Importing Data via File
---
There are several ways to import data with VDX. Doing so via a csv file containing data is the most straightforward and easiest to explain. For a full description of the other import methods, as well as all of the configuration options that will be shown below, please see the included documentation.

In order to use VDX to import data, the following must be true:
  * The VDX service must be running.
  * The dataset being imported must have an entry in vdxSources.config.
  * The dataset being imported must have associated config files, usually stored in the VDX_BASE/config directory.

For example, importing a dataset consisting of SO2 data might have a configuration that looks like the following:
* vdxSources.config  
    ```
    source=gravitydata
    gravitydata.class=gov.usgs.volcanoes.vdx.data.generic.fixed.SQLGenericFixedDataSource
    gravitydata.description=Gravity
    gravitydata.vdx.name=gravitydata
    ```

* VDX_BASE/config/datasource.gravitydata.config  
    ```
    dataSource=gravitydata

    gravitydata.channels=MSUM,BASE

    channel=MSUM
    MSUM.name=Mountain Summit
    MSUM.latitude=12.345678
    MSUM.longitude=-123.456789

    channel=BASE
    BASE.name=Base
    BASE.latitude=12.98765
    BASE.longitude=-123.987654

    gravitydata.column=gravity
    gravitydata.gravity.idx=1
    gravitydata.gravity.description=Gravity
    gravitydata.gravity.unit=Milligals
    gravitydata.gravity.checked=1
    gravitydata.gravity.active=1

    gravitydata.column=crosslevel
    gravitydata.crosslevel.idx=2
    gravitydata.crosslevel.description=Cross Level
    gravitydata.crosslevel.unit=Counts
    gravitydata.crosslevel.checked=0
    gravitydata.crosslevel.active=1

    gravitydata.column=longlevel
    gravitydata.longlevel.idx=3
    gravitydata.longlevel.description=Long Level
    gravitydata.longlevel.unit=Counts
    gravitydata.longlevel.checked=0
    gravitydata.longlevel.active=1
    ```

* VDX_BASE/config/importFile.gravitydata.config  
    ```
    vdx.config=/path/to/VDX.config

    @include datasource.gravitydata.config
    MSUM.fields=TIMESTAMP,gravity,crosslevel,longlevel
    BASE.fields=TIMESTAMP,gravity,crosslevel,longlevel

    fields=TIMESTAMP,gravity
    filemask=CCCC
    timestamp=M/d/yyyy HH:mm:ss
    ```

* File with data to be imported (MSUM.csv)
    ```
    4/1/2017 00:10:00,0.123,33000,32000
    4/1/2017 00:20:00,0.123,33000,32000
    4/1/2017 00:30:00,0.123,33000,32000
    4/1/2017 00:40:00,0.123,33000,32000
    4/1/2017 00:50:00,0.123,33000,32000
    4/1/2017 01:00:00,0.123,33000,32000
    4/1/2017 01:10:00,0.123,33000,32000
    4/1/2017 01:20:00,0.123,33000,32000
    4/1/2017 01:30:00,0.123,33000,32000
    4/1/2017 01:40:00,0.123,33000,32000
    ```

Now just run the importer:  
```
java -cp lib/vdx.jar gov.usgs.volcanoes.vdx.in.ImportFile -c config/importFile.gravitydata.config MSUM.csv
```
If the database doesn't already exist, it'll be created and then the data will be inserted. A successful import will produce output like the following:  
```
2017-04-07 12:47:22: (INFO) Reading config file config/importFile.gravitydata.config
2017-04-07 12:47:22: (INFO) filemask:CCCC/headerlines:0/delimiter:,
2017-04-07 12:47:22: (INFO) [Rank] Raw Data
2017-04-07 12:47:22: (INFO)
2017-04-07 12:47:22: (INFO) [DataSource] gravitydata
2017-04-07 12:47:23: (INFO) [Columns] gravity,crosslevel,longlevel
2017-04-07 12:47:23: (INFO) [Channels]MSUM,BASE
2017-04-07 12:47:23: (INFO)
2017-04-07 12:47:23: (INFO) importing: MSUM.csv
```
... and in MySQL:  
```
mysql> use v3_gravitydata$genericfixed;
mysql> select * from MSUM;
+-----------+---------+------------+-----------+-----+-----+
| j2ksec    | gravity | crosslevel | longlevel | tid | rid |
+-----------+---------+------------+-----------+-----+-----+
| 544277400 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544278000 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544278600 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544279200 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544279800 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544280400 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544281000 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544281600 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544282200 |   0.123 |      33000 |     32000 |   2 |   1 |
| 544282800 |   0.123 |      33000 |     32000 |   2 |   1 |
+-----------+---------+------------+-----------+-----+-----+
10 rows in set (0.00 sec)
```
