package cdg_taxi_distance_age;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CDG_Taxi_distance_ageV2 {

    public Connection establishOracleConnection() {
        System.out.println("-------- Oracle JDBC Connection Testing --------");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Oracle JDBC Driver?");
            return null;
        }
        System.out.println("Oracle JDBC Driver Registered!");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@YOURIPHERE:INSTANCE#:orcl", "sys AS SYSDBA",
                    "MYPASSWORD");

        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console. " + e.getMessage());
            return null;
        }

        if (connection != null) {
            System.out.println("You made it, take control of your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
        return connection;
    }
    
    public Connection establishHANAConnection() {
        System.out.println("-------- HANA JDBC Connection Testing --------");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(                    
                    "jdbc:sap://YOURIPHERE:INSTANCE#/?autocommit=false", "SCHEMA", "PASSWORD");
        } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error?");
            e.printStackTrace();
            return null;
        }
        if (connection != null) {
            System.out.println("Connection to HANA successful!");
        }
        return connection;
    }

    //run a query that does not need a result
    public boolean executeSQLUpdate(String myquery, Connection myc) {
        try {
            //System.out.println("----Beginning SQL_Update statement execution---------");
            Statement stmt = myc.createStatement();
//PreparedStatement stmt = myc.prepareStatement(myquery);
//stmt.setInt(1, 306);
stmt.executeUpdate(myquery);



            //stmt.executeUpdate(myquery);
            
            stmt.close();
            
            //System.out.println(">> " + myquery);
            //System.out.println("----SQL_Update statement completed---------");
            return true;
        } catch (SQLException e) {
            System.out.println("Query Failed! Check output console. " + e.getMessage());
            return false;
        }
    }
   
    //run a query to return a result to the console
    public void runQuery(String myquery, Connection myc){
        try {

            //step3 create the statement object  
            Statement stmt = myc.createStatement();

            //step4 execute query  
            ResultSet rs = stmt.executeQuery(myquery);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    //displayText.append(s + "\n");
                    if (i > 1) {
                        System.out.print(" ### ");
                    }                   
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            System.out.println("Connection Failed herp! Check output console. " + e.getMessage());
        }
    }
    
    //publish a query result to a text file
    public void writeQuery(String myquery, String fileName, Connection myc ){
        try {
            //create the statement object  
            Statement stmt = myc.createStatement();

            //execute query  
            ResultSet rs = stmt.executeQuery(myquery);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();        
            
            //Writer writer = null;           
            Writer writer = new BufferedWriter(new FileWriter(fileName, true));
            for (int j=1; j<=columnsNumber;j++)
            {
                writer.append(rsmd.getColumnName(j)+"###");
            }
            writer.append("\r\n");
            try 
            {
                while (rs.next()) 
                {
                    for (int i = 1; i <= columnsNumber; i++) 
                    {
                        if (i > 1) {
                            System.out.print("###");
                            writer.append("###");
                        }
                        String columnValue = rs.getString(i);
                        System.out.print(columnValue + " " + rsmd.getColumnName(i));
                        writer.append(columnValue);                      
                    }
                    System.out.println("");
                    writer.append("\r\n");
                    //writer.append(s + "\r\n");
                }
            } catch (IOException ex) {
                //Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("Could not write, error: " + ex.getMessage());
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException ex) {
                        Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Connection Failed herp! Check output console. " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Need to remove entries in GPS log that have same driver_id & log_dt
    public ArrayList<String> detectDuplicates(String myquery, Connection myc) {
        try {
            System.out.println("----------Beginning SQL_Query statement execution---------");
            //step3 create the statement object  
            Statement stmt = myc.createStatement();

            //step4 execute query  
            ResultSet rs = stmt.executeQuery(myquery);
            ArrayList<String> duplicates = new ArrayList<>();

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) {
                        //System.out.print(",  ");
                    }
                    String columnValue = rs.getString(i);
                    //System.out.print(columnValue + " " + rsmd.getColumnName(i));
                    duplicates.add(columnValue);
                }
                //System.out.println("");
            }
            //System.out.println(duplicates);
            System.out.println("----------SQL_Query statement completed---------");
            System.out.println("----Total Array size: " + duplicates.size());
            System.out.println(myquery);
            System.out.println("-------------------------------------------------");
            return duplicates;

        } catch (SQLException e) {
            System.out.println("detectDuplicates Connection Failed! Check output console. " + e.getMessage());
            return null;
        }
    }

    //This method runs through the GPS log per driver and selects the most recent position to add into the event log table
    public void gpsJoin(String myquery, Connection myc) {
        CDG_Taxi_distance_ageV2 myClass3 = new CDG_Taxi_distance_ageV2();

        ArrayList<Integer> drivers = myClass3.populateDriverArray();

        for (int i = 0; i < drivers.size(); i++) {
            myClass3.executeSQLUpdate("CREATE TABLE SYSTEM.ESC_WM_DRIVER_" + String.valueOf(drivers.get(i)) + "_TEMP AS SELECT * FROM SYSTEM.ESC_GPSLog_100drivers1month WHERE driver_id = " + String.valueOf(drivers.get(i)), myc);

            myClass3.executeSQLUpdate("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 el SET (GPS_EVENT_DT) = (SELECT max(gps.event_dt) FROM SYSTEM.ESC_WM_DRIVER_" + String.valueOf(drivers.get(i)) + "_TEMP gps WHERE gps.event_dt <= el.log_dt AND el.driver_id = " + String.valueOf(drivers.get(i)), myc);

        }

    }

    //adding coordinates and calculating distance and age of coordinates
    public void addCoordinates(ArrayList<Integer> drivers, Connection myc) {
        CDG_Taxi_distance_ageV2 myClass2 = new CDG_Taxi_distance_ageV2();
        //myConnection = myClass2.establishOracleConnection();       
        
        System.out.println("Now starting location_x...");
        //Setting location_x from header_info
        String myQuery = ("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 SET driver_locX = (to_number(substr(header_info, 15, 2) || substr(header_info, 13, 2), 'xxxx') / 100000 + 103.55)");
        myClass2.executeSQLUpdate(myQuery, myc);
        
        System.out.println("Finished updating location_x! Now starting location_y...");
            
        //Setting location_y from header_info
        String myQuery2 = ("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 SET driver_locY = (to_number(substr(header_info, 11, 2) || substr(header_info, 9, 2), 'xxxx') / 100000 + 1)");
        myClass2.executeSQLUpdate(myQuery2, myc);
        
        System.out.println("Finished updating location_y! Now calculating distance...");
        
        //Calculating the distance between the two locations at most recent GPS log_dt
        String myQuery3 = ("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 SET TAXI_DIST_AT_EVNT = (SQRT(power((driver_locX - pax_locX),2)+power((driver_locY - pax_locY),2))*111319.49079327358)");
        myClass2.executeSQLUpdate(myQuery3, myc);
        
        System.out.println("Finished calculating distance! Now calculating age of GPS EVENT_DT...");
        
        //Calculating the age of the gps log_dt as compared the the log_dt of the event log
        String myQuery4 = ("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 SET GPS_LOG_AGE_SEC = (((log_dt - gps_event_dt)*60*60*24))");
        myClass2.executeSQLUpdate(myQuery4, myc);
        
        System.out.println("Finished the table!!! Congrats!!!");        
        
    }

    //Only run-once
    public ArrayList<Integer> populateDriverArray() {
        ArrayList<Integer> drivers = new ArrayList<>();
        drivers.add(1828590);
        drivers.add(1518231);
        drivers.add(1168986);
        drivers.add(1183163);
        drivers.add(7402670);
        drivers.add(178012);
        drivers.add(1378809);
        drivers.add(623280);
        drivers.add(1630310);
        drivers.add(7524907);
        drivers.add(1678963);
        drivers.add(1116545);
        drivers.add(1111822);
        drivers.add(1196349);
        drivers.add(1201477);
        drivers.add(194367);
        drivers.add(213830);
        drivers.add(1656137);
        drivers.add(7115062);
        drivers.add(1461932);
        drivers.add(1312885);
        drivers.add(7627425);
        drivers.add(143454);
        drivers.add(906113);
        drivers.add(1611099);
        drivers.add(1520788);
        drivers.add(7041031);
        drivers.add(6846011);
        drivers.add(1188389);
        drivers.add(964406);
        drivers.add(1831280);
        drivers.add(1287228);
        drivers.add(1743679);
        drivers.add(1097340);
        drivers.add(1176177);
        drivers.add(7403432);
        drivers.add(1291879);
        drivers.add(6844443);
        drivers.add(1135500);
        drivers.add(7423355);
        drivers.add(317787);
        drivers.add(103847);
        drivers.add(2010719);
        drivers.add(747202);
        drivers.add(1493097);
        drivers.add(186017);
        drivers.add(1395868);
        drivers.add(217842);
        drivers.add(35991);
        drivers.add(49268);
        drivers.add(97280);
        drivers.add(46161);
        drivers.add(416414);
        drivers.add(7148792);
        drivers.add(180472);
        drivers.add(1414146);
        drivers.add(2573008);
        drivers.add(1695404);
        drivers.add(1688357);
        drivers.add(2018787);
        drivers.add(1238161);
        drivers.add(1606935);
        drivers.add(1123394);
        drivers.add(1457358);
        drivers.add(7790054);
        drivers.add(1243013);
        drivers.add(6843944);
        drivers.add(1203291);
        drivers.add(89192);
        drivers.add(707062);
        drivers.add(1440369);
        drivers.add(1591456);
        drivers.add(1381018);
        drivers.add(1431189);
        drivers.add(6911959);
        drivers.add(2179685);
        drivers.add(469848);
        drivers.add(65037);
        drivers.add(7228766);
        drivers.add(1572944);
        drivers.add(1132218);
        drivers.add(1390122);
        drivers.add(2182261);
        drivers.add(8305977);
        drivers.add(1155045);
        drivers.add(1833988);
        drivers.add(1718709);
        drivers.add(1156564);
        drivers.add(1210232);
        drivers.add(7246617);
        drivers.add(6811720);
        drivers.add(2174432);
        drivers.add(1743817);
        drivers.add(7109130);
        drivers.add(1553483);
        drivers.add(1362873);
        drivers.add(597055);
        drivers.add(1275673);
        drivers.add(86692);
        drivers.add(7912638);
        return drivers;
    }
   
    //substract header info from GPS log based on driver_id and log_dt
    public void pullInHeaderInfo (CDG_Taxi_distance_ageV2 myClass, Connection myc, ArrayList<Integer> drivers) {
        //drivers.size()
        for (int i = 0; i < drivers.size();i++)
        {
            long tStart = System.currentTimeMillis();
            System.out.println("Starting to match driver: " + (i+1) + "...");
            
            //creating temp table for each individual driver
            String myQuery = ("CREATE TABLE SYSTEM.ESC_wmtempdriver AS( SELECT * FROM SYSTEM.esc_gpslog_100drivers1month where driver_id = '" + drivers.get(i) + "') ORDER BY EVENT_DT");
            myClass.executeSQLUpdate(myQuery, myc);
            
            //matches temp table and brings in header info into EL
            String myQuery2 = ("UPDATE SYSTEM.ESC_NW_jobevent_100drivers2 a SET HEADER_INFO = (SELECT b.HEADER_INFO FROM SYSTEM.ESC_wmtempdriver b WHERE a.GPS_EVENT_DT = b.event_dt AND a.DRIVER_ID = b.DRIVER_ID) WHERE a.DRIVER_ID = '"+ drivers.get(i) + "'");
            myClass.executeSQLUpdate(myQuery2, myc);
            
            //delete temp table to be created in next iteration
            String myQuery3 = ("DROP TABLE SYSTEM.ESC_wmtempdriver");
            myClass.executeSQLUpdate(myQuery3, myc);
            
            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            double elapsedSeconds = tDelta / 1000.0;
            
            System.out.println("Num Drivers Completed: " + (i+1) + "/" + drivers.size());
            System.out.println("Driver " + drivers.get(i) + " took: " + elapsedSeconds + " seconds");
        }
    }
    
    //recursion to delete GPS duplicates...prevents opening too many statements
    public void slaveDriverForDeletingGPSDups(CDG_Taxi_distance_ageV2 myClass, Connection myConnection, ArrayList<String> duplicates, boolean hasExceptions) {
        myConnection = myClass.establishOracleConnection();
        //test query to ensure that connection is working properly
        //myClass.query("select * from SYSTEM.ESC_NW_jobevent_1828590", myOracleConnection);

        //adding coordinates to tables - SQL table is hardcoded!!!
        //myClass.addCoordinates(drivers, myOracleConnection);
        //The query to delete entries is hard coded!!!
        //Detect log_dt & driver_id duplicates in GPS_log
        duplicates = myClass.detectDuplicates("SELECT event_dt, driver_id FROM SYSTEM.esc_gpslog_100drivers1month GROUP BY event_dt, driver_id HAVING count(*) > 1", myConnection);

        //CDG_Taxi_distance_ageV2 myClass2 = new CDG_Taxi_distance_ageV2();
        for (int j = 0; j < duplicates.size(); j += 2) {
            System.out.println(j + "/" + duplicates.size() + ">>>>>>>>>>>>>>>>> Deleting dt = timestamp'" + duplicates.get(j) + "'" + " AND driver_id = '" + duplicates.get(j + 1) + "'");
            String myQuery = ("DELETE FROM SYSTEM.esc_gpslog_100drivers1month WHERE event_dt = timestamp'" + duplicates.get(j) + "'" + " AND driver_id = '" + duplicates.get(j + 1) + "'");

            if (myClass.executeSQLUpdate(myQuery, myConnection) == false) {
                hasExceptions = true;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
                }
                slaveDriverForDeletingGPSDups(myClass, myConnection, duplicates, hasExceptions);
            }
            //myClass.executeSQLUpdate(myQuery, myOracleConnection);
            try {
                Thread.sleep(150);
            } catch (InterruptedException ex) {
                Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    //updating the job event log table to note accepts and rejects on a job by job basis.
    //As long as a driver accepts, they will be credited with one offer and one accept
    //need to track linesProcessed, properJobs, prematureRejects, prematureAccepts, noOffers, onlyOffers throughout each recursive call
    public void countJobAccept(String myquery, Connection myc, CDG_Taxi_distance_ageV2 myClass, int linesProcessed, int properJob, int foundPrematureReject, int foundPrematureAccept, int noOffers, int onlyOffers, String tempDriver, String tempJob){
        try {            
            // rs will be scrollable, will not show changes made by others,
            // and will be updatable
            System.out.println("----------Starting outer while loop----------");
            long tStart = System.currentTimeMillis();
            Statement stmt = myc.createStatement(
                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                      ResultSet.CONCUR_UPDATABLE);

            //execute query 
            ResultSet rs = stmt.executeQuery(myquery);
            
            Writer writer = null;
        
            try {
                writer = new BufferedWriter(new FileWriter("c:\\data\\file-output.txt", true));
            } catch (IOException ex) {
                Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            int cursorMasterTemp = 0;
            String previousDriverID = tempDriver;
            String previousJobNo = tempJob;
            String currentDriverID = null;
            String currentJobNo = null;
            String currentEventCode = null;
            processJob currentJob = new processJob();
                       
            try{
            while (rs.next()) {
                cursorMasterTemp++;//increment to keep track of current row cursor
                //System.out.println(cursorMasterTemp);

                //need to check current driver and job to that of the previous row
                currentDriverID = rs.getString("DRIVER_ID");
                currentJobNo = rs.getString("JOB_NO");
                currentEventCode = rs.getString("EVENT_CODE");
                
                //shorten table every 20 million rows to prevent out of memory error
                if(cursorMasterTemp % 20000000 == 0){
                    while (rs.next()) {                        
                        currentDriverID = rs.getString("DRIVER_ID");
                        currentJobNo = rs.getString("JOB_NO");
                        currentEventCode = rs.getString("EVENT_CODE");
                        
                        System.out.println("YOU ARE INSIDE MODULUS AT LINE: " + (linesProcessed+cursorMasterTemp));
                        if ((currentDriverID.equals(previousDriverID)) && (currentJobNo.equals(previousJobNo))) {
                           currentJob.checkEvent(currentEventCode, cursorMasterTemp, rs);
                           cursorMasterTemp++;
                        } else if ((!currentDriverID.equals(previousDriverID)) || (!currentJobNo.equals(previousJobNo))){
                            previousDriverID = currentDriverID;
                            previousJobNo = currentJobNo;    
                            
                           currentJob.checkWholeJob(cursorMasterTemp, rs);
                           System.out.println("YOU HIT THE END OF A JOB!");   
                           
                           currentJob.setFoundOffer(false);
                           currentJob.setFoundAccept(false);
                           currentJob.setFoundReject(false);
                           currentJob.setFoundPrematureAcceptBool(false);
                           currentJob.setFoundPrematureRejectBool(false);
                           currentJob.setCursorOfferIndex(0);                          
                           //resetting booleans and ints for new job and driver
                           break;
                        }     
                    }   
                    System.out.println("HIT THE MODULUS! Job changed at line#: " + (linesProcessed+cursorMasterTemp));
                    
                    try {
                        writer.append("YOU HAVE REACHED LINE #: " + (linesProcessed+cursorMasterTemp));
                        writer.append("\r\n");
                        writer.append("NUM PROPER JOBS: " + currentJob.getProperJob());
                        writer.append("\r\n");
                        writer.append("Number of Jobs where REJECT comes before offer: " + currentJob.getFoundPrematureRejectTemp());
                        writer.append("\r\n");
                        writer.append("Number of Jobs where ACCEPT comes before offer: " + currentJob.getFoundPrematureAcceptTemp());
                        writer.append("\r\n");
                        writer.append("Number of Jobs with no Offers but has accept or reject: " + currentJob.getNoOffersTemp());
                        writer.append("\r\n");
                        writer.append("Number of Jobs with only Offers: " + currentJob.getOnlyOffersTemp());  
                        writer.append("\r\n");
                        writer.close();
                    } catch (IOException ex) {
                        Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
                    }                    
                    
                    rs.close();
                    stmt.close();
                    linesProcessed += cursorMasterTemp;
                    System.out.println("----------RS and STMT closed in recursion!----------");
                    
                    long tEnd = System.currentTimeMillis();
                    long tDelta = tEnd - tStart;
                    double elapsedMinutes = ((tDelta / 1000.0)/60);                   
                    System.out.println("----------20 million lines took: " + elapsedMinutes + " minutes!!!----------");
                    
                    String recursiveQuery = ("select DRIVER_ID, JOB_NO, EVENT_CODE, JOB_OFFER_BOOL, JOB_ACCEPT_BOOL from system.ESC_WM_JOB_EVENT_LOG_ORDERED WHERE ROWNUMBER > " + (linesProcessed-1));
                    myClass.countJobAccept(recursiveQuery, myc, myClass, linesProcessed, currentJob.getProperJob(), currentJob.getFoundPrematureRejectTemp(), currentJob.getFoundPrematureAcceptTemp(), currentJob.getNoOffersTemp(), currentJob.getOnlyOffersTemp(), previousDriverID, previousJobNo);
                    //Preventing Out of Memory Error by shortening table and recursive call
                    break;
                }                    
                
                if ((currentDriverID.equals(previousDriverID)) && (currentJobNo.equals(previousJobNo))) { //when driver and job remain the same
                    
                    //checking current job ---> does the same as commented chunk of code below
                    //can be found in processJob.java Class
                    currentJob.checkEvent(currentEventCode, cursorMasterTemp, rs);
                                       
                } else if ((!currentDriverID.equals(previousDriverID)) || (!currentJobNo.equals(previousJobNo))){ //when driver or job changes                
                    //assign new driver and job based on change
                    previousDriverID = currentDriverID;
                    previousJobNo = currentJobNo;                   
                    
                    currentJob.checkWholeJob(cursorMasterTemp, rs);
                    
                    currentJob.setFoundOffer(false);
                    currentJob.setFoundAccept(false);
                    currentJob.setFoundReject(false);
                    currentJob.setFoundPrematureAcceptBool(false);
                    currentJob.setFoundPrematureRejectBool(false);
                    currentJob.setCursorOfferIndex(0);
                    //resetting booleans and ints for new job and driver
                    
                    //must check job for current job after calculating previous job
                    currentJob.checkEvent(currentEventCode, cursorMasterTemp, rs);
                }  
                
                //print out number of each kind of jobs every 10000 rows
                //This should be a running total throughout even when the method is called again
                if (cursorMasterTemp%10000 == 0){
                    System.out.println("YOU HAVE REACHED LINE #: " + (linesProcessed+cursorMasterTemp));
                    System.out.println("NUM PROPER JOBS: " + currentJob.getProperJob());
                    System.out.println("Number of Jobs where REJECT comes before offer: " + currentJob.getFoundPrematureRejectTemp());
                    System.out.println("Number of Jobs where ACCEPT comes before offer: " + currentJob.getFoundPrematureAcceptTemp());
                    System.out.println("Number of Jobs with no Offers but has accept or reject: " + currentJob.getNoOffersTemp());
                    System.out.println("Number of Jobs with only Offers: " + currentJob.getOnlyOffersTemp());                                      
                }
                
                /* testing cursor
                rs.absolute(2); // moves the cursor to the second row of rs
                String grabString = rs.getString("ADDRESS_REF");
                System.out.println(grabString);                
                rs.updateString("ADDRESS_REF", "JAH QU"); // updates the
                rs.updateRow(); // updates the row in the data sourc
                rs.moveToCurrentRow();
                rs.absolute(cursorMasterTemp);//restore cursor to current row
                */                 
            } //END OF OUTER WHILE LOOP              
            
            //FAIL SAFE...SHOULD NOT HIT THIS ERROR
            //RECURSION WILL OCCUR ANYWAY WITH ERROR THROWN
            }catch(java.lang.OutOfMemoryError ex){
                
                Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);                
                System.out.println("Hit the OUT OF MEM ERROR! Starting from line #: " + cursorMasterTemp);
                while (rs.next()) {
                        cursorMasterTemp++;
                        
                        currentDriverID = rs.getString("DRIVER_ID");
                        currentJobNo = rs.getString("JOB_NO");
                        currentEventCode = rs.getString("EVENT_CODE");
                        
                        System.out.println("YOU ARE INSIDE MEM ERROR OMGGGGGGGGGGG AT LINE: " + cursorMasterTemp);
                        if ((currentDriverID.equals(previousDriverID)) && (currentJobNo.equals(previousJobNo))) {
                           currentJob.checkEvent(currentEventCode, cursorMasterTemp, rs);
                        } else if ((!currentDriverID.equals(previousDriverID)) || (!currentJobNo.equals(previousJobNo))){
                            previousDriverID = currentDriverID;
                            previousJobNo = currentJobNo;
                           currentJob.checkWholeJob(cursorMasterTemp, rs);
                           System.out.println("YOU HIT THE END OF A JOB IN THE MEM ERROR...THIS SHOULD NOT BE POSSIBLE!");                           
                           
                           currentJob.setFoundOffer(false);
                           currentJob.setFoundAccept(false);
                           currentJob.setFoundReject(false);
                           currentJob.setFoundPrematureAcceptBool(false);
                           currentJob.setFoundPrematureRejectBool(false);
                           currentJob.setCursorOfferIndex(0);
                           //resetting booleans and ints for new job and driver
                           
                           break;
                        }       
                    }
                    rs.close();
                    stmt.close();
                    linesProcessed += cursorMasterTemp;
                    String recursiveQuery = ("select DRIVER_ID, JOB_NO, EVENT_CODE, JOB_OFFER_BOOL, JOB_ACCEPT_BOOL from system.ESC_WM_JOB_EVENT_LOG_ORDERED WHERE ROWNUMBER > " + (linesProcessed-1));
                    myClass.countJobAccept(recursiveQuery, myc, myClass, linesProcessed, currentJob.getProperJob(), currentJob.getFoundPrematureRejectTemp(), currentJob.getFoundPrematureAcceptTemp(), currentJob.getNoOffersTemp(), currentJob.getOnlyOffersTemp(), previousDriverID, previousJobNo);
                    //Preventing Out of Memory Error by shortening table and recursive call 
            }
            
            //The last row needs to be processed because there is no recognized job change to compare to
            //If bored...can move in to separate class for function call -- YAYAYAY DONE!
            
            currentJob.checkWholeJob(cursorMasterTemp, rs);
            
            System.out.println("YOU HAVE REACHED LINE #: " + cursorMasterTemp);
            System.out.println("NUM PROPER JOBS: " + currentJob.getProperJob());
            System.out.println("Number of Jobs where REJECT comes before offer: " + currentJob.getFoundPrematureRejectTemp());
            System.out.println("Number of Jobs where ACCEPT comes before offer: " + currentJob.getFoundPrematureAcceptTemp());
            System.out.println("Number of Jobs with no Offers but has accept or reject: " + currentJob.getNoOffersTemp());
            System.out.println("Number of Jobs with only Offers: " + currentJob.getOnlyOffersTemp());
            //Print out the final results now!           
            
            rs.close();
            stmt.close();
            //make sure to close statement and result set!
            
        } catch (SQLException e) {
            System.out.println("Connection Failed...derp! Check output console. " + e.getMessage());
        }
    }
    
    //necessary to mask certain columns based on criteria
    public void maskColumn(Connection myc, CDG_Taxi_distance_ageV2 myClass){
        Scanner scanner = new Scanner(System.in);
        
        ArrayList<String> columnToUpdateData = new ArrayList<>();
               
        System.out.println("Please specify the column to be updated: ");
        String columnToBeUpdated = scanner.nextLine();
        
        String newQuery = ("SELECT " + columnToBeUpdated + " FROM C##NAHWU.ESC_Z_NW_PAX_JOB_FULL2MASK2");            
        
        System.out.println("Your new query is: " + newQuery);
        System.out.println("-------- Populating Array --------");
        
        try {  
            Statement stmt = myc.createStatement(
                                      ResultSet.TYPE_SCROLL_INSENSITIVE,
                                      ResultSet.CONCUR_UPDATABLE);
            //updatable result set for easy updating
            
            //execute query 
            ResultSet rs = stmt.executeQuery(newQuery);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    columnToUpdateData.add(rs.getString(i));
                    //add each current ID to an array for later use
                }
            }
            
            System.out.println("-------- Starting to Mask --------");
            
            for(int j=0; j<columnToUpdateData.size(); j++)
            {
                if(((j+1) % 1000000) == 0)
                {
                    System.out.println("You are starting row: " + (j+1) + "/" + columnToUpdateData.size());
                }
                //print the status every 1000 lines
                
                if(isLong(columnToUpdateData.get(j)) == true)
                //This should take care of extraneous IDs that can't be converted to type long
                {
                    long temp = Long.parseLong(columnToUpdateData.get(j)) + 5476;
                    //converting each string ID into integer for masking purposes
                    //5476 can be a modular mask...can be changed in the future
                    //hard-coded for simplicity in tables 

                    String temp2 = String.valueOf(temp);
                    //convert back to string for continuity

                    rs.absolute(j+1);//move cursor to corresponding row of the column ID
                    rs.updateString(columnToBeUpdated, temp2);//update to the new masked ID
                    rs.updateRow();
                }
            }
            
            rs.close();
            stmt.close();
            //make sure to close statement and result set!
            
        } catch (SQLException e) {
            System.out.println("Something went wrong with the query! Check output console. " + e.getMessage());
        }       
    }
    
    public static boolean isLong(String s) {
    try { 
        Long.parseLong(s); 
    } catch(NumberFormatException e) { 
        return false; 
    } catch(NullPointerException e) {
        return false;
    }
    // only got here if we didn't return false
    return true;
}
    
    //Marking a driver's acceptance boolean value
    //If a driver clearly rejects a job he is marked as "NO" (stored in the database as 0)
    //When a driver accepts a job, it is marked as "YES" (stored in the database as 1)
    public void driverAcceptCount(Connection myc, CDG_Taxi_distance_ageV2 myClass){
        Scanner scanner = new Scanner(System.in);
        
        //Ideally this will be done for each driver, for now...will be done one at a time       
        System.out.println("Please specify the Driver_ID: ");
        String selectedDriver = scanner.nextLine();
        
        String myQuery = ("SELECT * FROM C##NAHWU.Z_DP_" + selectedDriver);
        
        System.out.println(myQuery);
        
        try {
            Statement stmt = myc.createStatement(
                                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                                    ResultSet.CONCUR_UPDATABLE);

            //execute query 
            ResultSet rs = stmt.executeQuery(myQuery);
            
            String currentJobNo = null;
            String currentEventCode = null;
            String currentDriver = null;
            String previousJobNo = null;
            boolean hasRejectOrAccept = false;
            
            try{
                while (rs.next()){
                    currentJobNo = rs.getString("JOB_NO");
                    currentEventCode = rs.getString("EVENT_CODE");
                    currentDriver = rs.getString("DRIVER_ID");

                if (currentJobNo.equals(previousJobNo) && hasRejectOrAccept == false && currentDriver.equals(selectedDriver)){ //to make sure we only give the driver 1 acceptance or rejection per job
                    if (currentEventCode.equals("155")){ //reject
                        String detail = rs.getString("DETAIL");
                        if(detail.endsWith("Driver Reject")){
                            rs.updateInt("ACCEPT_REJECT", 0);//update to reflect that the selected driver outright rejected the job
                            rs.updateRow();
                            hasRejectOrAccept = true;
                        }
                    } else if (currentEventCode.equals("154")){
                        rs.updateInt("ACCEPT_REJECT", 1);//update to reflect that the selected driver accepted the job
                        rs.updateRow();
                        hasRejectOrAccept = true;
                    }
                } else if (!currentJobNo.equals(previousJobNo)){
                    previousJobNo = currentJobNo;
                    hasRejectOrAccept = false;//must reset this boolean for a new job
                }    
    
                }
            } catch (SQLException e){
                System.out.println("Your matching failed deprest...ohnooooo! Check output console. " + e.getMessage());
            }
        rs.close();
        stmt.close();    
        } catch(SQLException e){
            System.out.println("Your query failed...merp! Check output console. " + e.getMessage());
        }
 
    }
    
    public void driverAcceptCountinHANA(Connection myc, CDG_Taxi_distance_ageV2 myClass){
        Scanner scanner = new Scanner(System.in);
        
        //Ideally this will be done for each driver, for now...will be done one at a time       
        System.out.println("Please specify the Driver_ID: ");
        String selectedDriver = scanner.nextLine();
        
        String myQuery = ("SELECT * FROM SYSTEM.Z_DP_" + selectedDriver + " ORDER BY job_no, log_dt");
        
        System.out.println("Your query is: " + myQuery);
        
        try {
            Statement stmt = myc.createStatement(
                                    ResultSet.TYPE_FORWARD_ONLY,
                                    ResultSet.CONCUR_READ_ONLY);

            //execute query 
            ResultSet rs = stmt.executeQuery(myQuery);
            
            String currentJobNo = null;
            String currentEventCode = null;
            String currentDriver = null;
            String previousJobNo = null;
            boolean hasRejectOrAccept = false;
            
            try{
                System.out.println("-------- Starting to analyze table --------");
                while (rs.next()){
                    currentJobNo = rs.getString("JOB_NO");
                    currentEventCode = rs.getString("EVENT_CODE");
                    currentDriver = rs.getString("DRIVER_ID");

                if (currentDriver == null){
                    //needed because a string comparison cannot be done against a null value
                }else if (currentJobNo.equals(previousJobNo) && (hasRejectOrAccept == false) && currentDriver.equals(selectedDriver)){ //to make sure we only give the driver 1 acceptance or rejection per job
                    if (currentEventCode.equals("155")){ //reject
                        String detail = rs.getString("DETAIL");
                        if(detail.endsWith("Driver Reject")){
                            String updateQuery = ("insert into \"SYSTEM\".\"Z_DP_6823511_FINAL_TEST\" values('" + rs.getString("LOG_DT") +  "','" + rs.getString("BOOKING_ID") +  "','" + rs.getString("JOB_NO") +  "','" + rs.getString("DRIVER_ID") +  "','" + rs.getString("EVENT_CODE") +  "','" + rs.getString("DETAIL") +  "','" + rs.getString("ADDRESS_REF") +  "','" + rs.getString("PICKUP_POINT") +  "','" + rs.getString("SETDOWN_ADDRESS") +  "','" + rs.getString("SETDOWN_POINT") + "','0')");
                            myClass.executeSQLUpdate(updateQuery, myc);
                            String commit = "commit";                    
                            myClass.executeSQLUpdate(commit, myc);
                            //moves the entire column to a new table based on the current criteria
                            //a commit statement is required in order to commit chages to the table
                            //auto commit may mess up a rollback changes in the future if the query itself is wrong
                            
                            hasRejectOrAccept = true;
                        }
                    } else if (currentEventCode.equals("154")){//accept
                        String updateQuery = ("insert into \"SYSTEM\".\"Z_DP_6823511_FINAL_TEST\" values('" + rs.getString("LOG_DT") +  "','" + rs.getString("BOOKING_ID") +  "','" + rs.getString("JOB_NO") +  "','" + rs.getString("DRIVER_ID") +  "','" + rs.getString("EVENT_CODE") +  "','" + rs.getString("DETAIL") +  "','" + rs.getString("ADDRESS_REF") +  "','" + rs.getString("PICKUP_POINT") +  "','" + rs.getString("SETDOWN_ADDRESS") +  "','" + rs.getString("SETDOWN_POINT") + "','1')");
                        myClass.executeSQLUpdate(updateQuery, myc);
                        String commit = "commit";                    
                        myClass.executeSQLUpdate(commit, myc);
                        hasRejectOrAccept = true;
                    }
                } else if (!currentJobNo.equals(previousJobNo)){
                    previousJobNo = currentJobNo;
                    hasRejectOrAccept = false;//must reset this boolean for a new job
                }    
    
                }
            } catch (SQLException e){
                System.out.println("Your matching failed deprest...ohnooooo! Check output console. " + e.getMessage());
            }
        rs.close();
        stmt.close();    
        } catch(SQLException e){
            System.out.println("Your query failed...merp! Check output console. " + e.getMessage());
        }
 
    }
    
    public static void main(String[] args) {
        //Initialization
//        CDG_Taxi_distance_ageV2 myClass = new CDG_Taxi_distance_ageV2();
//        Connection myOracleConnection = myClass.establishOracleConnection();
//        Connection myHANAConnection = myClass.establishHANAConnection();
//
//        ArrayList<Integer> drivers = myClass.populateDriverArray();
//        ArrayList<String> duplicates = new ArrayList<>();
//        boolean hasExceptions = false;
 
        //myConnection = myClass.establishOracleConnection();
        //test query to ensure that connection is working properly
        //myClass.runQuery("select count(*) from SYSTEM.esc_booking where booking_dt > 'TO_DATE('01-APR-15', 'DD-MON-RR')'", myOracleConnection);
        
        //Deleting duplicate GPS entries that cause mismatches within the EL data
        //myClass.slaveDriverForDeletingGPSDups(myClass, myOracleConnection, duplicates, hasExceptions);
        
        //matching header info by creating individual driver temp tables and matching with EL to reduce execution time
        //myClass.pullInHeaderInfo(myClass, myOracleConnection, drivers);
        
        //Substracts each GPS Coordinate and calculates distance and age from header info
        //myClass.addCoordinates(drivers, myOracleConnection);
        
        // String myQuery = ("select * from system.esc_job where pax_contact = '94552604'");
        //String fileName = ("c:\\data\\file-output.txt");
        
        //myClass.writeQuery(myQuery, fileName, myOracleConnection);
        
        //myClass.driverAcceptCountinHANA(myHANAConnection, myClass);        
        
        //Counting the number of rate of acceptance by a driver
        //MAX of 1 offer and 1 accept per job
        //1 offer given when driver rejects
        //nothing given when accept or reject comes first
        //nothing given when only offer given
        //should see metrics closer to CDG when after run ---> higher acceptance rate from drivers
        //String testQuery = ("select DRIVER_ID, JOB_NO, EVENT_CODE, JOB_OFFER_BOOL, JOB_ACCEPT_BOOL from system.ESC_JOB_EVENT_LOG_TESTING");
        //String myQuery2 = ("select DRIVER_ID, JOB_NO, EVENT_CODE, JOB_OFFER_BOOL, JOB_ACCEPT_BOOL from system.ESC_WM_JOB_EVENT_LOG_ORDERED WHERE ROWNUMBER >= 695988498");        
        //myClass.maskColumn(myOracleConnection, myClass);      
        
        /*
        //TESTING EXCEPTION!!!!!!!!!!!!
        //LESSON LEARNED: the cursor can only point when the result set has processed that particular line (ie: cannot look into the future!)
        
        Statement stmt;
        try {
            stmt = myOracleConnection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            
            ResultSet rs = stmt.executeQuery(myQuery2);
            
            int cursorMaster = 32000000;
            
            while (rs.next()) {
                cursorMaster++;
                rs.absolute(cursorMaster);
                
                System.out.println("Row #: " + rs.getRow());
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex);
        }catch (java.lang.OutOfMemoryError ex2) {
            Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex2);
            System.out.println("Successfully caugh Out of Memory Error in CATCH!");
        }catch (Error ex3) {
            Logger.getLogger(CDG_Taxi_distance_ageV2.class.getName()).log(Level.SEVERE, null, ex3);
            System.out.println("Successfully caugh Out of Memory Error in ERROR!");
        }*/
        
//        try {            
//            myOracleConnection.close();
//            if (hasExceptions == false) {
//                System.out.println("Your application completed without errors!");
//            } else {
//                System.out.println("try again.....");
//            }
//        } catch (SQLException e) {
//            System.out.println("Could not close connection! Check output console. " + e.getMessage());
//        }
        
        int n = 1;
        
        System.out.printf("%d %d", n++, n++);
    }
}
