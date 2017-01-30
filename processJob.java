package cdg_taxi_distance_age;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class processJob {

    private String previousDriver = null;
    String previousJob = null;
    boolean foundAccept = false;
    int foundPrematureAccept = 0;
    int foundPrematureReject = 0;

    boolean foundOffer = false;
    boolean foundReject = false;
    boolean foundPrematureAcceptBool = false;
    boolean foundPrematureRejectBool = false;
    int cursorOfferIndex = 0;
    int properJob;
    int onlyOffersTemp;
    int noOffersTemp;
    int foundPrematureAcceptTemp;
    int foundPrematureRejectTemp;

    
    public processJob(){}
    
    
    public void checkEvent(String currentEventCode, int cursorMaster, ResultSet rs) {

        if ((foundAccept == true)) {
            //do nothing as long as an accept AND offer has already been found OR premature accept has been found
            //continue;
        } else if (currentEventCode.equals("10") && foundOffer == false) {
            //driver has been offered a job
            //Multiple offers will not bring down the drivers overall acceptance rate since we only count one offer per job
            foundOffer = true;
            cursorOfferIndex = cursorMaster; //mark the row to which and offer boolean will be given if an accept or rejection follows
        } else if ((currentEventCode.equals("154")) && foundAccept == false && foundOffer == true && foundPrematureAcceptBool == false) {
            //driver accepts for the first time after he is offered a job
            //Multiple driver accepts will not bring up the drivers overall acceptance rate since we only count one accept per job
            foundAccept = true;
            try {
                rs.updateInt("JOB_ACCEPT_BOOL", 1);
                rs.updateRow();
            } catch (SQLException ex) {
                Logger.getLogger(processJob.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (currentEventCode.equals("154") && foundAccept == false && foundOffer == false && foundPrematureAcceptBool == false && foundReject == false) {
            //driver accepts before (row wise) he is offered a job
            //We will not count any other event codes in this job
            foundPrematureAcceptBool = true;
        } else if (currentEventCode.equals("155") && foundReject == false && foundOffer == false && foundPrematureRejectBool == false && foundAccept == false) {
            //driver rejects before (row wise) he is offered a job
            //We will not count any other event codes in this job
            foundPrematureRejectBool = true;
        } else if (currentEventCode.equals("155") && foundReject == false) {
            //Rejection will be marked if it is the first rejection by driver
            //Multiple rejections will not affect the driver negatively since we only count one reject per job
            foundReject = true;
        }
    }
    
    public void checkWholeJob(int cursorMaster, ResultSet rs) {
        if ((foundOffer == true) && ((foundAccept == true) || (foundReject == true))) {
            try {
                //Valid job! An offer has been found along with an accept or reject
                rs.absolute(cursorOfferIndex);//move cursor back to the where the first offer occurs
                rs.updateInt("JOB_OFFER_BOOL", 1);//update to reflect the one job offer
                rs.updateRow();
                rs.absolute(cursorMaster);//restore cursor to original postion
                properJob ++;
            } catch (SQLException ex) {
                Logger.getLogger(processJob.class.getName()).log(Level.SEVERE, null, ex);
            }
                    } else if ((foundOffer == true) && (foundAccept == false) && (foundReject == false) && ((foundPrematureAcceptBool == false) && (foundPrematureRejectBool == false))) { 
                        // if an offer has been found but no accept or reject
                        onlyOffersTemp++;   
                    } else if ((foundOffer == false) && ((foundPrematureAcceptBool == true) || (foundPrematureRejectBool == true) || (foundReject == true))) { 
                        //if accepts and rejects have been found with no offers
                        noOffersTemp++;                        
                    } else if (foundPrematureAcceptBool == true) { 
                        //if accepts have been found with out an offer first
                        foundPrematureAcceptTemp ++;                       
                    } else if (foundPrematureRejectBool == true) { 
                        //if rejects have been found with out an offer first
                        foundPrematureRejectTemp++;                       
                    }
    }
    
    
    
    public int getOnlyOffersTemp() {
        return onlyOffersTemp;
    }

    public void setOnlyOffersTemp(int onlyOffersTemp) {
        this.onlyOffersTemp = onlyOffersTemp;
    }

    public int getNoOffersTemp() {
        return noOffersTemp;
    }

    public void setNoOffersTemp(int noOffersTemp) {
        this.noOffersTemp = noOffersTemp;
    }

    public int getFoundPrematureAcceptTemp() {
        return foundPrematureAcceptTemp;
    }

    public void setFoundPrematureAcceptTemp(int foundPrematureAcceptTemp) {
        this.foundPrematureAcceptTemp = foundPrematureAcceptTemp;
    }

    public int getFoundPrematureRejectTemp() {
        return foundPrematureRejectTemp;
    }

    public void setFoundPrematureRejectTemp(int foundPrematureRejectTemp) {
        this.foundPrematureRejectTemp = foundPrematureRejectTemp;
    }

    public int getProperJob() {
        return properJob;
    }

    public void setProperJob(int properJob) {
        this.properJob = properJob;
    }    

    public String getPreviousDriver() {
        return previousDriver;
    }

    public void setPreviousDriver(String previousDriver) {
        this.previousDriver = previousDriver;
    }

    public String getPreviousJob() {
        return previousJob;
    }

    public void setPreviousJob(String previousJob) {
        this.previousJob = previousJob;
    }

    public boolean isFoundAccept() {
        return foundAccept;
    }

    public void setFoundAccept(boolean foundAccept) {
        this.foundAccept = foundAccept;
    }

    public int getFoundPrematureAccept() {
        return foundPrematureAccept;
    }

    public void setFoundPrematureAccept(int foundPrematureAccept) {
        this.foundPrematureAccept = foundPrematureAccept;
    }

    public int getFoundPrematureReject() {
        return foundPrematureReject;
    }

    public void setFoundPrematureReject(int foundPrematureReject) {
        this.foundPrematureReject = foundPrematureReject;
    }

    public boolean isFoundOffer() {
        return foundOffer;
    }

    public void setFoundOffer(boolean foundOffer) {
        this.foundOffer = foundOffer;
    }

    public boolean isFoundReject() {
        return foundReject;
    }

    public void setFoundReject(boolean foundReject) {
        this.foundReject = foundReject;
    }

    public boolean isFoundPrematureAcceptBool() {
        return foundPrematureAcceptBool;
    }

    public void setFoundPrematureAcceptBool(boolean foundPrematureAcceptBool) {
        this.foundPrematureAcceptBool = foundPrematureAcceptBool;
    }

    public boolean isFoundPrematureRejectBool() {
        return foundPrematureRejectBool;
    }

    public void setFoundPrematureRejectBool(boolean foundPrematureRejectBool) {
        this.foundPrematureRejectBool = foundPrematureRejectBool;
    }

    public int getCursorOfferIndex() {
        return cursorOfferIndex;
    }

    public void setCursorOfferIndex(int cursorOfferIndex) {
        this.cursorOfferIndex = cursorOfferIndex;
    }

}
