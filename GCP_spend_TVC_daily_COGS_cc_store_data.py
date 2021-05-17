#########################################################################################
###  This script gets the store and CC data and creates costs per day for TVC         ###
#########################################################################################


#############################
### Begin Module Imports  ###
#############################
from google.cloud import bigquery                # gets BQ python libraries
import sys                                       # sys module - allows passing of variable at runtime
import pymysql                                   # mysql module
import logging                                   # logging module
import logging.config                            # logging config
import GCP_spend_secrets                         # one stop shop for passwords and common conx variables
### End Module Imports  ###

#############################
### Begin Conx Config     ###
#############################
logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
# Get the logger specified in the file
logger = logging.getLogger(__name__)
# Conx to SQL 
try:
    conn = pymysql.connect(host=GCP_spend_secrets.host, port=GCP_spend_secrets.port, user=GCP_spend_secrets.user, passwd=GCP_spend_secrets.passwd, db=GCP_spend_secrets.db)
    logger.info("Conx to StGCP MySQL DB was successful.") 
except: 
    logger.info("ERROR: Conx to StGCP MySQL DB FAILED.") 
cur = conn.cursor()
### End Conx Config  ###

# NOT caring about inv_month since it seems that costs per day are pretty consistent and just get divvied between inv_months when it is the 1st or 2nd
# this len checks to see if a variable was included at the end of the python statement (2 is one argument / number)
if len(sys.argv) == 2:
    diffdate = sys.argv[1]
else:
    diffdate = 1
logger.info("The diff date is: %s", str(diffdate))

# Update Credit_per_day column from NULL to zero
def query_null_credit_to_zero():
    q = """ Update CostTVCcogsPerDay
            Set Credit_per_day = 0
            Where Datediff(now(), uDate) = %s
                and Credit_per_day is NULL """ % (diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    conn.commit()

# Pull CC data from yesterday to update the following columns in the CostTVCcogsPerDay table:
    # CC_complete | CC_cxld | CC_submitted | CC_started | CC_total
def query_pull_cc_data_into_CostTVCcogsPerDay():
    # Create a dictionary to cpature the values from the sql query against the cycle count / stores table
    d = {}

    q = """ Select Date(status_date) as Date
            ,status
            ,count(*) as count
            from tvc_bu_cycle_count 
            Where datediff(now(), status_date) = %s
            Group by Date(status_date)
            ,status; """ % (diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    # add the query results to the dictionary created above
    for row in cur:
        d[row[1]] = row[2]

    # assign values to the variables for each status possibility
    cxld = d.get('CANCELLED')
    cmpl =  d.get('COMPLETE')
    strt = d.get('STARTED')
    subm =  d.get('SUBMITTED')

    # check to see if any variable is None (ie, not returned by the SQL query), set to zero if so
    if cxld is None:
        cxld = 0

    if cmpl is None:
        cmpl = 0

    if strt is None:
        strt = 0

    if subm is None:
        subm = 0

    # Update the CostTVCcogsPerDay table with the cycle count data
    q = """ Update CostTVCcogsPerDay
            Set CC_complete = %d
            ,CC_cxld = %d
            ,CC_submitted = %d
            ,CC_started = %d
            Where Datediff(now(), uDate) = %s """ % (cmpl, cxld, strt, subm, diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    conn.commit()

# Pull the Store data from yesterday and update the CostTVCcogsPerDay table
def query_pull_store_data_into_CostTVCcogsPerDay():
    # Create a dictionary to cpature the values from the sql query against the cycle count / stores table
    q = """ Select Date(status_date) as Date
            ,count(distinct site_id)
            from tvc_bu_cycle_count 
            Where datediff(now(), status_date) = %s
            group by Date(status_date); """ % (diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    # add the query results to the dictionary created above
    for row in cur:
        strs = row[1]

    # check to see if strs is None.  This should never happen, but if it does, it is set to zero
    if strs is None:
        strs = 0

    # Update the CostTVCcogsPerDay table with the cycle count data
    q = """ Update CostTVCcogsPerDay
            Set Stores_per_day = %d
            Where Datediff(now(), uDate) = %s """ % (strs, diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    conn.commit()

# Update the Totals columns for yesterday
def query_sum_for_totals():
    q = """ Update CostTVCcogsPerDay
            Set Total_per_day = Cost_per_day + Credit_per_day
            ,CC_total = CC_complete + CC_cxld + CC_submitted + CC_started
            Where Datediff(now(), uDate) = %s """ % (diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    conn.commit()
    
# Update Cost_per_store_per_day via sum Total_per_day / Stores_per_day for yesterday
def query_sum_for_cost_per_day():
    q = """ Update CostTVCcogsPerDay
            Set Cost_per_CC_per_day = Round(Total_per_day/CC_total, 2)
            ,Cost_per_store_per_day = Round(Total_per_day/Stores_per_day, 2)
            Where Datediff(now(), uDate) =  %s """ % (diffdate)
    # this will not get written to the log unless the log.conf file levels are changed to take debug
    logger.debug(q)
    cur.execute(q)
    conn.commit()
 

# checks to see if this script is being run solo or is called by another script.
# if solo (ie, is being run out of the main python directory), then it executes the function 
# if called by another script, that script will call the func and execute it
if __name__ == '__main__':
    query_null_credit_to_zero()
    query_pull_cc_data_into_CostTVCcogsPerDay()
    query_pull_store_data_into_CostTVCcogsPerDay()
    query_sum_for_totals()
    query_sum_for_cost_per_day()

