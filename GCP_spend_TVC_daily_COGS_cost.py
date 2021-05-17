#########################################################################################
###  This script gets the costs per day for TVC COGS from the BigQuery Billing DB     ###
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

def query_bq():
    # the GCP Project that BQ exists in to be called
    client = bigquery.Client(project='shoppertrak-testing')
    # the BQ query to be run, with variable for invoice month
    query = """SELECT cast(export_time as DATE) as Date
           ,sum(cost) as cost
            From `shoppertrak-testing.Billing.gcp_billing_export_v1_01EF05_72AFCF_0A2689`
            Where (project.id = 'tvc-apps-device-mgnt'
                or project.id = 'tvc-apps-stg'
                or project.id = 'tvc-common'
                or project.id = 'tvc-stg'
                or project.id = 'tvc-prod'
                or project.id = 'tvc-prod-core'
                or project.id = 'tvc-prod-apps'
                or project.id = 'tvc-prod-common'
                or project.id = 'tvc-stg2'
                or project.id = 'tvc-apps-stg2'
                or project.id = 'common-bq-edg-prod'
                or project.id = 'tvc-prod-prev'
                or project.id = 'tvc-bq-prod')
                and date_diff(date(CURRENT_DATETIME()), date(export_time), Day) = @diffdate
            Group by Date""" 
    query_params = [bigquery.ScalarQueryParameter('diffdate', 'INT64', diffdate)]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location='US',
        job_config=job_config)  # API request - starts the query

    results = query_job.result()  # Waits for job to complete.
    # for row in results:
    #     d = row.Date
    #     cost = row.cost 
    #     print 'The date is: ', d
    #     print 'The cost is: ', cost
    # results printed from the query
    for row in results:
       q = """ Insert into CostTVCcogsPerDay (uDate, dDate, Cost_per_day)
                Values ('%s', '%s', %.2f) """ % (row.Date, row.Date, row.cost)
       # this will not get written to the log unless the log.conf file levels are changed to take debug
       logger.debug(q)
       cur.execute(q)
       conn.commit()

# checks to see if this script is being run solo or is called by another script.
# if solo (ie, is being run out of the main python directory), then it executes the function 
# if called by another script, that script will call the func and execute it
if __name__ == '__main__':
    query_bq()

