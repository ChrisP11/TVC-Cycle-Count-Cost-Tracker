########################################################
###  This module calls a series of other modules     ###
###  To generate daily tracking numbers for TVC COGS ###
########################################################

#############################
### Begin Module Imports  ###
#############################
import GCP_spend_TVC_daily_COGS_cost             # direct conx to BQ, dumps costs into MySQL
import GCP_spend_TVC_daily_COGS_credit           # direct conx to BQ, dumps credits into MySQL
import GCP_spend_TVC_daily_COGS_cc_store_data    # A series of functions to run MySQL queries against the CostTVCcogsPerDay table
import sys                                       # sys module - allows passing of variable at runtime
import logging                                   # logging module
import logging.config                            # logging config
logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
# Get the logger specified in the file
logger = logging.getLogger(__name__)

# allows this code to take an external variable to represent diff date and get recovery data from BQ Billing
# this len checks to see if a variable was included at the end of the python statement (2 is one argument / number)
if len(sys.argv) == 2:
    diffdate = sys.argv[1]
else:
    diffdate = 1

logger.info("The diff date is: %s", str(diffdate))

# query BQ for costs per day
GCP_spend_TVC_daily_COGS_cost.query_bq()
logger.info("GCP TVC COGS costs have been acquired and inserted into the LOGcost table.")

# query BQ for credits per day
GCP_spend_TVC_daily_COGS_credit.query_bq()
logger.info("GCP TVC COGS credits have been acquired and inserted into the LOGcost table.")

# Update Credit_per_day column from NULL to zero
GCP_spend_TVC_daily_COGS_cc_store_data.query_null_credit_to_zero()

# Pull CC data from yesterday to update the CostTVCcogsPerDay table
GCP_spend_TVC_daily_COGS_cc_store_data.query_pull_cc_data_into_CostTVCcogsPerDay()

# Pull the Store data from yesterday and update the CostTVCcogsPerDay table
GCP_spend_TVC_daily_COGS_cc_store_data.query_pull_store_data_into_CostTVCcogsPerDay()

# Update the Totals columns for yesterday
GCP_spend_TVC_daily_COGS_cc_store_data.query_sum_for_totals()

# Update Cost_per_store_per_day via sum Total_per_day / Stores_per_day for yesterday
GCP_spend_TVC_daily_COGS_cc_store_data.query_sum_for_cost_per_day()

