# 20210225 I believe I need to start tagging projects as an opt-in for this script
# IE, a tag called 'AddSname' or something like that with a value of 'True'
# currently I have a somewhat complicated filter on my gcloud project list command
# to avoid gsuite based projects plus a project that has api issues
# but as more and more projects get added, the more often this will break
# and the reality is I do not need to attach the sname tag to every project GCE
# just to the really big ones

##################################################################################################
#####  Reviews all the projects in Gcloud to verify each GCE instance has the sname label.   #####
#####  This allows for granular tracking of costs in the GCP Billing BigQuery DB             #####
##################################################################################################

#############################
### Begin Module Imports  ###
#############################

import subprocess                                # allows bash commands
import json                                      # json module, allows reading of json data
import time                                      # allows tracking of run time
import logging                                   # logging module
import logging.config                            # logging config
### End Module Imports  ###

#############################
### Begin Conx Config     ###
#############################
logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
# Get the logger specified in the file
logger = logging.getLogger(__name__)

### End Conx Config  ###

def GCE_add_sname_label():
    # this snippet begins tracking how long the script takes to run
    start_time = time.time()

    # get project list
    o1 = subprocess.Popen('gcloud projects list --format="json(projectId, name)" --filter="-projectId:sys-* AND -projectId:sensormatic-monitoring"', shell=True, stdout=subprocess.PIPE)
    # forces script to wait until bash script is complete
    o1.wait()
    # takes the output of the glcoud command and puts it in a json string format (I think)
    jsonS,_ = o1.communicate()
    # loads into a format that allows json type commands to work on it.  totally don't understand this.  
    # Got it from: https://stackoverflow.com/questions/42161711/json-parsing-python-subprocess
    projectsJSON = json.loads(jsonS)
    # variable used to track how many GCE instances were missing an sname label before this was run
    mlabels = 0
    # this variable is used to scroll through the data returned from the cursor
    c=0
    for row in projectsJSON:
    # while c < 1:
        # this is where the 'c' variable is used
        projectName = projectsJSON[c]["name"]
        projectId = projectsJSON[c]["projectId"]
        # projectName = 'tvc-common'
        # projectId = 'tvc-common'
        cmd = ("gcloud config set project %s") % projectId
        # runs the command, switches to the new project
        o2 = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        # waits for the subprocess above to finish, otherwise the next subprocess gets data from the wrong (previous) set of data
        o2.wait()
        c+=1
        # gets the list of GCE instances in the project without an sname label
        cmd3 = 'gcloud compute instances list --filter=-"labels.sname:*" --format="json(NAME, zone)"'
        o3 = subprocess.Popen(cmd3, shell=True, stdout=subprocess.PIPE)
        o3.wait()
        # takes the output of the glcoud command and puts it in a json string format (I think)
        jsonS,_ = o3.communicate()
        # loads into a format that allows json type commands to work on it.  totally don't understand this.  
        # Got it from: https://stackoverflow.com/questions/42161711/json-parsing-python-subprocess
        data = json.loads(jsonS)
        mlabels+=len(data)
        # x is used to iterate through the json in the data list (or is it a tuple?  dmn, I never know)
        x=0
        for row in data:
            zone = data[x]["zone"]
            name = data[x]["name"]
            cmd4 = 'gcloud compute instances add-labels %s --labels=sname=%s --zone=%s' % (name, name, zone)
            o4 = subprocess.Popen(cmd4, shell=True, stdout=subprocess.PIPE)
            o4.wait()
            logger.info("%s server had an sname tag added", name) 
            x+=1

    logger.info("%s servers had an sname tag added", mlabels) 

    # outputs how long the script took to run
    runtime =  time.time() - start_time
    logger.info("Runtime for the GCE JSON was %s.", runtime) 

if __name__ == '__main__':
    GCE_add_sname_label()