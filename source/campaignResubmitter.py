import os, sys, logging, traceback
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from models.job import Job
from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured
import submissionTools

def resubmitCampaign(Session,campName):


    QUEUE_NAME = 'ANALY_TJLAB_LQCD'
    VO = 'Gluex'
    # read yaml description

    jobdef = None

    try:
        campaign = Session.query(Campaign).filter(Campaign.name.like(campName)).first()
        if (campaign is None):
            print(coloured("No campaign of name "+campName+" found. Currently defined campaigns are: \n"),"red")
            for c in Session.query(Campaign.name).all():
                print(c[0])
            sys.exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)


    aSrvID = None

    print campaign.id
    for j in campaign.jobs.filter(Job.status.like('failed')):
        print(j.id, j.status)
        jobSpec = submissionTools.createJobSpec(walltime=j.wallTime, command=j.script, outputFile=j.outputFile, nodes=j.nodes, campaignID=campaign.id)
        j.servername = jobSpec.jobName
        s,o = Client.submitJobs([jobSpec])
        try:
            j.pandaID = o[0][0]
            j.status = 'submitted'
            j.subStatus = 'submitted'
            print(coloured(j.iterable.strip()+", "+str(o[0][0])+"\n",'green'))
        except Exception as e:
            logging.error(traceback.format_exc())
            print(coloured(j.iterable.strip()+" job failed to submit\n",'red'))
            j.status = 'failed'
            j.subStatus = 'failed'
        Session.commit()
    return None
