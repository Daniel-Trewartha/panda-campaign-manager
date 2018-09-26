import os, sys, logging, traceback
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from models.job import Job
from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured
import submissionTools

def resubmitCampaign(Session,campName,resubmit_cancelled):


    QUEUE_NAME = 'ANALY_TJLAB_LQCD'
    VO = 'Gluex'
    # read yaml description

    jobdef = None

    try:
        campaign = Session.query(Campaign).filter(Campaign.name.like(campName)).first()
        if (campaign is None):
            print(coloured("No campaign of name "+campName+" found. Currently defined campaigns are: \n","red"))
            for c in Session.query(Campaign.name).all():
                print(c[0])
            sys.exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)


    aSrvID = None

    submitStatus = ['failed']
    submitStatus.append('cancelled') if resubmit_cancelled else submitStatus
    for j in campaign.jobs.filter(Job.status.in_(submitStatus)).all():
        jobSpec = submissionTools.createJobSpec(walltime=j.wallTime, command=j.script, outputFile=j.outputFile, nodes=j.nodes, jobName=j.serverName)
        j.servername = jobSpec.jobName
        s,o = Client.submitJobs([jobSpec])
        try:
            j.pandaID = o[0][0]
            j.status = 'submitted'
            j.subStatus = 'submitted'
            if (j.iterable):
                print(coloured(j.iterable.strip()+", "+str(o[0][0])+"\n",'green'))
            else:
                print(coloured(j.serverName.strip()+", "+str(o[0][0])+"\n",'green'))
        except Exception as e:
            logging.error(traceback.format_exc())
            if (j.iterable):
                print(coloured(j.iterable.strip()+" job failed to submit\n",'red'))
            elif (j.serverName):
                print(coloured(j.serverName.strip()+" job failed to submit\n",'red'))
            else:
                print(coloured(str(j.id)+" job failed to submit\n",'red'))
            j.status = 'failed'
            j.subStatus = 'failed'
        Session.commit()
    return None
