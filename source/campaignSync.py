#!/usr/bin/env python


import os, sys, time, json, subprocess, random, re, logging, traceback, yaml, datetime

from models.campaign import Campaign
from models.job import Job
import Client
#You have to draw the line somewhere.
from termcolor import colored as coloured

#Unpack a server name into a campaign name, optionally iterable
def unpackServerName(name):
    campFormat = 'c:[^:]*:'
    iterableFormat = 'i:[^:]*:'
    oFFormat = 'oF:[^:]*:'
    campP = re.compile(campFormat)
    iterableP = re.compile(iterableFormat)
    oFP = re.compile(oFFormat)
    try:
        cN = campP.search(name).group()[2:-1]
        #No colons in campaign names
        cN = re.sub(':','',cN)
    except:
        cN = None
    try:
        i = iterableP.search(name).group()[2:-1]
    except:
        i = None
    try:
        oF = iterableoF.search(name).group()[3:-1]
    except:
        oF = None
    return (cN,i,oF)

def syncCampaign(Session):

    try:
        output = Client.getAllJobs()
        if output[0] != 0:
            raise Exception("Server error")
        else:
            output = json.loads(output[1])['jobs']
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)

    jobsToRepopulate = []
    for j in output:
        try:
            #Check for pre-existing job with this pandaid
            #We have to evaluate these queries lazily to avoid throwing an unnecessary exception
            if (j['pandaid'] and j['jobname']):
                isExistingPandaID = Session.query(Job).filter(Job.pandaID.like(j['pandaid']))
                isExistingJobName = Session.query(Job).filter(Job.serverName.like(j['jobname']))
                if ( isExistingPandaID.first() is None and isExistingJobName.first() is None):
                    if(len(j['jobname'])>37):
                        #See if the jobname fits the format
                        campaignName, i, oF = unpackServerName(j['jobname'])
                        if(campaignName):
                            campaign = Session.query(Campaign).filter(Campaign.name.like(campaignName)).first()
                            if (campaign is None):
                                campaign = Campaign(name=campaignName,lastUpdate=datetime.datetime.utcnow())
                                Session.add(campaign)
                                Session.commit()
                            #We can't recover the job script from the monitor output - we do that with another query below
                            job = Job(script="unknown",campaignID=campaign.id,pandaID=j['pandaid'],serverName=j['jobname'],status=j['jobstatus'],subStatus=j['jobsubstatus'])
                            if i:
                                job.iterable = i
                            #In some instances panda server can report a null substatus. Converting these to empty strings to fulfil database rules
                            if not j['jobsubstatus']:
                                job.subStatus = ""
                            Session.add(job)
                            Session.commit()

                            #Record that this campaign/job id pair was missing, but only after it's been committed
                            jobsToRepopulate.append((campaign.id,job.pandaID))
        except Exception as e:
            logging.error(traceback.format_exc())
            Session.rollback()

    #We need to query each job individually to get its job parameters
    campsToRepopulate = set([seq[0] for seq in jobsToRepopulate])
    for c in campsToRepopulate:
        try:
            camp = Session.query(Campaign).get(c)
            jobs = [seq[1] for seq in jobsToRepopulate if seq[0] == c]
            #Recreate the jobs that were missing
            camp.updateJobs(Session,recreate=True,jobs_to_query=jobs)
            #Now update them all to make sure everything is legit
            camp.updateJobs(Session)
            #Now check to see if we have duplicate output files
            for OF in Session.query(Job).with_entities(Job.outputFile).group_by(Job.outputFile).all():
                jobsThisOF = Session.query(Job).filter(Job.outputFile.like(OF[0])).count()
                if (jobsThisOF > 1):
                    print(coloured('Warning:'+str(jobsThisOF)+' job(s) have shared output file: \n'+OF[0]+'\n','red'))
        except Exception as e:
            logging.error(traceback.format_exc())
            Session.rollback()
    return None
