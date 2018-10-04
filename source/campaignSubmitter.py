import os, sys, re, logging, traceback, datetime,subprocess, json
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from models.job import Job
from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured
import submissionTools

def submitCampaign(Session,campSpecFile,listFile):


    # read yaml description

    jobdef = None

    try:
        campdef = submissionTools.PandaJobsJSONParser.parse(campSpecFile)
        campaign = Session.query(Campaign).filter(Campaign.name.like(campdef['campaign'])).first()
        if (campaign is None):
            #Don't let colons into campaign names
            campName = re.sub(':','',campdef['campaign'])
            campaign = Campaign(name=campName,lastUpdate=datetime.datetime.utcnow())
            Session.add(campaign)
            Session.commit()
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)


    aSrvID = None

    for j in campdef['jobs']:
        nodes = j['nodes']
        walltime = j['walltime']
        queuename = j['queuename']
        try:
            outputFile = j['outputFile'].strip()
        except:
            outputFile = None
        command = j['command']

        try:
            iterable = j['iterable'].strip()
        except:
            iterable = None

        #Check to see if this is a duplicate output file
        jobsThisOF = Session.query(Job).filter(Job.outputFile.like(outputFile)).count() 
        if (jobsThisOF > 0):
            print(coloured('Warning:'+str(jobsThisOF)+' job(s) already exist with output file: \n'+outputFile+'\n','red'))

        dbJob = Job(script=command,nodes=nodes,wallTime=walltime,status="To Submit",subStatus="To Submit",campaignID=campaign.id,outputFile=outputFile)
        dbJob.serverName = 'c:'+campaign.name+':'
        if iterable:
            dbJob.serverName += 'i:'+iterable+':'
        if outputFile:
            #Panda Server doesn't like slashes in its job names
            dbJob.serverName += 'oF:'+re.sub('/',';',outputFile)+':'
        dbJob.serverName += subprocess.check_output('uuidgen')
        
        dbJob.iterable = iterable

        jobSpec = submissionTools.createJobSpec(walltime=walltime, command=command, outputFile=outputFile, nodes=nodes, jobName=dbJob.serverName)
        s,o = Client.submitJobs([jobSpec])
        try:
            print(o)
            dbJob.pandaID = o[0][0]
            dbJob.status = 'submitted'
            dbJob.subStatus = 'submitted'
            print(coloured(iterable.strip()+", "+str(o[0][0])+"\n",'green'))
        except Exception as e:
            logging.error(traceback.format_exc())
            print(coloured(iterable.strip()+" job failed to submit\n",'red'))
            dbJob.status = 'failed'
            dbJob.subStatus = 'failed'
        Session.add(dbJob)
        Session.commit()
    
    return None
