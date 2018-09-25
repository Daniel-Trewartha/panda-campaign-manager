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
            campaign = Campaign(name=campdef['campaign'],lastUpdate=datetime.datetime.utcnow())
            Session.add(campaign)
            Session.commit()
    except Exception as e:
        logging.error(traceback.format_exc())
        Session.rollback()
        sys.exit(1)


    aSrvID = None

    nodes = campdef['jobtemplate']['nodes']
    walltime = campdef['jobtemplate']['walltime']
    queuename = campdef['jobtemplate']['queuename']
    try:
        outputFile = campdef['jobtemplate']['outputFile'].strip()
    except:
        outputFile = None
    command = campdef['jobtemplate']['command']

    if (listFile):
        iterList = []
        with open(listFile,'r') as f:
            for i in f:
                iterList.append(i.strip())
    else:
        iterList = ['']

    for iterable in iterList:
        if (listFile):
            jobCommand = re.sub('<iter>',iterable,command)
            jobOutput = re.sub('<iter>',iterable,outputFile)
        else:
            jobCommand = command
            jobOutput = outputFile

        #Check to see if this is a duplicate output file
        jobsThisOF = Session.query(Job).filter(Job.outputFile.like(jobOutput)).count() 
        if (jobsThisOF > 0):
            print(coloured('Warning:'+str(jobsThisOF)+' job(s) already exist with output file: \n'+jobOutput+'\n','red'))

        dbJob = Job(script=jobCommand,nodes=nodes,wallTime=walltime,status="To Submit",campaignID=campaign.id,outputFile=jobOutput)
        dbJob.servername = campaign.name+subprocess.check_output('uuidgen')
        if (listFile):
            dbJob.iterable = iterable

        Session.add(dbJob)
        Session.commit()

        jobSpec = submissionTools.createJobSpec(walltime=walltime, command=jobCommand, outputFile=jobOutput, nodes=nodes, jobName=dbJob.servername)
        s,o = Client.submitJobs([jobSpec])
        try:
            dbJob.pandaID = o[0][0]
            dbJob.status = 'submitted'
            dbJob.subStatus = 'submitted'
            print(coloured(iterable.strip()+", "+str(o[0][0])+"\n",'green'))
        except Exception as e:
            logging.error(traceback.format_exc())
            print(coloured(iterable.strip()+" job failed to submit\n",'red'))
            dbJob.status = 'failed'
            dbJob.subStatus = 'failed'
        Session.commit()
    
    return None
