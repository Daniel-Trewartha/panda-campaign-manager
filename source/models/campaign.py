import datetime, os, sys, subprocess, logging, traceback
from sqlalchemy import Column, Integer, String, Interval, DateTime, JSON, event, ForeignKey, UniqueConstraint, asc, desc, not_
from sqlalchemy.orm import relationship, mapper, joinedload
from sqlalchemy.inspection import inspect
from sqlalchemy.event import listen
from alchemybase import Base
import Client
from job import Job
#You have to draw the line somewhere.
from termcolor import colored as coloured

class Campaign(Base):
    __tablename__ = 'campaigns'
    __name__ = 'campaign'
        
    id = Column(Integer, primary_key=True)
    jobs = relationship("Job", back_populates="campaign",cascade="all, delete-orphan",lazy="dynamic")
    name = Column('name',String,nullable=False,unique=True)
    lastUpdate = Column('lastUpdate',DateTime)

    def terminalStates(self):
    #Those statuses which are terminal and will not change in the future
    #This is a function because SQLalchemy base implements __init__ and we don't want to play silly buggers with that
        return ["finished","failed","cancelled"]

    def statusReport(self,Session,options=None):
        printStr = "\n Status Report for Campaign: "+coloured(self.name,"yellow")+"\n"
        printStr += "Last updated: "+str(self.lastUpdate)+"\n"
        printStr += str(self.jobs.count())+" total jobs\n\n"
        for site in self.jobs.with_entities(Job.computingSite,Job.status).filter(not_(Job.status.like("cancelled"))).group_by(Job.computingSite).all():
            if site[0] and (site[0] not in ["NULL",""]):
                cS = site[0]
            else:
                cS = "Not yet allocated to a site"
            siteCount = str(self.jobs.filter(Job.computingSite.like(site[0])).filter(not_(Job.status.like("cancelled"))).count())
            printStr += cS + ": " + siteCount + "\n"
        cancelled = self.jobs.filter(Job.status.like('cancelled')).count()
        if cancelled > 0:
            printStr += "Cancelled: "+str(cancelled)+"\n"
        printStr += "\n"

        for status in self.jobs.with_entities(Job.status,Job.subStatus).group_by(Job.status,Job.subStatus).all():
            if (status[0] in ["finished","staged"]):
                colour = 'green'
            elif (status[0] in ["failed","cancelled"]):
                colour = 'red'
            else:
                colour = 'blue'
            jobsThisStatus = self.jobs.filter(Job.status.like(status[0]),Job.subStatus.like(status[1]))
            statCount = str(jobsThisStatus.count())
            mostRecent = str(jobsThisStatus.order_by(desc('stateChangeTime')).first().stateChangeTime)
            oldest = str(jobsThisStatus.order_by(asc('stateChangeTime')).first().stateChangeTime)
            printStr += coloured("Status: %s \nSubStatus: %s \nCount: %s\nMost Recent: %s\nOldest: %s\n \n" % (status[0],status[1],statCount,mostRecent,oldest),colour)
        return printStr

    def updateJobs(self,Session,recreate=False,jobs_to_query=None):
        if (not jobs_to_query):
            jobs_to_query = []
            for j in self.jobs.all():
                if j.status not in self.terminalStates():
                    jobs_to_query.append(j.pandaID)
        o = Client.getJobStatus(jobs_to_query)
        updated = len(jobs_to_query)
        if (o):
            for j in o[1]:
                if (j):
                    try:
                        dbJob = Session.query(Job).filter(Job.pandaID == j.PandaID).one()
                        dbJob.updateFromJobSpec(j,recreate)
                        Session.commit()
                    except Exception as e:
                        updated -= 1
                        logging.error(traceback.format_exc())
                        Session.rollback()
            self.lastUpdate = datetime.datetime.utcnow()
            Session.commit()
        retStr = str(self.jobs.count() - len(jobs_to_query))+" jobs finished or failed\n"
        retStr += "Updated "+str(updated)+" in progress jobs\n"
        if (updated < len(jobs_to_query)):
            retStr += "Warning: "+str(len(jobs_to_query) - updated)+" jobs failed to update\n" 
        return retStr

    def deleteJobs(self,Session):
        #Kill ALL jobs associated with this campaign
        jobs_to_delete = []
        for j in self.jobs.all():
            if j.status not in self.terminalStates():
                jobs_to_delete.append(j.pandaID)
        o = Client.killJobs(jobs_to_delete)
        deleted = 0
        for j,isDead in zip(jobs_to_delete,o[1]):
            #We should do something more fancy in terms of logging which jobs failed to delete I think
            deleted += 1 if isDead else deleted
        retStr = "Deleted "+str(deleted)+" in progress jobs\n"
        retStr += str(self.jobs.count() - len(jobs_to_delete))+" jobs already finished or failed \n"
        retStr += str(len(jobs_to_delete) - deleted)+" jobs failed to delete"
        #Also trigger an update
        self.updateJobs(Session)
        return retStr