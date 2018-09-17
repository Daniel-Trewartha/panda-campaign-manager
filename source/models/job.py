import datetime, os, sys, subprocess
from sqlalchemy import Column, Integer, String, Interval, DateTime, JSON, event, ForeignKey
from sqlalchemy.orm import relationship, mapper, joinedload
from sqlalchemy.inspection import inspect
from sqlalchemy.event import listen
from alchemybase import Base

class Job(Base):
    __tablename__ = 'jobs'
    __name__ = 'job'
    
    #Purely local attributes    
    id = Column(Integer, primary_key=True)
    pandaID = Column('pandaID',String,nullable=True,unique=True)
    script = Column('script',String,nullable=False)
    #Could be config, time-slice etc. Whatever we are iterating over to produce a campaign
    iterable = Column('iterable',String,nullable=True)
    campaignID = Column('campaignID',Integer,ForeignKey("campaigns.id"),nullable=False)
    campaign = relationship("Campaign", back_populates="jobs")

    #Attributes created at submission time
    serverName = Column('serverName',String)
    nodes = Column('nodes',Integer,default=1)
    wallTime = Column('wallTime',String,default='00:01:00')
    outputFile = Column('outputFile',String,nullable=True)
    script = Column('script',String, nullable=False)

    #Attributes that will be updated from the panda server

    attemptNr = Column('attemptNr',Integer,nullable=True) #Unsure to what extent attemptNr is functional serverside
    computingSite = Column('computingSite',String,nullable=False,default="NULL")
    creationTime = Column('creationTime',DateTime,nullable=True) #Note that this is the time from the panda server - the existence of a creation time confirms that it exists on the server
    stateChangeTime = Column('stateChangeTime',DateTime,nullable=True)
    status = Column('status',String)
    subStatus = Column('subStatus',String)


    def updateFromJobSpec(self,jobSpec):
        self.status = jobSpec.jobStatus
        self.subStatus = jobSpec.jobSubStatus
        self.attemptNr = jobSpec.attemptNr
        self.computingSite = jobSpec.computingElement
        self.creationTime = jobSpec.creationTime
        self.stateChangeTime = jobSpec.stateChangeTime