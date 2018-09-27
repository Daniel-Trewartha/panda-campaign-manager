import logging, traceback, os, sys
from models.campaign import Campaign
from models.job import Job
#You have to draw the line somewhere.
from termcolor import colored as coloured

def statusReport(Session,campName,short=False):
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

    campaign.updateJobs(Session)
    if(short):
        return campaign.shortReport(Session)
    else:
        return campaign.statusReport(Session)

def checkIterables(Session):
    #Check for iterables that are repeatedly failing across campaigns/jobs - indication that input files are corrupted or missing
    retStr = ""
    for iterable in Session.query(Job).with_entities(Job.iterable).group_by(Job.iterable).all():
        failCount = Session.query(Job).filter(Job.iterable.isnot(None)).filter(Job.iterable == iterable[0]).filter(Job.status.like('failed')).count()
        if failCount > 1:
            retStr += coloured(str(failCount)+" failures with iterable ",'red')+coloured(iterable[0],'yellow')+coloured(". Possibly missing or corrupt input?\n","red")
    return retStr

def statusCampaign(Session,campName=None):

    if campName is not None:
        return statusReport(Session,campName)
    else:
        retStr = ""
        for c in Session.query(Campaign.name).all():
            retStr += statusReport(Session,c[0],short=True)
        retStr += '\n'
        retStr += checkIterables(Session)
        return retStr