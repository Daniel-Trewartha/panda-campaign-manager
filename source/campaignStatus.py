import logging, traceback, os, sys
from models.campaign import Campaign
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

def statusCampaign(Session,campName=None):

    if campName is not None:
        return statusReport(Session,campName)
    else:
        retStr = ""
        for c in Session.query(Campaign.name).all():
            retStr += statusReport(Session,c[0],short=True)
        return retStr