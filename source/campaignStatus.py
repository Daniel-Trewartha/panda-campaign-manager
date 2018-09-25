import logging, traceback, os, sys
from models.campaign import Campaign
#You have to draw the line somewhere.
from termcolor import colored as coloured

def statusCampaign(Session,campName):

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
    return campaign.statusReport(Session)
