"""
Created on June 07, 2018

@author: Rajnikant Rakesh
This function returns the load date to be used for a daily load job
It also calcuates the table partition decorator for the Bigquery table
tzoffset is of the form +07:00
reportdate is of form 201806
Returns a dictionary {'tablepartitiondecorator':'20170422', 'reportdate':'2017-04-22'}
"""

from datetime import datetime
from datetime import timedelta
from datetime import date

def setdailyloadparams(tzoffset, reportdate=None):

    if reportdate == None:
        localtime = datetime.utcnow()+ timedelta( 
                                                 hours= int(tzoffset.split(":")[0]), 
                                                minutes= int(tzoffset.split(":")[1]))
        localtime = localtime - timedelta(days=1)
        tblpartitiondeco = str(datetime.date(localtime)).replace("-", "")
        greportdate = str(datetime.date(localtime))
    else:
        tblpartitiondeco = reportdate
        greportdate = str(datetime.strptime(reportdate, "%Y-%m-%d %H:%M:%S", ts).date())
    return {'tablepartitiondecorator':tblpartitiondeco, 
            'reportdate':greportdate}