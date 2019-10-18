# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 11:26:49 2019

Script to convert a selected event from an Antelope database to Hypoinverse

@author: Rachel
"""
import pandas as pd

def getData(eventid, dbid):
    eventdb = []
    
    #set paths to relevant tables
    pathevtbl = dbid + '/' + dbid + '.event'
    pathortbl = dbid + '/' + dbid + '.origin'
    pathassoctbl = dbid + '/' + dbid + '.assoc'
    patharrivtbl = dbid + '/' + dbid + '.arrival'    
        
    #load antelope tables to dataframes and pull event related rows
    evtbl = pd.read_csv(pathevtbl,header=None,delim_whitespace=True,
                        names=['evid','evname','prefor','auth','commid',
                        'lddate'])
    ortbl = pd.read_csv(pathortbl,header=None,delim_whitespace=True,
                        names=['lat','lon','depth','time','orid','evid',
                               'jdate','nass','ndef','ndp','grn','srn','etype',
                               'review','depdp','dtype','mb','mbid','ms',
                               'msid','ml',
                               'mlid','algorithm','auth','commid','lddate'])
    assoctbl = pd.read_csv(pathassoctbl,header=None,delim_whitespace=True,
                           names=['arid','orid','sta','phase','belief',
                                  'delta','seaz','esaz','timeres','timedef',
                                  'azres','azdef','slores','slodef','emares',
                                  'wgt','vmodel','commid','lddate'])                            
    arrivtbl = pd.read_csv(patharrivtbl,header=None,delim_whitespace=True,
                           names=['sta','time','arid','jdate','stassid',
                                  'chanid','chan','iphase','stype','detim',
                                  'azimuth','delaz','slow','delslo','ema',
                                  'rect','amp','per','logat','clip','fm',
                                  'snr','qual','auth','commid','lddate'])
                           
    eventinfo = evtbl[evtbl.evid==eventid]  
    
    #join data from other tables associated with event
    eventdb = pd.merge(eventinfo, ortbl, on='evid', how='left',
                       suffixes=['_ev','_or']) 
    eventdb = eventdb.merge(assoctbl, on='orid', how='left',suffixes=['','_assoc'])
    eventdb = eventdb.merge(arrivtbl, on='arid', how='left',suffixes=['','_arriv'])
        
    return eventdb
    
def write2Hypoinverse(eventdb, ffname):
    wsp = ' ' #initialize all variables as appropriately lengthed white space    
    
    #Initialize summary header variables as right length of spaces
    yr = wsp*4 #year (I4)
    mdhm = wsp*8 #month, day, hour, minute (4I2)
    ortime = wsp*4 #origin time in s (F4.2)
    latdeg = wsp*2 #lat in degrees (F2.0)
    ns = wsp #S for south, blank otherwise (A1)
    latmin = wsp*4 #lat in min (F4.2)
    londeg = wsp*3 #long in degrees (F3.0)
    ew = wsp #E for east, otherwise blank (A1)
    lonmin = wsp*4 #long in min (F4.2)
    dep = wsp*5 #depth in km (F5.2)
    mag = wsp*3 #magnitude from max S amplitude from NCSN stations (F3.2)
    numt = wsp*3 #number p and s times (I3)
    mazgap = wsp*3 #max azimuthal gap in degrees (I3)
    stadist = wsp*3 #distance to nearest station in degrees (F3.0)
    rmstt = wsp*4 #rms travel time residual (F4.2)
    aziprin1 = wsp*3 #azimuth of largest principal error (F3.0)
    dipprin1 = wsp*2 #dip of largest principal error (F2.0)
    sizprin1 = wsp*4 #size of largest principal error (F4.2)
    aziprin2 = wsp*3 #azimuth of intermediate principal error (F3.0)
    dipprin2 = wsp*2 #dip of intermediate principal error (F2.0)
    sizprin2 = wsp*4 #size of intermediate principal error (F4.2)
    codamag = wsp*3 #coda duration magnitude from NCSN stations (F3.2)
    evrmk = wsp*3 #event location remark (A3)
    sizprin3 = wsp*4 #size of smallest principal error (F4.2)
    auxrmk = wsp*2 #auxiliary remark (2A1)
    numst = wsp*3 #num s times (I3)
    horzerr = wsp*4 #horizontal error (F4.2)
    verterr = wsp*4 #vertical error  (F4.2)
    numpfm = wsp*3 #number of p first motions (I3)
    smagweights = wsp*4 #sum of s amplitude mag weights ~num readings (F4.1)
    durmagweights = wsp*4 #sum duration mag weights ~num readings (F4.1)
    madsmag = wsp*3 #median absolute difference s-amp mag (F3.2)
    maddurmag = wsp*3 #median absolute difference duration mag (F3.2)
    emod = wsp*3 #3-letter code for crust/delay model (A3)
    eqauth = wsp*1 #last authority for earthquake (A1)
    scode = wsp*1 #most common P and S source code (A1)
    durcode = wsp*1 #most common duration source code (A1)
    ampcode = wsp*1 #most common amplitude source code (A1)
    cdm = wsp*1 #coda duration magnitude type code (A1)
    validpicks = wsp*3 #number valid p and s picks (I3)
    smagcode = wsp*1 #s amp magnitude type code (A1)
    extmagcode = wsp*1 #external amplitude mag type code (A1)
    extmag = wsp*3 #external magnitude (F3.2)
    extmagweights = wsp*3 #sum of external magnitude weights (F3.1)
    altamptype = wsp*1 #alternate amplitude mag label code (A1)
    altamp = wsp*3 #alternate amplitude magnitude (F3.2)    
    altampweights = wsp*3 #sum of alternative magnitude weights (F3.2)
    evid = wsp*10 #event ID (I10)
    prefmaglab = wsp*1 #preferred mag label (A1)
    prefmag = wsp*3 #preferred mag chosen by PRE command (F3.2)
    prefmagweights = wsp*4 #sum of preferred mag weights (F4.1)
    altdurmagtype = wsp*1 #alt code duration mag type code (A1)
    altdurmag = wsp*3 #alt code duration magnitude (F3.2)
    altdurmagweights = wsp*4 #sum of alt duration magnitude weights (F4.1)
    versnum = wsp*1 #QDDS version number (A1)
    oriversnum = wsp*1 #origin instance version num (A1)
    
    #set necessary variables to input database values
    yr = str(2010)
    ew = 'E'
    oriversnum = 'L'
    evid = '0123456789'
    emod = 'EAR'
    
    #concat variables to single line
    headln = yr+mdhm+ortime+latdeg+ns+latmin+londeg+ew+lonmin+dep+mag+numt+ \
    mazgap+stadist+rmstt+aziprin1+dipprin1+sizprin1+aziprin2+dipprin2+ \
    sizprin2+codamag+evrmk+sizprin3+auxrmk+numst+horzerr+verterr+numpfm+ \
    smagweights+durmagweights+madsmag+maddurmag+emod+eqauth+scode+durcode+ \
    ampcode+cdm+validpicks+smagcode+extmagcode+extmag+extmagweights+ \
    altamptype+altamp+altampweights+evid+prefmaglab+prefmag+prefmagweights+ \
    altdurmagtype+altdurmag+altdurmagweights+versnum+oriversnum+'\n'
    
    #write to event file
    with open(ffname, 'a') as the_file:
        the_file.write(headln)
    
    #Initialize pick data lines
    
    #loop through picks
    
    #set necessary variables to input database values
    
    #concat to single line
    
    #write to event file
    
    return
    
    
    
#Database name - put tables in folder that matches the directory name
dbname = 'GADBPart1'

#Database event id - integer corresponding to event to prep for hypoinverse 
eventid = 69

#Arc file name for writing hypoinverse-compatible output
arcfname = str(eventid) + '.arc'

#Get data for this event id
ev1dbm = getData(eventid,dbname)

#Write data in hypoinverse format in file ffname
write2Hypoinverse(ev1dbm,arcfname)