# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 11:26:49 2019

Functions (above) and script that calls functions (below) to get data for a 
selected event from an input Antelope database and write to a Hypoinverse-
compatible file

Antelope tables are in a slightly modified version of CSS (Center for Seismic
Studies) scehma

Assumes Antelope database tables are stored in the following way for database
named AntDB as an example:

workingdirectory/AntDB - name of directory where tables are stored
tables have names AntDB.origin, AntDB.arrival, etc.

Check the output after running to see if data was not written to the
output file because of length issues; this script will not write data to 
the file if it's the wrong length, but it will run and output the data that
could not be written to the command line

@author: Rachel
"""
import pandas as pd
import time
import numpy as np

def getData(eventid, dbid):
    print('Fetching data for event ' + str(eventid)+'\n')    
    
    #function merges key Antelope tables and extracts event information to 
    #return eventdb, a labeled dataframe with event location and pick 
    #information. Feed the dataframe into function write2Hypoinverse to write 
    #data to a properly formatted text file.
    
    eventdb = []
    
    #set paths to relevant tables
    pathevtbl = dbid + '/' + dbid + '.event'
    pathortbl = dbid + '/' + dbid + '.origin'
    pathassoctbl = dbid + '/' + dbid + '.assoc'
    patharrivtbl = dbid + '/' + dbid + '.arrival'
    pathnetwktbl = dbid + '/' + dbid + '.snetsta'    
        
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
    netwktbl = pd.read_csv(pathnetwktbl,header=None,delim_whitespace=True,
                           names=['netwk','sta','staid','lddate'])
                              
    #TO DO - ADD STATION AND NETWORK MAGNITUDE TABLES
                           
    eventinfo = evtbl[evtbl.evid==eventid]  
    
    #join data from other tables associated with event
    eventdb = pd.merge(eventinfo, ortbl, on='evid', how='left',
                       suffixes=['_ev','_or']) 
    eventdb = eventdb.merge(assoctbl, 
                            on='orid', how='left',suffixes=['','_assoc'])
    eventdb = eventdb.merge(arrivtbl, 
                            on='arid', how='left',suffixes=['','_arriv'])
    eventdb = eventdb.merge(netwktbl, 
                            on='sta', how='left',suffixes=['','_netsta']) 
     
    return eventdb
    
def writeLength(oldstr, newstr):
    #Function checks that any variable change is the correct length for 
    #Hypoinverse format. Also converts variable to a string regardless of 
    #input type
    
    accstr = oldstr #default return old string unless new string is right len
    if len(str(oldstr)) == len(str(newstr)):
        accstr=str(newstr)
    else:
        print('Cannot change string, '+str(newstr)+' check proposed string length')
    return accstr
    
def writeMdhm(etime):
    #function converts epoch time in Antelope database to 8-char month day
    #hour minute time format for hypoinverse    
    mdhmstr = '';
    mdhmstr += format(time.gmtime(etime).tm_mon,'02')
    mdhmstr += format(time.gmtime(etime).tm_mday, '02')
    mdhmstr += format(time.gmtime(etime).tm_hour, '02')
    mdhmstr += format(time.gmtime(etime).tm_min, '02')
    return mdhmstr

def write2Hypoinverse(eventdb, ffname):
    #Takes in a dataframe eventdb created by getData and writes that data in 
    #the proper format to a text file ffname that is properly formatted for 
    #Hypoinverse.

    #This format starts with an event header line. Many of the blank variables
    #will be over-written by hypoinverse
    #The following lines include pick information for the event
    #Each event then has a terminator line with input information about the 
    #event, such as the trial earthquake origin location and time

    #check if dataframe is empty i.e. event id provided does not exist
    if eventdb.empty == True:
        print('DataFrame is empty. Check that event ' + str(ffname) + \
        ' exists. Will not write to Hypoinverse file.\n')
        return    
    
    print('Writing event data to Hypoinverse file: '+ \
    str(eventdb['evid'].iloc[0]))    
    
    #initialize all variables as appropriately lengthed white space
    wsp = ' ' #initialize all variables as appropriately lengthed white space    
    
    #PART 1: HEADER LINE
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
    
    #set variables from whitespace to input database values
    #using function writeLength to protect correct length format of variables
    yr = writeLength(yr,str(eventdb['jdate'].iloc[0])[:4])  
    
    mdhm = writeLength(mdhm,writeMdhm(eventdb['time'].iloc[0]))   
    
    ortime = writeLength(ortime,"{:>4}".format(int(time.gmtime(\
    (eventdb['time'].iloc[0])).tm_sec*100+round(eventdb['time']\
    .iloc[0]%1*100,0))))
    
    latdeg = writeLength(latdeg,\
    "{:>2}".format(int(np.floor(abs(eventdb['lat'].iloc[0])))))
    
    if eventdb['lat'].iloc[0] < 0:
        ns = writeLength(ns,'S')
        
    latmin = writeLength(latmin,"{:>4}".format(\
    int(np.round(abs(eventdb['lat'].iloc[0]%1)*60*100))))
    
    londeg = writeLength(londeg,\
    "{:>3}".format(int(np.floor(abs(eventdb['lon'].iloc[0])))))   
    
    if eventdb['lon'].iloc[0] > 0:
        ew = writeLength(ew,'E')

    lonmin = writeLength(lonmin,"{:>4}".format(\
    int(np.round(abs(eventdb['lon'].iloc[0])%1*60*100))))
    
    dep = writeLength(dep, "{:>5}".format(int(np.round(\
    eventdb['depth'].iloc[0]*100))))
    
    #NOTE 0 PLACEHOLDER BECAUSE NCSN MAG NOT CALCULATED
    mag = writeLength(mag,'  0')     
    
    numt = writeLength(numt, "{:>3}".format(eventdb['nass'].iloc[0]))    
    
    evid = writeLength(evid,"{:>10}".format(int(eventdb['evid'].iloc[0])))
    
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
        
        if len(headln) != 165: #check that header line is the correct length 
            print('error: header line is incorrect length, not writing event'+\
            'to file\n')       
            return
        else:
            the_file.write(headln)    
    
    #PART 2: PICK LINES
    #loop through picks
    for iind, erow in eventdb.iterrows():
        
        #initialize pick data lines
        statcode = wsp*5 #left justified 5 letter station code (A5)
        statnet = wsp*2 #seismic network code (A2)
        comp1code = wsp*1 #station component code 1 letter (A1)
        comp3code = wsp*3 #station component code 3 letter (A3)
        prmk = wsp*2 #P remark (A2)
        pfm = wsp*1 #P first motion (A1)
        pweightcode = wsp*1 #P weight code (I1)
        pyr = wsp*4 #pick year (I4)
        pmdhm = wsp*8 #pick month, day, hour, minute (4I2)
        psec = wsp*5 #P pick second (F5.2)
        presid = wsp*4 #P pick residual (F4.2)
        normpwt = wsp*3 #normalized P weight (F3.2)
        ssec = wsp*5 #S arrival second (F5.2)
        srmk = wsp*2 #S remark (A2)
        sweightcode = wsp*1 #s weight code (I1)
        sresid = wsp*4 #s travel time residual (F4.2)
        amp = wsp*7 #amplitude peak-to-peak (F7.2)
        ampunitcd = wsp*2 #amp units code (I2)
        normswt = wsp*3 #s weight used (F3.2)
        pdelay = wsp*4 #P delay time (F4.2)
        sdelay = wsp*4 #S delay time (F4.2)
        epidist = wsp*4 #epicentral distance (F4.1)
        emang = wsp*3 #emergence angle at source (F3.0)
        ampmagwtcode = wsp*1 #amplitude magnitude weight code (I1)
        durmagwtcode = wsp*1 #duration magnitude weight code (I1)
        amppd = wsp*3 #period at which station amplitude measured (F3.2)
        statrmk = wsp*1 #station remark (A1)
        codadur = wsp*4 #coda duration in s (F4.0)
        azi2stat = wsp*3 #azimuth to station in deg E of N (F3.0)
        durmag = wsp*3 #station duration magnitude (F3.2)
        ampmag = wsp*3 #amplitude magnitude (F3.2)
        impP = wsp*4 #importance of P arrival (F4.3)
        impS = wsp*4 #importance of S arrival (F4.3)
        dscode = wsp*1 #data source code (A1)
        durmagcode = wsp*1 #duration magnitude code (A1)
        ampmagcode = wsp*1 #amplitude magnitude code (A1)
        statloccode = wsp*2 #2 letter stat location code (A2)
        amptype = wsp*2 #amplitude type (I2)
        acomp3code = wsp*3 #alternate 3-letter component code (A3)
        ampmagyn = wsp*1 #X if station amplitude mag not used in event mag (A1)
        durmagyn = wsp*1 #X if station duration mag not used in event mag (A1)
        
        #set necessary variables to input database values
        
        statcode = writeLength(statcode,"{:<5}".format(erow['sta']))
        
        statnet = writeLength(statnet,"{:<2}".format(erow['netwk']))
    
        if 'Z' in erow['chan']:
            comp1code = writeLength(comp1code,'V')
        else:
            comp1code = writeLength(comp1code,'H')
            
        comp3code = writeLength(comp3code,"{:<3}".format(erow['chan'][:3]))
        
        pyr = writeLength(pyr,time.gmtime(erow['time_arriv']).tm_year)
        pmdhm = writeLength(pmdhm,writeMdhm(erow['time_arriv'])) 
        #write P versus S arrival info in different variables    
        if erow['iphase'] == 'P':
            prmk = writeLength(prmk,'iP')
        
            #assign first motion direction if available            
            if erow['fm'] == 'U':
                pfm = writeLength(pfm,'U')
            elif erow['fm'] == 'D':
                pfm = writeLength(pfm,'D')
    
            #LATER MAKE BETTER WEIGHTING SCHEME DOWNWEIGHT ARRIVALS FAR AWAY
            pweightcode = writeLength(pweightcode,2)
    
            psec = writeLength(psec,"{:>5}".format(int(time.gmtime\
            ((erow['time_arriv'])).tm_sec*100+round(erow['time_arriv']\
            %1*100,0))))
            
        elif erow['iphase'] == 'S':
            srmk = writeLength(srmk,'ES')
            
            sweightcode = writeLength(sweightcode,2)
            
            ssec = writeLength(ssec,"{:>5}".format(int(time.gmtime\
            ((erow['time_arriv'])).tm_sec*100+\
            round(erow['time_arriv']%1*100,0))))
    
        #placeholder 0s or no weights(4) where no measurements available
        amp = writeLength(amp, "{:>7}".format(0))    
    
        ampunitcd = writeLength(ampunitcd, "{:>2}".format(0))
    
        durmagwtcode = writeLength(durmagwtcode, 4)
    
        codadur = writeLength(codadur, "{:>4}".format(0))    
    
        durmag = writeLength(durmag, "{:>3}".format(0))
        
        #concat to single line
        pline = statcode+statnet+wsp+comp1code+comp3code+wsp+prmk+pfm+\
        pweightcode+pyr+pmdhm+psec+presid+normpwt+ssec+srmk+wsp+\
        sweightcode+sresid+amp+ampunitcd+normswt+pdelay+sdelay+epidist+\
        emang+ampmagwtcode+durmagwtcode+amppd+statrmk+codadur+azi2stat+\
        durmag+ampmag+impP+impS+dscode+durmagcode+ampmagcode+statloccode+\
        amptype+acomp3code+ampmagyn+durmagyn+'\n' 
        
        #write to event file
        with open(ffname, 'a') as the_file:
            if len(pline) != 121: #check that pick line is the correct length 
                print('error:pick line is incorrect length, not writing pick'+\
                'to file')        
            else:
                the_file.write(pline)
                
        #END OF LOOP FOR WRITING PICK LINES
                
            
    #PART 3: TERMINATOR LINE
    #initialize terminator line variables to correct length        
    trialhrmin = wsp*4 #trial hour and minute (A4)
    trialsec = wsp*4 #trial second (2I2)
    triallatdeg = wsp*2 #trial latitude (F2.0)
    triallatmin = wsp*4 #trial latitude minutes (F4.2)
    triallongdeg = wsp*3 #trial longitude (F3.0)
    triallongmin= wsp*4 #trial longitude minutes (F4.2)
    trialdepth = wsp*5 #trial depth (F5.2)
    trialidnum = wsp*10 #trial ID number (I10)
    
    #set variables to proper input parameters - just event ID for now
    #this is being skipped because current documentation doesn't clearly 
    #indicate if negative degree locations are appropriately considered by
    #hypoinverse code when in terminator line; inputting preliminary origin
    #in header line instead
    
    #trialhrmin = writeLength(trialhrmin,mdhm[4:])
    #trialsec = writeLength(trialsec,ortime)
    #triallatdeg = writeLength(triallatdeg,latdeg)
    #triallatmin = writeLength(triallatmin,latmin)
    #triallongdeg = writeLength(triallongdeg,londeg)
    #triallongmin = writeLength(triallongmin,lonmin)
    #trialdepth = writeLength(trialdepth,dep)
    trialidnum = writeLength(trialidnum,evid)
    

    #concat to a single line
    termline=wsp*6+trialhrmin+trialsec+triallatdeg+wsp+triallatmin+\
    triallongdeg+wsp+triallongmin+trialdepth+wsp*28+trialidnum+'\n'  
    
    #write line to event file   
    with open(ffname, 'a') as the_file:
        if len(termline) != 73: #check that pick line is the correct length 
            print('error: terminator line is incorrect length, not writing '+\
            'to file') 
        else:
            the_file.write(termline)
    
    return
    
def writeSta2Hypoinverse(dbid, ffname, append_stations=False):
    #function uses Antelope tables to write a hypoinverse station file
    #dbid is the name of the directory within working directory that holds
    #the antelope tables, where the database is also called dbid
    #ffname is the name of the station file to write to
    #append stations is a T/F variable to indicate whether to append to file
    #ffname (True) or to over-write ffname (False)

    stadb = [] #initialize master station dataframe
    newlines = [] #initialize variable to test for duplicate station lines    
    
    #Paths to antelope tables with station data
    pathnetwktbl = dbid + '/' + dbid + '.snetsta'
    pathinsttbl = dbid + '/' + dbid + '.instrument'
    pathsensortbl = dbid + '/' + dbid + '.sensor'
    pathsitetbl = dbid + '/' + dbid + '.site'
    pathsitechantbl = dbid + '/' + dbid + '.sitechan'
    pathinsttbl = dbid + '/' + dbid + '.instrument'
    
    if append_stations==False:
        openvar = 'w'
    else:
        openvar = 'a'
        
    #load and join tables into a single dataframe
    sitetbl = pd.read_fwf(pathsitetbl,header=None,colspecs='infer',
                          names=['sta','ondate','offdate','lat','lon',
                                 'elev','staname','statype','refsta',
                                 'dnorth','deast','lddate'])
    sitechantbl = pd.read_csv(pathsitechantbl,header=None,
                              delim_whitespace=True,names=['sta','chan',
                              'ondate','chanid','offdate','ctype','edepth',
                              'hang','vang','descrip','lddate'])
    sensortbl = pd.read_fwf(pathsensortbl,header=None,colspecs='infer',
                            names=['sta','chan','time','endtime','inid',
                                   'chanid','jdate','calratio','calper',
                                   'tshift','instant','lddate'])
    insttbl = pd.read_fwf(pathinsttbl,header=None,colspecs='infer',
                          names=['inid','insname','instype','band','digital',
                                 'samprate','ncalib','ncalper','dir','dfile',
                                 'rsptype','lddate'])
    nettbl = pd.read_fwf(pathnetwktbl,header=None,colspecs='infer',
                         names=['net','sta','staid','lddate'])
                                    
    stadb = pd.merge(sitetbl,sitechantbl,on='sta',how='left',
                     suffixes=['','_chan'])
    #if sensor type changes during deployment then this may add rows
    stadb = stadb.merge(sensortbl,on=['sta','chan'],
                        how='left',suffixes=['','_sens'])
    stadb = stadb.merge(insttbl,on='inid',how='left',suffixes=['','_inst'])                                
    stadb = stadb.merge(nettbl,on='sta',how='left',suffixes=['','_netwk'])
    
    #loop through stations and write station into for file ffname
    for iind,erow in stadb.iterrows():
        #initialize properly-lengthed variables for Hypoinverse station file
        wsp = ' '    
        statcode = wsp*5 #left justified 5 letter station code (A5, 1X)
        statnet = wsp*2 #seismic network code (A2, 1X)
        comp1code = wsp*1 #station component code 1 letter (A1)
        comp3code = wsp*3 #station component code 3 letter (A3, 1X)
        statweight = wsp*1 #station weight (A1)    
        latdeg = wsp*2 #latitude in degrees (I2, 1X)    
        latmin = wsp*7 #latitude in minutes (F7.4)
        ns = wsp*1 #blank for N, S for south (A1)
        longdeg = wsp*3 #longitude in degrees (I3,1X)
        longmin = wsp*7 #longitude in minutes (F7.4)
        ew = wsp*1 #blank for W, E for east (A1)
        elev = wsp*4 #elevation in m (I4)
        amppd = wsp*3 #period at which station amplitude measured (F3.1,2X)    
        altcrust = wsp*1 #2 or A indicates using an alternate crustal model(A1)
        stadelayrmk = wsp*1 #optional station delay remark (A1)
        pdelay1 = wsp*5 #P delay (s) for set 1 (F5.2, 1X)
        pdelay2= wsp*5 #P delay (s) for set 2 (F5.2, 1X)
        ampmagadj = wsp*5 #amplitude magnitude correction (F5.2)
        ampmagwt = wsp*1 #amplitude magnitude weight (A1)
        durmagadj = wsp*5 #duration magnitude correction (F5.2)
        durmagwt = wsp*1 #duration magnitude weight (A1)
        itype = wsp*1 #instrument type code (I1)
        icalib = wsp*6 #instrument calibration factor (F6.2)
        sta2code = wsp*2 #2 letter station code (A2)
        sta3altcode = wsp*3 #3 letter alternate component code (A3)
        negdep = wsp*1 #make depth negative regardless of sign (A1)
        
        #set station line variables using data from tables
        statcode = writeLength(statcode,"{:<5}".format(erow['sta']))
        
        statnet = writeLength(statnet,"{:<2}".format(erow['net']))
        
        if 'Z' in erow['chan']:
            comp1code = writeLength(comp1code,'V')
        else:
            comp1code = writeLength(comp1code,'H')
                
        comp3code = writeLength(comp3code,"{:<3}".format(erow['chan'][:3]))
    
        latdeg = writeLength(latdeg,\
        "{:>2}".format(int(np.floor(abs(erow['lat'])))))    
        
        if erow['lat'] < 0:
            ns = writeLength(ns,'S')
            
        latmin = writeLength(latmin,"{:>7}".format(\
        int(np.round(abs(erow['lat']%1)*60*10000))))
        
        longdeg = writeLength(longdeg,\
        "{:>3}".format(int(np.floor(abs(erow['lon'])))))   
        
        if erow['lon'] > 0:
            ew = writeLength(ew,'E')
    
        longmin = writeLength(longmin,"{:>7}".format(\
        int(np.round(abs(erow['lon'])%1*60*10000))))
        
        elev = writeLength(elev,"{:>4}".format(int(erow['elev']*1000)))
        
        #instrument type code - does a text search to guess instrument type
        #note that this does not distinguish between inst types 1 and 3, 1 Hz
        #L4C velocity transducers where one type has attenuation history and  
        #the other has CAL factor history; defaults to type 1
        if str(erow['insname']).find('Wood-Anderson')>-1:
            itype = writeLength(itype,0)
        elif str(erow['insname']).find('L4C')>-1:
            itype = writeLength(itype,1)
        elif str(erow['insname']).find('Sprengnether')>-1:
            itype = writeLength(itype,1)
        elif str(erow['insname']).find('Nanometrics')>-1:
            itype = writeLength(itype,4)
        elif str(erow['insname']).find('Guralp')>-1:
            itype = writeLength(itype,5)
        elif str(erow['insname']).find('STS-1')>-1:
            itype = writeLength(itype,6)
        elif str(erow['insname']).find('STS-2')>-1:
            itype = writeLength(itype,7)
        
        #calib factor -- based on ncalib value in table  
        if np.isnan(erow['ncalib']) == False:        
            icalib = writeLength(icalib,"{:>6}".format(\
            int(round(erow['ncalib']*100))))
        else:
            icalib = writeLength(icalib,"{:>6}".format(0.0))
        
        #these variables default to 0, uncertain antelope table equivalent
        amppd = writeLength(amppd,"0.0")    
        pdelay1 = writeLength(pdelay1," 0.00")    
        pdelay2 = writeLength(pdelay2," 0.00")
        ampmagadj = writeLength(ampmagadj," 0.00")
        durmagadj = writeLength(durmagadj," 0.00")
        
        #concat variables to a single line
        staline = statcode+wsp+statnet+wsp+comp1code+comp3code+wsp+statweight+\
        latdeg+wsp+latmin+ns+longdeg+wsp+longmin+ew+elev+amppd+wsp*2+altcrust+\
        stadelayrmk+pdelay1+wsp+pdelay2+wsp+ampmagadj+ampmagwt+durmagadj+\
        durmagwt+itype+icalib+sta2code+sta3altcode+negdep+'\n'
    
        #check that the line is not a duplicate already written to file
        if staline not in newlines:            
            newlines.append(staline)
            
            #write line to file and add to newlines
            with open(ffname, openvar) as the_file:
                if len(staline) != 87: #check pick line is the correct length 
                    print(len(staline))
                    print('error: terminator line is incorrect length, '+ \
                    'not writing to file\n') 
                else:
                    the_file.write(staline)
        
        else:
            print('station line duplicate not written to station file\n')
            print('duplicate line '+staline+'\n')
    
        openvar='a' #add remaining station entries to file
    return stadb
#######################MODIFY BELOW HERE#######################
    
#Database name - put tables in folder in working directory that matches dbname
#Or change paths to files in function getData
dbname = 'GADBPart1'

#PART 1 for writing station master file
#stadb = writeSta2Hypoinverse(dbname,dbname+'.sta',append_stations=False)

#PART 2a for fetching 1 event's data from tables and writing output to file
#Database event id - integer corresponding to event to prep for hypoinverse 
#eventid = 69
#Arc file name for writing hypoinverse-compatible output
#arcfname = str(eventid) + '.arc'
#Get data for this event id
#ev1dbm = getData(eventid,dbname)
#Write data in hypoinverse format in file ffname
#write2Hypoinverse(ev1dbm,arcfname)

#PART 2b for multiple event's data from tables and writing output to file
evidlist = [69,99,2504,202,366,415,2507,435,653,1074,1104,1282,1331,
            1510,1593,1594,1682,1798,1819,1901,1905,2028,2151,2241,
            2263,2270,2272,2308,2336,2360,2362,2419,2420,2421,2436,
            2437,2445,2457,2517,2468,2470,2473,2476,2478]
arcfname = 'GADBPart1Events.arc'
for seisevent in evidlist:
    evdbm = getData(seisevent,dbname)
    write2Hypoinverse(evdbm,arcfname)