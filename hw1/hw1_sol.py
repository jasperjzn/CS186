# ### * BEGIN STUDENT CODE *

# In[4]:

import apachetime
import time

def apache_ts_to_unixtime(ts):
    """
    @param ts - a Apache timestamp string, e.g. '[02/Jan/2003:02:06:41 -0700]'
    @returns int - a Unix timestamp in seconds
    """
    dt = apachetime.apachetime(ts)
    unixtime = time.mktime(dt.timetuple())
    return int(unixtime)


# In[96]:

import csv
import os
import re

def process_logs(dataset_iter):
    """
    Processes the input stream, and outputs the CSV files described in the README.    
    This is the main entry point for your assignment.
    
    @param dataset_iter - an iterator of Apache log lines.
    
    My code that parses a log into ip address and time comes from "http://www.seehuhn.de/blog/52"
    """
    # --------------------------------------------------------------------------------------------------
    with open("hits.csv", "w+") as hits_file:
        fieldnames=["ip","timestamp"]
        writer = csv.DictWriter(hits_file, fieldnames = fieldnames, lineterminator='\n')
        writer.writeheader()
        
        for i, line in enumerate(dataset_iter):
            this_ip=line.split( )[0]
            time_1=line.split( )[3]
            time_2=line.split( )[4]
            writer.writerow({"ip":this_ip,"timestamp":apache_ts_to_unixtime(time_1 + " " + time_2)})
        
    hits_file.close()

    #---------------------------------------------------------------------------------------------------
    
    with open("hits.csv","rb") as f:
        with open("temp.csv","w+") as f1:
            f.next() 
            for line in f:
                f1.write(line)
    f.close()
    f1.close()
                
    ! sort temp.csv > temp1.csv
    
        
    with open("sessions.csv", "w+") as sessions_file:
        fieldnames_1=["ip","session_length","num_hits"]
        writer_1=csv.DictWriter(sessions_file, fieldnames=fieldnames_1, lineterminator='\n')
        writer_1.writeheader()
        
        current_ip=" "
        current_first=" "
        current_last=" "
        current_num = " "
        
        with open("temp1.csv", "rb") as temp_file:
            reader = csv.reader(temp_file)
            for row in reader:
                this_ip = row[0]
                this_request_time = row[1]
                
                
                if current_ip == " ":
                    current_ip = this_ip
                    current_first = this_request_time
                    current_last = this_request_time
                    current_num = "1"
                else:    
                    if this_ip == current_ip:
                        if (int(this_request_time)-int(current_last)) <= 1800:
                            current_last = this_request_time
                            current_num = str(int(current_num) + 1)
                        else:
                            writer_1.writerow({"ip":current_ip,"session_length":str(int(current_last)-int(current_first)),"num_hits":current_num})
                            current_first = this_request_time
                            current_last = this_request_time
                            current_num="1"
                    else:
                        writer_1.writerow({"ip":current_ip,"session_length":str(int(current_last)-int(current_first)),"num_hits":current_num})
                        current_ip = this_ip
                        current_first = this_request_time
                        current_last = this_request_time
                        current_num = "1"
            writer_1.writerow({"ip":current_ip,"session_length":str(int(current_last)-int(current_first)),"num_hits":current_num})
            
    sessions_file.close()
    temp_file.close()
    os.remove("temp.csv")
    os.remove("temp1.csv")
    
    # --------------------------------------------------------------------------------------------------
    with open("sessions.csv","rb") as f2:
        with open("temp2.csv","w+") as f3:
            f2.next() 
            for line in f2:
                f3.write(line)
    f2.close()
    f3.close()
    
    ! sort -n -t "," -k2 temp2.csv > temp3.csv
    
    with open("session_length_plot.csv", "w+") as plot_file:
        fieldnames_2=["left","right","count"]
        writer_2 = csv.DictWriter(plot_file, fieldnames = fieldnames_2, lineterminator='\n')
        writer_2.writeheader()
        current_lower = 0
        current_upper = 2
        accumulate = 0
        
        with open("temp3.csv", "rb") as temp_file:
            reader = csv.reader(temp_file)
            for row in reader:
                this_count = row[1]
                if current_lower <= int(this_count) and int(this_count) < current_upper:
                    accumulate = accumulate + 1
                else:
                    writer_2.writerow({"left":str(current_lower),"right":str(current_upper),"count":accumulate})
                    current_lower = current_upper
                    current_upper = current_upper * 2
                    accumulate = 1
            writer_2.writerow({"left":str(current_lower),"right":str(current_upper),"count":accumulate})    
    plot_file.close()
    temp_file.close()
    os.remove("temp2.csv")
    os.remove("temp3.csv")
        
        
        


# ### * END STUDENT CODE *
import os
DATA_DIR = os.environ['MASTERDIR'] + '/sp16/hw1/'

import zipfile

def process_logs_large():
    """
    Runs the process_logs function on the full dataset.  The code below 
    performs a streaming unzip of the compressed dataset which is (158MB). 
    This saves the 1.6GB of disk space needed to unzip this file onto disk.
    """
    with zipfile.ZipFile(DATA_DIR + "web_log_large.zip") as z:
        fname = z.filelist[0].filename
        f = z.open(fname)
        process_logs(f)
        f.close()
process_logs_large()
