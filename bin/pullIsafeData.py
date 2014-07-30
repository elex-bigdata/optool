import datetime
import smtplib
import json
import urllib
import time
from email.mime.text import MIMEText
import Queue
import threading
import sys

#ref : organic / banner/ adwords /
#groupby : ref1 / version / nation / language / os / geoip

refs = ["organic","banner","adwords"]
groupbys = ["ref1","language","os","version","geoip"]

#refs = ["adwords"]
#groupbys = ["ref1"]

#datetime.datetime.now().strftime('%b-%d-%y %H:%M:%S');
today = datetime.date.today()
day1 = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
day2 = (today - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
day6 = (today - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
#,"liuqixing@elex-tech.com"
mailto_list = ["liqiang@xingcloud.com","liuqixing@elex-tech.com"]
mail_host = "smtp.qq.com"
mail_user = "xamonitor@xingcloud.com"
mail_pass = "22C1NziwxZI5F"
#day1 day1 ref ref groupby day1 day1 ref ref groupby ref ref groupby ref ref groupby day2 day2 ref ref groupby ref ref groupby day6 day6

queue = Queue.Queue()

with open(sys.path[0]+"/params.txt") as f:
    params = f.read()

class myThread (threading.Thread):
    def __init__(self, ref, groupby):
        threading.Thread.__init__(self)
        self.threadID = ref+"_"+groupby
        self.ref = ref
        self.groupby = groupby

    def run(self):
        line = params%(day1,day1,self.ref,self.ref,self.groupby,day1,day1,self.ref,self.ref,self.groupby,self.ref,self.ref,self.groupby,self.ref,
                       self.ref,self.groupby,day2,day2,self.ref,self.ref,self.groupby,self.ref,self.ref,self.groupby,day6,day6)
        url = "http://69.28.58.61:8082/dd/query?pagesize=100&params=" + line #urllib.quote(line)

        global queue
        try:
            res = self.queryData(url)
            while "pending" in res:
                time.sleep(1*60)
                print "retry %s %s"%(self.ref,self.groupby)
                res = self.queryData(url)

            jstr = json.loads(res)
            result = self.parseData(jstr["datas"])
            queue.put(result)
        except Exception, e:
            print str(e)
            print "error for ",self.ref,self.groupby

    def parseData(self,datas):
        global queue
        kv = {}
        data2map = {}
        for data in datas:
            nv = data[1]["new"]
            if not isinstance(nv, int):
                nv = -1
            kv[data[0]] = nv
            data2map[data[0]] = data[1]

        sortedData = sorted(kv.items(), key=lambda d:d[1], reverse=True)

        content = self.ref+self.groupby + ':<div>'
        content += "<table style='border: 1px solid'>"
        content += "<tr><td colspan='5' style='border:1px solid'>%s %s</td></tr>"%(self.ref,self.groupby)
        content += "<tr><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td></tr>\n"%("","new","uninstall","2day","6day")

        totalNew = 0
        totalUninstall = 0
        for sd in sortedData:
            data = data2map[sd[0]]
            day2percent = data["2day"]
            if isinstance(day2percent, float):
                day2percent = str(day2percent*100) + "%"
            day6percent = data["6day"]
            if isinstance(day6percent, float):
                day6percent = str(day6percent*100) + "%"

            content += "<tr><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td></tr>\n"%(sd[0],data["new"],data["uninstall"],day2percent,day6percent)
            if isinstance(data["new"], int):
                totalNew += data["new"]
            if isinstance(data["uninstall"], int):
                totalUninstall += data["uninstall"]
        content += "<tr><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td><td style='border:1px solid'>%s</td></tr>\n"%("",totalNew,totalUninstall,"","")

        content +="</table></br></div>"

        queue.put(content)

    def queryData(slef, url):
        page = urllib.urlopen(url)
        res = page.read()
        page.close()
        return res



def sendMail(content):
    me="xamonitor@xingcloud.com"
    msg = MIMEText(content, _subtype='html', _charset='gb2312')
    msg['Subject'] = "sof-isafe " + day1
    msg['From'] = "xamonitor@xingcloud.com"
    msg['To'] = ";".join(mailto_list)
    try:
        s = smtplib.SMTP()
        s.connect(mail_host)
        s.login(mail_user,mail_pass)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
        return True
    except Exception, e:
        print str(e)
        return False

print "Start at ", datetime.datetime.now()
thread_list = list();
for ref in refs:
    for groupby in groupbys:
        print ref, groupby, datetime.datetime.now()
        thread_list.append(myThread(ref,groupby))

for thread in thread_list:
    thread.start()

for thread in thread_list:
    thread.join()

contentmap = {}
mailcontent = ""

while not queue.empty():
    xx = queue.get()
    if xx:
        i = xx.index(":")
        contentmap[xx[0:i]] = xx[i+1:]

mailcontent += '<div>'
for ref in refs:
    for groupby in groupbys:
        mailcontent += contentmap[ref+groupby]
mailcontent += '</div>'

if not mailcontent:
    mailcontent = "error"

sendMail(mailcontent)
print "End at ", datetime.datetime.now()
