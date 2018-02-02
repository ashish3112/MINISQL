import sys
import csv
import re
import copy
class table(object):
    def __init__(self,table_attribute,val):
        self.table_attribute=table_attribute
        self.table_name=table_attribute[0]
        self.table_nsattribute=copy.deepcopy(table_attribute)
        self.tdict={}
        self.joined_col=''
        self.val=val
    def read_table(self):
        try:
            fr=open(self.table_name+'.csv','r')
        except IOError:
            print "Table defined in metadata don't exists"
            exit()
        lines=csv.reader(fr)
        for line in lines:
            #print line
            tval=[]
            for fval in line:
                fval=int(fval)
                tval.append(fval)
            self.val.append(tval)
        fr.close()
    def get_val(self):
        return self.val
    def get_attlist(self):
        return self.table_nsattribute
    def intialise_dict(self):
        for i in range(1,len(self.table_attribute)):
            self.table_nsattribute[i]=self.table_name+'.'+self.table_nsattribute[i]
        #print self.table_attribute
        for i in range(1,len(self.table_attribute)):
            self.tdict[self.table_attribute[i]]=i-1
    def check_agg(self,shows):
        temp1=re.search('max\([A-Za-z0-9]+\)',shows);
        temp2=re.search('min\([A-Za-z0-9]+\)',shows);
        temp3=re.search('avg\([A-Za-z0-9]+\)',shows);
        if temp1!=None:
            return 1
        elif temp2!=None:
            return 2
        elif temp3!=None:
            return 3
    def print_table(self,seltype,listshows):
        if seltype==-1:
            for i in range(1,len(self.table_attribute)):
                if self.table_attribute[i]!=self.joined_col:
                    print self.table_attribute[i],
            print
            for i in self.val:
                for j in range(0,len(i)):
                    if self.table_attribute[j+1]!=self.joined_col:
                        print i[j],
                print
        elif seltype==0:
            #print self.table_attribute
            for shows in listshows:
                if shows not in self.table_attribute:
                    print "No "+shows+" coloumn in selected table"
                    print "Also note to mention coloumn with table name while using two tables"
                    return
            for i in range(1,len(self.table_attribute)):
                if self.table_attribute[i] in listshows:
                    print self.table_attribute[i],
            print
            for i in range(len(self.val)):
                for j in range(len(self.val[i])):
                    if self.table_attribute[j+1] in listshows:
                        print self.val[i][j],
                print
        elif seltype==1:
            tsum=0
            for shows in listshows:
                tys=self.check_agg(shows)
                shows=re.sub('\(',' ',shows)
                shows=re.sub('\)',' ',shows)
                tshows=shows.split()
                if tys==1:
                    print "Max("+tshows[1]+")"
                    tsum=-1000000000000
                    for i in range(len(self.val)):
                        tsum=max(tsum,self.val[i][self.tdict[tshows[1]]])
                elif tys==2:
                    tsum=1000000000000
                    print 'Min('+tshows[1]+')'
                    for i in range(len(self.val)):
                        tsum=min(tsum,self.val[i][self.tdict[tshows[1]]])
                elif tys==3:
                    print 'Avg('+tshows[1]+')'
                    tsum=0
                    for i in range(len(self.val)):
                        tsum+=self.val[i][self.tdict[tshows[1]]]
                    tsum=float(tsum)/float(len(self.val))
                print tsum
        elif seltype==2:
            listcols=[]
            for shows in listshows:
                shows=re.sub('\(',' ',shows)
                shows=re.sub('\)',' ',shows)
                tshows=shows.split()
                listcols.append(tshows[1])
            pf=[]
            tpf=[]
            print 'distict(',
            for i in range(1,len(self.table_attribute)):
                if self.table_attribute[i] in listcols:
                    print self.table_attribute[i],
            print ')'
            for i in self.val:
                tpf=[]
                for j in range(1,len(self.table_attribute)):
                    if self.table_attribute[j] in listcols:
                        tpf.append(i[j-1])
                if tpf not in pf:
                    pf.append(tpf)
            for i in pf:
                for j in i:
                    print j,
                print
if __name__ == "__main__":
    t1attb=['table1','A','B','C']
    t1=table(t1attb,[])
    t1.read_table()
    t1.print_table()
