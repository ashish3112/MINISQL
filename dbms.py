import sqlparse
import re
import sys
from table import table
import copy
class DBMS(object):
    def __init__(self,mdname):
        self.mdname=mdname
        self.tabledict={}
    def make_tables(self):
        fr=open(self.mdname,'r+') ##Error handling needed on this line
        data=fr.read()
        data=data.split('<begin_table>')
        for lines in data:
            if lines!='':
                lines=re.sub('\r','',lines)
                lines=lines.split('\n')
                while '' in lines: lines.remove('')
                lines.pop()
                ctable=table(lines,[])
                ctable.intialise_dict()
                ctable.read_table()
                self.tabledict[lines[0]]=ctable
        fr.close()
    def synerr(self):
        print "Incorrect syntax"
    def notat(self):
        print "Not a table"
        exit()
    def join(self,table1,table2):
        val1=self.tabledict[table1].get_val()
        val2=self.tabledict[table2].get_val()
        ret=[]
        for i in range(len(val1)):
            #tret=val1[i]
            for j in range(len(val2)):
                tret=val1[i]
                tret=tret+val2[j]
                ret.append(tret)
        #print ret
        attl1=self.tabledict[table1].get_attlist()
        attl2=self.tabledict[table2].get_attlist()
        jtableatt=attl1+attl2[1:]
        jtable=table(jtableatt,ret)
        jtable.intialise_dict()
        return jtable
    def where_remover(self,rtable,wheres):
        temp1=re.search('[A-Za-z.0-9]+=[0-9]+',wheres)
        temp2=re.search('[A-Za-z.]+=[A-Za-z.0-9]+',wheres)
        if temp1!=None:
            listadel=[]
            tval=re.split('[A-Za-z.0-9]+=',wheres)
            tval=int(tval[1])
            coloumnname=re.split('=[0-9]+',wheres)
            coloumnname=coloumnname[0]
            val=rtable.get_val()
                #print val
            for values in val:
                if coloumnname in rtable.tdict:
                    if values[rtable.tdict[coloumnname]]==tval:
                        listadel.append(values)
                else:
                    self.notat()
            #print listadel
            rtable.val=listadel
        elif temp2!=None:
            listadel=[]
            coloumnname=wheres.split('=')
            col1=coloumnname[0]
            col2=coloumnname[1]
            val=rtable.get_val()
                #print val
            for values in val:
                if values[rtable.tdict[col1]]==values[rtable.tdict[col2]]: #Error handling needed
                    listadel.append(values)
                #print listadel
            rtable.val=listadel
            rtable.joined_col=col1
        else:
            #print "boom"
            self.synerr()
            exit()
    def get_table(self,listtables,listwheres):
        #print listwheres
        rtable=None
        if len(listtables)==1:
            rtable=self.tabledict[listtables[0]]
        else:
            rtable=self.join(listtables[0],listtables[1])
        if len(listwheres)==0:
            return rtable
        else:
            if len(listwheres)==2:
                self.synerr()
                exit()
            elif len(listwheres)==1:
                self.where_remover(rtable,listwheres[0])
                return rtable
            elif len(listwheres)==3:
                if listwheres[1].upper()=='AND':
                    self.where_remover(rtable,listwheres[0])
                    self.where_remover(rtable,listwheres[2])
                    return rtable
                elif listwheres[1].upper()=='OR':
                    rtable1=copy.deepcopy(rtable)
                    rtable2=copy.deepcopy(rtable)
                    self.where_remover(rtable1,listwheres[0])
                    self.where_remover(rtable2,listwheres[2])
                    val1=rtable1.get_val()
                    val2=rtable2.get_val()
                    for tval in val2:
                        if tval not in val1:
                            val1.append(tval)
                    rtable.val=val1
                    return rtable
                else:
                    self.synerr()
                    exit()
    def handleq(self,qy,qtype):
        listshows=str(qy.tokens[2])
        listshows=listshows.split(',')
        listtables=str(qy.tokens[6])
        listtables=listtables.split(',')
        listwheres=[]
        if qtype==3:
            listwheres=str(qy.tokens[8])
            listwheres=listwheres.strip('WHERE ')
            if listwheres=='':
                self.synerr()
                exit()
            listwheres=listwheres.split(' ')
        #print listwheres
        if listtables[0] not in self.tabledict:
            self.notat()
            return
        elif len(listtables)==2 and listtables[1] not in self.tabledict:
            self.notat()
            return
        else:
            proctable=self.get_table(listtables,listwheres)
        #proctable=self.tabledict[listtables[0]]
        if listshows[0]=='*':
            proctable.print_table(-1,listshows)
            return
        seltype=0  ## Normal 0,Aggregate 1,Distinct 2
        temp1=re.search('max\([A-Za-z0-9]+\)',listshows[0]);
        temp2=re.search('min\([A-Za-z0-9]+\)',listshows[0]);
        temp3=re.search('avg\([A-Za-z0-9]+\)',listshows[0]);
        if temp1!=None or temp2!=None or temp3!=None:
            seltype=1
        temp4=re.search('distinct\([A-Za-z0-9]+\)',listshows[0]);
        if temp4!=None:
            seltype=2
        proctable.print_table(seltype,listshows)
    def checkquery(self,qy):
        qt=-1
        ret=0
        if len(qy.tokens)>9 or len(qy.tokens)==8:
            self.synerr()
            return qt
        if str(qy.tokens[0])!='SELECT':
            self.synerr()
            return qt
        else:
            ret+=1
        if str(qy.tokens[4])!='FROM':
            #print "trace"
            self.synerr()
            return qt
        else:
            ret+=1
        if len(qy.tokens)==9:
            if str(qy.tokens[8]).startswith('WHERE'):
                ret+=1
            else:
                self.synerr()
                return qt
        if ret==1:
            self.synerr()
            return qt
        return ret
    def query(self,unfquery):
        fquery=sqlparse.format(unfquery, keyword_case='upper')
        toks=sqlparse.parse(fquery)
        qy=toks[0]
        #print qy.tokens
        typeq=self.checkquery(qy)
        if typeq==-1:
            exit()
        else:
            self.handleq(qy,typeq)
if __name__ == '__main__':
    if len(sys.argv)!=2:
        print "Incorrect number of argument presented"
        exit()
    db=DBMS('metadata.txt')
    db.make_tables()
    #print sys.argv[1]
    db.query(sys.argv[1])
