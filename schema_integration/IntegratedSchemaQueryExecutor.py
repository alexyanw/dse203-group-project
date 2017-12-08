import pickle

import re
import sys
import collections
sys.path.append("..\\query_capability\\")

from ViewDefinitions import view
from hybrid_engine import HybridEngine

gsm = pickle.load(open('global_schema_mappings.pkl','rb'))

#for key in gsm:
#    print gsm[key].get_name()



def execute(query,condition=None,limit=None,debug=False,just_debug=False):

    qpe_dict_list=[]
    qpe_dl=dict()
    qpe_dl['table']=[]

    try:
        query = query.lower()
        [head,body] = query.split('->')
    except :
        print ("Unexpected format. Please stick to this format \nans(x,y)->viewname(x,_,y_)")
        sys.exit(1)
    #qpe_dl 'qpe_datalog' is a dictionary as expected by qpe team
    qpe_dl['result']=head
    head_cols = get_cols(head)

    while True:
        #ptrn = '^(\S*?)\((\S*?)\)'        #pattern to find the view in body
        #match = re.search(ptrn,body)
        strt = body.find('(')
        stp  = body.find(')')
        if strt!=-1 and stp !=-1:
            gv = body[:strt]
            c_body = gv+'('+body[strt+1:stp]+')'
            gv_cols = get_cols(c_body)
            l = find_needed_cols(gv_cols,head_cols,condition)
            dl = gsm[gv].get_child_dl(needed_cols=l)
            #dl = gsm[gv].unwrap()
            #print dl
            qpe_dl['table'] += dl
        else :
            #print("Unable to find body view.Please stick to this format. \nans(x,y)->viewname(x,_,y_)")
            #sys.exit(1)
            break
        body=body[stp+2:]

    qpe_dl['condition'] =condition
    qpe_dl['limit']=limit
    qpe_dl['table']=refine_query(qpe_dl['table'])


    qpe_dict_list=[qpe_dl]
    if debug==True:
        print_qpe_dict_list(qpe_dict_list)

    if just_debug==True:
        return "  "

    engine = HybridEngine(
        postgres={'server': '132.249.238.27', 'port': 5432, 'database': 'bookstore_pr', 'user': 'student',
                  'password': '123456'},
        asterix={'server': '132.249.238.32', 'port': 19002, 'dataverse': 'bookstore_pr'},
        solr={'server': '132.249.238.28', 'port': 8983, 'core': 'bookstore_pr'})
    qpe_dict_list=[qpe_dl]
    print(qpe_dl['table'])

    return(engine.queryDatalog(qpe_dict_list))

    #return qpe_dl

    #return(engine.queryDatalog(qpe_dict))

    #return qpe_dl

    #qpe_dl['condition'] =


#Function to remove any redundant subgoals.
def refine_query(ql):

    #for x in ql
    #set([x for x in ql if l.count(x) = 1])

    ret_list = list(set(ql))

    return ret_list


def get_cols(q):
    q=q.replace(" ","")
    col_text = q[q.find("(")+1:q.find(")")]
    return col_text.split(',')

###This function returns needed cols in the body of query 'q_c' ,
#  by looking into columns passed in head and conditions
###
def find_needed_cols(q_c,h_c,cnd):
    #create a list that will function as a map of needed cols
    l = ['']*len(q_c)
    q_c = [ '' if (x=='_')or(x==' ') else x for x in q_c]
    for i in range(len(q_c)):
        if  (q_c[i]!='') and  ( (q_c[i] in h_c)or find_string_in_cond(cnd,q_c[i])) :

                l[i]=q_c[i]

    return l


def find_string_in_cond(l_c,col):
    for e_c in l_c:
        if e_c.find(col)>=0:
            return True

    return False



def print_qpe_dict_list(dl):
    for dt in dl:
        print((dt['result'])+'->')
        for tb in dt['table']:
            print("\t%s"%tb)
        if dt['condition']!=None:
            print("condition:"),
            print(dt['condition'])
        if dt['limit']!=None:
            print("limit:"),
            print(dt['limit'])


def print_available_views():
    for each_view in gsm:
        if gsm[each_view].type=='in_global_view':
            print (gsm[each_view].datalog_string())


#print_available_views()

#result=execute('ans(cid,asin,nodeid)->Cust_Prod(cid,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,asin,nodeid),CategoryLevel_view(nodeid, level_1, level_2, level_3, level_4, level_5)',condition=["cid='102019'"],debug=True)



#print(result)