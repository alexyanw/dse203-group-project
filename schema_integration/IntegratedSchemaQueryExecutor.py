import pickle
from ViewDefinitions import view
import re
import sys


gsm = pickle.load(open('global_schema_mappings.pkl','r'))

#for key in gsm:
#    print gsm[key].get_name()



def execute(query,cond=None,limit=None):

    qpe_dict_list=[]
    qpe_dl=dict()

    try:
        [head,body] = query.split('->')
    except :
        print ("Unexpected format. Please stick to this format \nans(x,y)->viewname(x,_,y_)")
        sys.exit(1)
    #qpe_dl 'qpe_datalog' is a dictionary as expected by qpe team
    qpe_dl['result']=head
    head_cols = get_cols(head)

    ptrn = '^(\S*?)\('        #pattern to find the view in body
    match = re.search(ptrn,body)
    if match:
        gv = match.group(1)
        gv_cols = get_cols(body)
        l = find_needed_cols(gv_cols,head_cols,cond)
        dl = gsm[gv].get_child_dl(needed_cols=l)
        #dl = gsm[gv].unwrap()
        #print dl
        qpe_dl['table'] = dl
    else :
        print("Unable to find body view.Please stick to this format. \nans(x,y)->viewname(x,_,y_)")

    qpe_dl['condition'] =cond
    qpe_dl['limit']=limit


    #return(engine.queryDatalog(qpe_dict))
    return qpe_dl



    #qpe_dl['condition'] =


def get_cols(q):
    q=q.replace(" ","")
    col_text = q[q.find("(")+1:q.find(")")]
    return col_text.split(',')


def find_needed_cols(q_c,h_c,cnd):
    #create a list that will function as a map of needed cols
    l = ['']*len(q_c)
    q_c = [ '' if (x=='_')or(x==' ') else x for x in q_c]
    for i in range(len(q_c)):
        if  (q_c[i]!='') and  ( (q_c[i] in h_c)or(cnd.find(q_c[i]))>0 ) :
                l[i]=q_c[i]

    return l

def print_available_views():
    for each_view in gsm:
        if gsm[each_view].type=='in_global_view':
            print (gsm[each_view].datalog_string())


#print_available_views()