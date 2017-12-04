import pickle
from ViewDefinitions import view
import re
import sys


gsm = pickle.load(open('global_schema_mappings.pkl','r'))

#for key in gsm:
#    print gsm[key].get_name()



def execute(query,condition=None,limit=None,debug=False,just_debug=False):

    qpe_dict_list=[]
    qpe_dl=dict()

    try:
        query = query.lower()
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
        l = find_needed_cols(gv_cols,head_cols,condition)
        dl = gsm[gv].get_child_dl(needed_cols=l)
        #dl = gsm[gv].unwrap()
        #print dl
        qpe_dl['table'] = dl
    else :
        print("Unable to find body view.Please stick to this format. \nans(x,y)->viewname(x,_,y_)")

    qpe_dl['condition'] =condition
    qpe_dl['limit']=limit


    qpe_dict_list=[qpe_dl]
    if debug==True:
        print_qpe_dict_list(qpe_dict_list)



    #return(engine.queryDatalog(qpe_dict))
    return qpe_dl



    #qpe_dl['condition'] =


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

#result=execute('ans(asin,pid)->Cust_Prod(cid, pid ,asin)',condition=["cid='102019'"],debug=True)



#print(result)