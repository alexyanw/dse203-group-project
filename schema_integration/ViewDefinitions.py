
import re
import difflib
import sys
import pickle
import random

#Dictionary for global schema mappings
gsm={}

class view:
    def __init__(self,name):
        self.name = name
        self.mapped_to = []  # A list of children view objects
        self.cols = []
        self.cond ={}
        self.col_map = []
        self.type ='in_global_view'

    def get_name(self):
        return self.name

    def unwrap(self,needed_cols=None):
        x=list()
        if len(self.mapped_to)>0:
            child_index=0
            for each_child in self.mapped_to:
                #From the needed_cols and this object's col_map , trying to propogate
                #the needed cols to child_needed_cols
                child_needed_cols = ['_']*len(each_child.cols)
                for e_t in self.col_map:
                    if needed_cols[e_t[0]]!='' and e_t[1]==child_index:
                        child_needed_cols[e_t[2]]=needed_cols[e_t[0]]
                #child_needed_cols should be checked if it has the necessary join columns enumerated
                if child_index in self.join_col_dict:
                    for e_d in self.join_col_dict[child_index]:
                        for c_col_idx in range(len(child_needed_cols)):
                            if child_needed_cols[c_col_idx]=='_' and (c_col_idx ==e_d):
                                child_needed_cols[c_col_idx]=self.join_col_dict[child_index][c_col_idx]
                x = x+each_child.unwrap(child_needed_cols)
                child_index+=1
            return x[:]
        else:
            return (self.datalog_string(needed_cols))

    def datalog_string(self,needed_cols=None):
        if needed_cols!=None:
            col_string=','.join(needed_cols)
        elif len(self.cols)>0:
            col_string = ','.join(self.cols)
        ret = "%s(%s)"%(self.name,col_string)
        return [ret]

    def get_child_dl(self,needed_cols):

        if len(self.cols) != len(needed_cols):
            #the no.of columns given in the query are not matching with whats defined.
            print ("Column number mismatch")
            sys.exit(2)

        #children datalogs obtained from unwrapping
        u_dl  = self.unwrap(needed_cols)
#        for e_t in self.col_map:
#            if needed_cols[e_t[0]]!='':
#                u_dl[e_t[1]]=substitute_col_name(vw=u_dl[e_t[1]],at=e_t[2],sub=needed_cols[e_t[0]])
        #print u_dl
        return u_dl


def substitute_col_name(vw,at,sub):
    cols=get_cols(vw)
    cols = [sub if i==at else '_' for i in range(len(cols))]
    ret = vw[:vw.find('(')+1]+','.join(cols)+')'
    return ret

def get_cols(q):
    q=q.replace(" ","")
    col_text = q[q.find("(")+1:q.find(")")]
    return col_text.split(',')

random_name_list=[]
def get_a_name():
    xy=0
    while (xy<10):
        ret = ''.join([random.choice('wxyz') for x in range(4)])
        if ret not in random_name_list:
            random_name_list.append(ret)
            return ret
        xy+=1

def schema_searcher(name):
    ret ={}
    schema_defs= open('source_descriptions.txt','r').read()
    schema_defs = schema_defs.lower()
#    fp=schema_defs.find("CREATE TABLE "+name)
    #  CREATE\s*TABLE\s*Customers\s*\((.*)\)\s;
    #  'CREATE\s*TABLE\s*Customers\s*\((.*)\)\s*?;'
    def_string = 'CREATE\s*TABLE\s*%s\s*\((.*?)\)\s*?;'%name
    match = re.search(def_string,schema_defs,re.DOTALL|re.IGNORECASE)
    if match:
        col_text=match.group(1)
        col_list = col_text.split(',')
        col_list = [x.strip().split(' ')[0] for x in col_list]
        ret['cols']=col_list[:]

        return ret
   #trying to match a view
    def_string = 'CREATE\s*VIEW\s*%s\s*AS\s*\((.*?)\);'%name
    match = re.search(def_string,schema_defs,re.DOTALL)
    if match:
        col_text = match.group(1)
        return col_text

    return None


def define_view(name,cols=None):
    view_object = view(name)
    gsm[view_object.get_name()]=view_object
    schema_def = schema_searcher(name)
    if schema_def is not None:
        gsm[view_object.get_name()].type='in_source'
        gsm[view_object.get_name()].cols=schema_def['cols'][:]
    elif cols != None:
        gsm[name].cols = cols[:]
    else:
        print("unknown view ", name)
        sys.exit(1)


def schema_matcher(head_cols,body_cols):

    col_map = []
    body_table_index = 0
    head_col_index=0
    for col in head_cols:
        for each_col_list in body_cols:
            sim_ratios = [ difflib.SequenceMatcher(None,col,x).ratio() for x in each_col_list]
            if max(sim_ratios)>0.9:
                col_map_tuple = (head_col_index,body_table_index,sim_ratios.index(max(sim_ratios)))
                col_map.append(col_map_tuple)
            body_table_index+=1
        head_col_index+=1
        body_table_index=0

    #print col_map
    return col_map

def join_cols_matcher(body_col_list):

    join_dict=dict()
    for i in range(len(body_col_list)):
        cur_col_list = body_col_list[i]
        j=i+1
        remaining_col_list =  body_col_list[j:]
        for col_idx in range(len(cur_col_list)):
            col = cur_col_list[col_idx]
            for k in range(len(remaining_col_list)):
                sim_ratios = [ difflib.SequenceMatcher(None,col,x).ratio() for x in remaining_col_list[k]]
                if max(sim_ratios)>0.9:
                    if i not in join_dict:
                        join_dict[i]=dict()
                    if col_idx not in join_dict[i]:
                        nm = get_a_name()
                        join_dict[i][col_idx]=nm
                    else:
                        nm=join_dict[i][col_idx]

                    if (j+k) not in join_dict:
                        join_dict[(j+k)] = dict()
                    idx = sim_ratios.index(max(sim_ratios))
                    if idx not in join_dict[(j+k)]:
                        join_dict[(j+k)][idx] = nm

    return join_dict

def define_mapping(map_str):
    #remove all white spaces
    map_str = map_str.replace(' ','')
    map_str = map_str.lower()
    #check for head
    [head,body]       = map_str.split('->')
    head_view  = head.split('(')[0]
    head_cols  = head[head.find('(')+1:head.find(')')].split(',')
    #print head , head_view
    #print head_cols
    if head_view not in gsm.keys():
        #if the cols are mentioned in head, then these cols need to be defined
        define_view(head_view,head_cols)
    #print gsm.keys()
    #print body
    #use pattern matching to obtain the body view list
    body_view_list=re.findall('^(.*?)\(',body)+re.findall('\),(.*?)\(',body)

    for each_view in body_view_list:
        if each_view not in gsm.keys():
            define_view(each_view)
    #print body_view_list
    #views for the mapping are defined at this point

    #Mapping body views to head view
    gsm[head_view].mapped_to= [gsm[x] for x in body_view_list ]

    #use pattern matching to obtain the column names used in body  --> this is not being used
    #body_cols_list=re.findall('\((.*?)\)',body)
    #body_cols_list = [x.split(',') for x in body_cols_list]
    #cum_col_length = sum([len(x) for x in body_cols_list],0)
    #body_cols_list = [col for each_list in body_cols_list for col in each_list ]

    #get the column names from mapped_to objects
    body_cols_list = [each_view.cols[:] for each_view in gsm[head_view].mapped_to ]


    #Schema Matching
    gsm[head_view].col_map = schema_matcher(head_cols,body_cols_list)
    gsm[head_view].join_col_dict = join_cols_matcher(body_cols_list)


    #print gsm[head_view].mapped_to

    #print "DONE."




define_mapping("Cust_Prod(CustomerId,ProductId,Asin)->                                                        \
               postgres.Customers(CustomerId,HouseholdId,Gender,FirstName),                                       \
               postgres.Orders(OrderId,CustomerId,CampaignId,OrderDate,City,State,ZipCode,PaymentType,TotalPrice, \
                        NumOrderLines,NumUnits),                                                         \
               postgres.OrderLines(OrderLineId,OrderId,ProductId,ShipDate,BillDate,UnitPrice,NumUnits,TotalPrice),\
               postgres.Products(ProductId,Name,GroupCode,GroupName,IsInStock,FullPrice,Asin)")


define_mapping("Global_Seasonal_View(ProductId,spring,summer,fall,winter,FullPrice,IsInStock)->          \
               postgres.Products(IsInStock,FullPrice),                                                            \
               postgres.seasonal_percentages(ProductId,spring,summer,fall,winter)")


define_mapping("Global_Seasonal_View_cust(ProductId,spring,summer,fall,winter,FullPrice,IsInStock)->      \
               Global_Seasonal_View(IsInStock,FullPrice),                                                 \
               postgres.Customers()")

pickle.dump( gsm, open( "global_schema_mappings.pkl", "w" ) )
