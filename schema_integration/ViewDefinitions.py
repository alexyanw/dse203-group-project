
import re
import difflib
import sys
import pickle
import random
import sys
sys.path.append("..\\query_capability\\")

from hybrid_engine import HybridEngine

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



engine = HybridEngine(
        postgres={'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres',
                  'password': 'pavan007'},
        asterix={'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
        solr={'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

def define_virtual_view_in_source(query):
    engine.execute(query)


define_virtual_view_in_source("CREATE OR REPLACE VIEW reviewcount as \
(                                                                    \
   SELECT reviewid, count(*) as reviewercount                        \
   FROM reviews                                                      \
   GROUP BY reviewid);")

define_virtual_view_in_source("CREATE OR REPLACE VIEW bookcount as       \
(                                           \
   SELECT asin, count(*) as bookreviewcount \
   FROM reviews                             \
   GROUP BY asin                            );")

define_virtual_view_in_source("CREATE OR REPLACE VIEW reviewvotesage as                                                   \
(                                                                                          \
   SELECT reviewid,                                                                        \
          asin,                                                                            \
          cast(overall AS int) AS rating,                                                  \
          cast(trim(LEADING '[' FROM substring(helpful FROM 0 FOR position(',' IN          \
            helpful))) AS INT) AS votesforreview,                                          \
          cast(trim(TRAILING ']' FROM substring(helpful FROM position(',' IN               \
            helpful)+2)) AS INT) AS outof,                                                 \
          extract(day FROM CURRENT_DATE - reviewtime) as age,                              \
          reviewtext,                                                                      \
          summary                                                                          \
   FROM reviews r                                                                          \
);")

define_virtual_view_in_source("CREATE OR REPLACE VIEW seasonal_percentages as                                                                      \
(                                                                                                                   \
   SELECT productid,                                                                                                \
          round(100*sum(case when month >= 3 and month < 6 then numunits else 0                                     \
            end)/sum(numunits),2) as spring,                                                                        \
          round(100*sum(case when month >= 6 and month < 9 then numunits else 0                                     \
            end)/sum(numunits),2) as summer,                                                                        \
          round(100*sum(case when month >= 9 and month < 12 then numunits else 0                                    \
            end)/sum(numunits),2) as fall,                                                                          \
          round(100*sum(case when (month = 12 or month < 3) then numunits else 0                                    \
            end)/sum(numunits),2) as winter                                                                         \
   FROM (                                                                                                           \
           SELECT productid, EXTRACT(MONTH FROM orderdate) as month, case when                                      \
                  l.numunits = 0 then 0.00001 else l.numunits end as numunits                                       \
           FROM orders o, orderlines l, customers c                                                                 \
           WHERE o.orderid = l.orderid AND c.customerid = o.customerid                                              \
         ) as temp                                                                                                  \
   GROUP BY productid                                                                                               \
);")

define_virtual_view_in_source("CREATE OR REPLACE VIEW p_instock_fp_d as                                  \
(                                                                         \
   SELECT productid, Cast(Max(p.fullprice) AS DECIMAL) as fullprice_d,    \
          CASE                                                            \
            WHEN Max(p.isinstock) = 'Y' THEN 1                            \
            ELSE 0                                                        \
          END as isinstock_d, asin                                        \
   FROM products p                                                        \
   GROUP BY p.productid                                                   \
);                                                                        ")

define_virtual_view_in_source("CREATE OR REPLACE VIEW regions_map as                                            \
(                                                                                \
   SELECT c.customerid,                                                          \
       case when o.state='.' then 0                                              \
            when o.state='' then 0                                               \
            when o.state='RI' then 1                                             \
            when o.state='NH' then 1                                             \
            when o.state='VT' then 1                                             \
            when o.state='CT' then 1                                             \
            when o.state='SP' then 1                                             \
            when o.state='MA' then 1                                             \
            when o.state='ME' then 1                                             \
            when o.state='PA' then 2                                             \
            when o.state='NJ' then 2                                             \
            when o.state='NY' then 2                                             \
            when o.state='OH' then 3                                             \
            when o.state='WI' then 3                                             \
            when o.state='IN' then 3                                             \
            when o.state='IL' then 3                                             \
            when o.state='MI' then 3                                             \
            when o.state='SD' then 4                                             \
            when o.state='IA' then 4                                             \
            when o.state='KS' then 4                                             \
            when o.state='MO' then 4                                             \
            when o.state='ND' then 4                                             \
            when o.state='MN' then 4                                             \
            when o.state='NE' then 4                                             \
            when o.state='NC' then 5                                             \
            when o.state='VA' then 5                                             \
            when o.state='SC' then 5                                             \
            when o.state='DC' then 5                                             \
            when o.state='GA' then 5                                             \
            when o.state='DE' then 5                                             \
            when o.state='WV' then 5                                             \
            when o.state='MD' then 5                                             \
            when o.state='FL' then 5                                             \
            when o.state='MS' then 6                                             \
            when o.state='TN' then 6                                             \
            when o.state='KY' then 6                                             \
            when o.state='AL' then 6                                             \
            when o.state='OK' then 7                                             \
            when o.state='LA' then 7                                             \
            when o.state='TX' then 7                                             \
            when o.state='AR' then 7                                             \
            when o.state='WY' then 8                                             \
            when o.state='UT' then 8                                             \
            when o.state='AZ' then 8                                             \
            when o.state='NM' then 8                                             \
            when o.state='ID' then 8                                             \
            when o.state='NV' then 8                                             \
            when o.state='CO' then 8                                             \
            when o.state='MT' then 8                                             \
            when o.state='CA' then 9                                             \
            when o.state='WA' then 9                                             \
            when o.state='AK' then 9                                             \
            when o.state='OR' then 9                                             \
            when o.state='GU' then 9                                             \
            when o.state='HI' then 9                                             \
            when o.state='PR' then 10                                            \
            when o.state='US' then 10                                            \
            when o.state='VI' then 10                                            \
            when o.state='AA' then 11                                            \
            when o.state='AE' then 12                                            \
            when o.state='AP' then 13                                            \
            when o.state='AB' then 14                                            \
            when o.state='BC' then 15                                            \
            when o.state='MB' then 16                                            \
            when o.state='NB' then 17                                            \
            when o.state='NL' then 18                                            \
            when o.state='NF' then 18                                            \
            when o.state='NT' then 19                                            \
            when o.state='NS' then 20                                            \
            when o.state='ON' then 21                                            \
            when o.state='PE' then 22                                            \
            when o.state='PQ' then 23                                            \
            when o.state='QC' then 23                                            \
            when o.state='SK' then 24                                            \
            when o.state='UK' then 25                                            \
            when o.state='EN' then 26                                            \
            when o.state='QL' then 27                                            \
            when o.state='CN' then 28                                            \
            when o.state='CH' then 28                                            \
            when o.state='DF' then 29                                            \
            when o.state='FR' then 30                                            \
            when o.state='GD' then 31                                            \
            when o.state='BD' then 32                                            \
            when o.state='KM' then 33                                            \
            when o.state='KW' then 34                                            \
            when o.state='LC' then 35                                            \
            when o.state='MG' then 36                                            \
            when o.state='PC' then 37                                            \
            when o.state='SO' then 38                                            \
            when o.state='SR' then 39                                            \
            when o.state='VC' then 40                                            \
            when o.state='YU' then 41                                            \
            else 0                                                               \
       end as region,                                                            \
       case when c.gender='M' then 1                                             \
            when c.gender='F' then 2                                             \
            else 0                                                               \
       end as gender                                                             \
   FROM customers c, orders o                                                    \
   WHERE c.customerid = o.customerid                                             \
   order by c.customerid                                                         \
);                                                                               ")


# define_mapping("Cust_Prod(CustomerId,ProductId,Asin)->                                                        \
#                postgres.Customers(CustomerId,HouseholdId,Gender,FirstName),                                       \
#                postgres.Orders(OrderId,CustomerId,CampaignId,OrderDate,City,State,ZipCode,PaymentType,TotalPrice, \
#                         NumOrderLines,NumUnits),                                                         \
#                postgres.OrderLines(OrderLineId,OrderId,ProductId,ShipDate,BillDate,UnitPrice,NumUnits,TotalPrice),\
#                postgres.Products(ProductId,Name,GroupCode,GroupName,IsInStock,FullPrice,Asin)")

define_mapping("Cust_Prod(customerid, householdid, gender, firstname, orderid, campaignid, orderdate, city, state, zipcode, paymenttype, order_totalprice, numorderlines, order_numunits, orderlineid, productid, shipdate, billdate, unitprice, ol_numunits, ol_totalprice, isinstock, fullprice, asin, nodeid) ->                                                        \
               postgres.Customers(),                                       \
               postgres.Orders(),                                                         \
               postgres.OrderLines(),\
               postgres.Products()")


define_mapping("Global_Seasonal_View(ProductId,spring,summer,fall,winter,FullPrice,IsInStock)->          \
               postgres.Products(IsInStock,FullPrice),                                                            \
               postgres.seasonal_percentages(ProductId,spring,summer,fall,winter)")


define_mapping("Global_Seasonal_View_cust(ProductId,spring,summer,fall,winter,FullPrice,IsInStock)->      \
               Global_Seasonal_View(IsInStock,FullPrice),                                                 \
               postgres.Customers()")

define_mapping("reviewFeatures_view(reviewid, asin, rating, votesforreview, outof, reviewercount, \
                bookreviewcount, age, nodeid, reviewtext, summary) -> postgres.reviewvotesage(), postgres.reviewcount(),\
                postgres.bookcount(), postgres.products()")

define_mapping("regions_map_view(customerid, region, gender) -> postgres.regions_map()")

pickle.dump( gsm, open( "global_schema_mappings.pkl", "wb" ) )
