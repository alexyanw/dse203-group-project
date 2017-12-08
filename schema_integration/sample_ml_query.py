import IntegratedSchemaQueryExecutor as ISQE



#query to list all the available views:
ISQE.print_available_views()

# "q1 from ML Team"
#result=ISQE.execute('ans(pid)->Global_Seasonal_View(pid,sp,_,_,_,_,_)','sp>50')

#for x in result:
#    print(x)
#    if result[x] is not None:
#        print("  ",result[x])
#    else:
#        print ("  ")

#print "q2"
#ISQE.execute('ans(pid)->Global_Seasonal_View_cust(pid,sp,_,_,_,_,_)','sp>50')
#result = ISQE.execute('ans(asin)->cust_prod(cid,householdid,gender,firstname,orderid,campaignid,orderdate,city,state,zipcode,paymenttype,order_totalprice,numorderlines,order_numunits,orderlineid,productid,shipdate,billdate,unitprice,ol_numunits,ol_totalprice,isinstock,fullprice,asin,nodeid)',condition=['cid=61893'], debug=True)
#result = ISQE.execute('ans(asin)->cust_prod(cid,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,asin,_)',condition=['cid=61893'], debug=True)

#result = ISQE.execute('ans(rid,summary)->reviewFeatures_view(rid, asin, rating, votesforreview, outof, reviewercount, \
 #               bookreviewcount, age, nodeid, reviewtext, summary)',condition=["rid='A05059033JWCACJUK57F2'"], debug=True)

#result = ISQE.execute('Ans(asin)->Cust_Prod(customerid,householdid,gender,firstname,orderid,campaignid,orderdate,city,state,zipcode,paymenttype,order_totalprice,numorderlines,order_numunits,orderlineid,productid,shipdate,billdate,unitprice,ol_numunits,ol_totalprice,isinstock,fullprice,asin,nodeid)', ['customerid=68099']);

# result = ISQE.execute('ans(asin)->cust_prod(cid,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,asin,_)',['cid=68099'])

# result = ISQE.execute('ans(nodeid, cat)->CategoryFlat_view(nodeid,cat)', ["cat = 'Education;Children & Teens'"], debug=True)


#result = ISQE.execute('ans(asin, cat)->cooccurrence_view(asin, _, _ , _ , _ , _, _, _, _, cat, _, _, _, _, _, _)', ["cat = 247"], debug=True)

#result = ISQE.execute('ans(asin, lvl1)->cooccurrence_view(asin, _, _ , _ , _ , _, _, _, _, lvl1, _ , _, _, _, _, _)', ["lvl1 = 247"], debug=True)

#result = ISQE.execute('ans(asin, lvl1)->Content_based_view(asin, rating, spring, summer, winter , fall, fullprice, isinstock , lvl1, lvl2 , lvl3, lvl4, lvl5)', ["lvl1 = 247"], debug=True)


#print(result)