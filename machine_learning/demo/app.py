from flask import Flask, flash, redirect, render_template, request, session, abort, jsonify, url_for, Response
import requests
import json
import numpy as np
from datetime import datetime
app = Flask(__name__)

########################
# init start
########################

# What mapping represent general category
NUM_GENERAL_CATEGORY = 0

# Number of recommendaions to provide in testrec page
NUM_RECOMMENDATIONS = 10

# Number of recommendaions to randomize from in content filtering
NUM_RECOMMENDATIONS_C = 40

# Ratio that identifies popularity for seasons
RATIO_POPULAR_SEASON = 0.4

# load in dictionary with encoded structure
dict_hstruct = np.load('../data/derived/dict_dir.npy', encoding = 'latin1').item()

# load in category name encodings
list_cat_names = []
with open('../data/encode_mappings/cats.txt') as f:
    list_cat_names = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
list_cat_names = [x.strip() for x in list_cat_names]

# load in asin names
list_asin_names = [x.decode('utf-8') for x in list(np.load('../data/extracts/asin.npy', encoding = 'latin1'))]

# load in customer x purchases matrix
matrix_cxp = np.load('../data/extracts/cust_item_matrix.npy', encoding = 'latin1')

# load in customer x demographics matrix
matrix_cxd = np.load('../data/extracts/demo_matrix.npy', encoding = 'latin1')

# load in depth x products x category lvls matrix
matrix_dxpxl = np.load('../data/extracts/categories_indexed.npy', encoding = 'latin1')

# load in general co-occurrence matrix
matrix_ccm_g = np.load('../data/derived/ccm_general.npy', encoding = 'latin1')

# load in seasons / price / instock product matrix
matrix_season_price_instock = np.load('../data/extracts/season_price_instock_indexed.npy', encoding = 'latin1')

# load in content rating array
matrix_content = np.load('../data/derived/rating_indexed.npy', encoding = 'latin1')

# load in customer ids
list_cust_ids = [int(x) for x in list(np.load('../data/extracts/custids.npy', encoding = 'latin1'))]


########################
# general functions
########################

def header_link(list_cat,custid,arg_seasonlist,arg_price):
	#base_link = "/testrec"
	base_link = build_general("/testrec",custid,arg_seasonlist,arg_price)
	list_link = []
	#list_link.append(base_link + "?custid=" + str(custid) + "&list_categories=0")
	list_link.append(base_link + "&list_categories=0")

	# if not just home link
	if list_cat[0] != NUM_GENERAL_CATEGORY:
		for i in range(len(list_cat)):
			#temp_link = base_link + "?custid=" + str(custid)
			temp_link = base_link
			for j,cat in enumerate(list_cat[:i+1]):
				temp_link += "&list_categories=" + str(cat)
			list_link.append(temp_link)				

	list_name = []
	list_name.append("Home")
	for i, cat in enumerate(list_cat):
		list_name.append(str(list_cat_names[list_cat[i]]))

	string_link = "<a href=\"" + list_link[0] + "\">" + list_name[0] + "</a>"
	if list_cat[0] != NUM_GENERAL_CATEGORY:
		for i in range(len(list_cat)):
			string_link += " > " + "<a href=\"" + list_link[i+1] + "\">" + list_name[i+1] + "</a>"
	return string_link

def sidebar_links(list_cat,custid,arg_seasonlist,arg_price):
	list_side_catid = []
	string_link = "<p>Categories</p>"

	# if general then just look up home
	if list_cat[0] == NUM_GENERAL_CATEGORY:
		list_side_catid = list(dict_hstruct.keys())
	# if max level then stop
	else:
		dict_ref = dict_hstruct
		for x in range(len(list_cat)):
			if list_cat[x] in dict_ref:
				dict_ref = dict_ref[list_cat[x]]
			else:
				dict_ref = {}
				break
		list_side_catid = list(dict_ref.keys())

	if len(list_side_catid) > 0:
		#base_link = "/testrec" + "?custid=" + str(custid)
		base_link = build_general("/testrec",custid,arg_seasonlist,arg_price)
		list_link = []

		if list_cat[0] != NUM_GENERAL_CATEGORY:
			for j,cat in enumerate(list_cat):
				base_link += "&list_categories=" + str(cat)

		for new_cat in list_side_catid:
				list_link.append(base_link + "&list_categories=" + str(new_cat))

		# Find real names for categories
		list_name = []
		for i, cat in enumerate(list_side_catid):
			list_name.append(str(list_cat_names[list_side_catid[i]]))

		for i in range(len(list_side_catid)):
			string_link += "<a href=\"" + list_link[i] + "\">" + list_name[i] + "</a><br>"
	else:
		string_link += ""

	## 
	string_link += "<hr><p>Popular during Season</p>"
	list_season_names = ["Any","Spring","Summer","Fall","Winter"]
	for sn in list_season_names:

		if sn == "Any":
			base_link = build_general("/testrec",custid,[],arg_price)
		elif sn in arg_seasonlist:
			base_link = build_general("/testrec",custid,list(set(arg_seasonlist) - set([sn])),arg_price)
		else:
			base_link = build_general("/testrec",custid,arg_seasonlist + [sn],arg_price)

		if list_cat[0] != NUM_GENERAL_CATEGORY:
			for j,cat in enumerate(list_cat):
				base_link += "&list_categories=" + str(cat)
		else:
			base_link += "&list_categories=0"

		string_link += "<a href=\"" + base_link + "\">" + sn + "</a><br>"

	## 
	string_link += "<hr><p>Max Price</p>"
	list_price_points = [0,8,15,20,40]
	for pp in list_price_points:

		base_link = build_general("/testrec",custid,arg_seasonlist,pp)

		if list_cat[0] != NUM_GENERAL_CATEGORY:
			for j,cat in enumerate(list_cat):
				base_link += "&list_categories=" + str(cat)
		else:
			base_link += "&list_categories=0"

		if pp == 0:
			string_link += "<a href=\"" + base_link + "\">" + "Any" + "</a><br>"
		else:
			string_link += "<a href=\"" + base_link + "\">" + "< " + str(pp) + "</a><br>"

	return string_link

def build_general(link,custid,arg_seasonlist,arg_price):
	str_gener = link + "?custid=" + str(custid)
	for s in arg_seasonlist:
		str_gener += "&list_seasons=" + str(s)
	if arg_price != 0:
		str_gener += "&max_price=" + str(arg_price)
	return str_gener

def get_cat_prod(cat_list):
	cat_array = np.ones(matrix_dxpxl.shape[1])
	if cat_list[0] != 0:
		cat_array = np.zeros(matrix_dxpxl.shape[1])
		for lvl in range(matrix_dxpxl.shape[0]):
			sub_cat_array = np.arange(matrix_dxpxl.shape[1])
			sub_dxpxl = matrix_dxpxl[lvl]
			for l in range(len(cat_list)):
				sub_cat_array = np.intersect1d(np.where(sub_dxpxl[:,l] == cat_list[l])[0],sub_cat_array)
			cat_array[sub_cat_array] = 1
	return cat_array

def get_idx_from_asin(asin_list):
	list_idx = []
	for asin in asin_list:
		try:
			list_idx.append(list_asin_names.index(asin))
		except ValueError:
			pass
	return list_idx

def get_idx_from_custid(custid_int):
	int_idx = 0
	try:
		int_idx = list_cust_ids.index(custid_int)
	except ValueError:
		pass
	return int_idx


########################
# mock rest queries
########################

@app.route('/get_demoid', methods=['GET'])
def get_demoid():
	# lookup table customerid input
	arg_custid = int(request.args.get('custid'))
	array_demo = np.zeros(matrix_cxd.shape[1])
	if arg_custid in list_cust_ids:
		array_demo = matrix_cxd[get_idx_from_custid(arg_custid)]
	return Response(json.dumps({'demographic_region': int(array_demo[0]), 'demographic_gender': int(array_demo[1])}),  mimetype='application/json')

@app.route('/get_purchases', methods=['GET'])
def get_purchases():
	# lookup table customerid input
	arg_custid = int(request.args.get('custid'))
	list_pur = []
	if arg_custid in list_cust_ids:
		list_temp = list(np.where(matrix_cxp[get_idx_from_custid(arg_custid)] > 0)[0])
		for l in list_temp:
			list_pur.append(list_asin_names[l])
	return Response(json.dumps({'purchases': list_pur}),  mimetype='application/json')

@app.route('/get_collabrec', methods=['GET'])
def get_collabrec():
	# read in variables
	arg_demographic_region = int(request.args.get('demographic_region'))
	arg_demographic_gender = int(request.args.get('demographic_gender'))
	arg_cat_list = list(map(int, request.args.getlist('list_categories')))
	arg_purchase_list = request.args.getlist('list_purchases')
	arg_num_rec = int(request.args.get('num_rec'))
	arg_season_list = request.args.getlist('list_seasons')
	arg_price = int(request.args.get('max_price',0))

	# Select corrrect matrix based on demographics, currently using general
	# add up all co-occurrence rows
	rowsum = np.zeros(matrix_ccm_g.shape[0])
	for p in get_idx_from_asin(arg_purchase_list):
		rowsum += matrix_ccm_g[p,:]

	# Remove perviously purchased
	rowsum[get_idx_from_asin(arg_purchase_list)] = 0

	# filter by season
	list_season_names = ["Spring","Summer","Fall","Winter"]
	season_filter_list = []
	for i, sea in enumerate(list_season_names):
		if sea in arg_season_list:
			season_filter_list.extend(np.where(matrix_season_price_instock[:,i] < RATIO_POPULAR_SEASON * 100)[0])
		season_filter_list = list(set(season_filter_list))
	rowsum[season_filter_list] = 0

	# filter by price
	if arg_price > 0:
		price_filter_list = np.where(matrix_season_price_instock[:,4] >= arg_price)[0]
		rowsum[price_filter_list] = 0

	# filter by instock
	instock_filter_list = np.where(matrix_season_price_instock[:,5] == 0)[0]
	rowsum[instock_filter_list] = 0

	# filter by category
	rowsum = rowsum * get_cat_prod(arg_cat_list)

	# Select top indexes
	indices = np.nonzero(rowsum)[0]
	toprec = indices[np.argsort(rowsum[indices])][-1 * arg_num_rec:][::-1]

	list_json = []
	for tr in toprec:
		list_json.append({'asin': list_asin_names[tr], 'metric': rowsum[tr]})

	#return jsonify(recommendations=list(toprec))
	return Response(json.dumps(list_json),  mimetype='application/json')

@app.route('/get_contentrec', methods=['GET'])
def get_contentrec():
	# read in variables
	arg_cat_list = list(map(int, request.args.getlist('list_categories')))
	arg_purchase_list = request.args.getlist('list_purchases')
	arg_num_rec = int(request.args.get('num_rec'))
	arg_season_list = request.args.getlist('list_seasons')
	arg_price = int(request.args.get('max_price',0))

	# content matrix
	rowsum = np.copy(matrix_content)
	rowsum[rowsum == 0] = 0.01

	# Remove perviously purchased
	rowsum[get_idx_from_asin(arg_purchase_list)] = 0

	# filter by season
	list_season_names = ["Spring","Summer","Fall","Winter"]
	season_filter_list = []
	for i, sea in enumerate(list_season_names):
		if sea in arg_season_list:
			season_filter_list.extend(np.where(matrix_season_price_instock[:,i] < RATIO_POPULAR_SEASON * 100)[0])
		season_filter_list = list(set(season_filter_list))
	rowsum[season_filter_list] = 0

	# filter by price
	if arg_price > 0:
		price_filter_list = np.where(matrix_season_price_instock[:,4] >= arg_price)[0]
		rowsum[price_filter_list] = 0

	# filter by instock
	instock_filter_list = np.where(matrix_season_price_instock[:,5] == 0)[0]
	rowsum[instock_filter_list] = 0

	# filter by category
	rowsum = rowsum * get_cat_prod(arg_cat_list)

	# Select top indexes
	indices = np.nonzero(rowsum)[0]
	randrec = indices[np.argsort(rowsum[indices])][-1 * NUM_RECOMMENDATIONS_C:][::-1]
	randindices = np.random.permutation(randrec)[:arg_num_rec]
	toprec = randindices[np.argsort(rowsum[randindices])][::-1]

	list_json = []
	for tr in toprec:
		list_json.append({'asin': list_asin_names[tr], 'metric': rowsum[tr]})

	#return jsonify(recommendations=list(toprec))
	return Response(json.dumps(list_json),  mimetype='application/json')

@app.route('/get_hybridrec', methods=['GET'])
def get_hybridrec():
	arg_demographic_region = int(request.args.get('demographic_region'))
	arg_demographic_gender = int(request.args.get('demographic_gender'))
	arg_cat_list = list(map(int, request.args.getlist('list_categories')))
	arg_purchase_list = request.args.getlist('list_purchases')
	arg_num_rec = int(request.args.get('num_rec'))
	arg_season_list = request.args.getlist('list_seasons')
	arg_price = int(request.args.get('max_price',0))

	# Initialize empty recommendations
	get_collabrec = []
	get_contentrec = []
	get_hybridrec = []

	# Collaborative Filtering
	# if no purchases don't use collaborative
	if len(arg_purchase_list) > 0:
		#Build query string for collaborative recommendation
		str_collabrec = request.url_root + 'get_collabrec?' + "demographic_region=" + str(arg_demographic_region) + "&demographic_gender=" + str(arg_demographic_gender) + "&num_rec=" + str(arg_num_rec)
		for p in arg_purchase_list:
			str_collabrec += "&list_purchases=" + str(p)
		for c in arg_cat_list:
			str_collabrec += "&list_categories=" + str(c)
		for s in arg_season_list:
			str_collabrec += "&list_seasons=" + str(s)
		if arg_price > 0:
			 str_collabrec += "&max_price=" + str(arg_price)
		get_collabrec = requests.get(str_collabrec).json()


	# Content Filtering
	# Build query string for content recommendation
	str_contentrec = request.url_root + 'get_contentrec?' + "num_rec=" + str(arg_num_rec)
	for p in arg_purchase_list:
		str_contentrec += "&list_purchases=" + str(p)
	for c in arg_cat_list:
		str_contentrec += "&list_categories=" + str(c)
	for s in arg_season_list:
		str_contentrec += "&list_seasons=" + str(s)
	if arg_price > 0:
		 str_contentrec += "&max_price=" + str(arg_price)
	get_contentrec = requests.get(str_contentrec).json()

	# Hybrid Combiner
	if len(arg_purchase_list) != 0:
		for r in get_collabrec:
			get_hybridrec.append(r['asin'])
	for r in get_contentrec:
		if r['asin'] not in get_hybridrec:
			get_hybridrec.append(r['asin']) 
		if len(get_hybridrec) >= arg_num_rec:
			break

	list_json = []
	for hr in get_hybridrec:
		list_json.append({'asin': hr.encode().decode()})

	return Response(json.dumps(list_json),  mimetype='application/json')

########################
# webpage endpoints
########################

@app.route("/")
def root():
	return redirect(url_for('main'))

@app.route("/main")
def main():
	return render_template('main.html')

# Generate collab, content, and combiner recommendation

@app.route("/testrec")
def testrec():
	# Make sure input is integer if not default to -1 (new customer)
	try:
		arg_custid = int(request.args.get('custid'))
	except:
		arg_custid = -1
	arg_catidlist = list(map(int, request.args.getlist('list_categories')))

	arg_seasonlist = request.args.getlist('list_seasons')

	arg_price = int(request.args.get('max_price',0))

	# Page link creation
	header_ahref = header_link(arg_catidlist, arg_custid, arg_seasonlist, arg_price)
	sidebar_ahref = sidebar_links(arg_catidlist, arg_custid, arg_seasonlist, arg_price)

	# Get demographic info
	get_demoid = requests.get(request.url_root+ 'get_demoid?custid=' + str(arg_custid)).json()

	# Get purchase info
	get_purchases = requests.get(request.url_root+ 'get_purchases?custid=' + str(arg_custid)).json()

	# Initialize empty recommendations
	get_collabrec = []
	get_contentrec = []
	get_hybridrec = []

	# Initialize runtime
	time_collabrec = 0
	time_contentrec = 0
	time_hybridrec = 0
	time_current = 0

	# Collaborative Filtering
	# if no purchases don't use collaborative
	if len(get_purchases['purchases']) > 0:
		#Build query string for collaborative recommendation
		str_collabrec = request.url_root + 'get_collabrec?' + "demographic_region=" + str(get_demoid['demographic_region']) + "&demographic_gender=" + str(get_demoid['demographic_gender']) + "&num_rec=" + str(NUM_RECOMMENDATIONS)
		for p in get_purchases['purchases']:
			str_collabrec += "&list_purchases=" + str(p)
		for c in arg_catidlist:
			str_collabrec += "&list_categories=" + str(c)
		for s in arg_seasonlist:
			str_collabrec += "&list_seasons=" + str(s)
		if arg_price > 0:
			 str_collabrec += "&max_price=" + str(arg_price)
		time_current = datetime.now()
		get_collabrec = requests.get(str_collabrec).json()
		time_collabrec = str((datetime.now() - time_current).total_seconds() * 1000) + " ms"

	# Content Filtering
	# Build query string for content recommendation
	str_contentrec = request.url_root + 'get_contentrec?' + "num_rec=" + str(NUM_RECOMMENDATIONS)
	for p in get_purchases['purchases']:
		str_contentrec += "&list_purchases=" + str(p)
	for c in arg_catidlist:
		str_contentrec += "&list_categories=" + str(c)
	for s in arg_seasonlist:
		str_contentrec += "&list_seasons=" + str(s)
	if arg_price > 0:
		 str_contentrec += "&max_price=" + str(arg_price)
	time_current = datetime.now()
	get_contentrec = requests.get(str_contentrec).json()
	time_contentrec = str((datetime.now() - time_current).total_seconds() * 1000) + " ms"

	# Hybrid Combiner
	# Build query string for hybrid recommendation
	str_hybridrec = request.url_root + 'get_hybridrec?' + "demographic_region=" + str(get_demoid['demographic_region']) + "&demographic_gender=" + str(get_demoid['demographic_gender']) + "&num_rec=" + str(NUM_RECOMMENDATIONS)
	for p in get_purchases['purchases']:
		str_hybridrec += "&list_purchases=" + str(p)
	for c in arg_catidlist:
		str_hybridrec += "&list_categories=" + str(c)
	for s in arg_seasonlist:
		str_hybridrec += "&list_seasons=" + str(s)
	if arg_price > 0:
		 str_hybridrec += "&max_price=" + str(arg_price)
	time_current = datetime.now()
	get_hybridrec = requests.get(str_hybridrec).json()
	time_hybridrec = str((datetime.now() - time_current).total_seconds() * 1000) + " ms"

	# Books in category
	get_book_list = []
	for b in list(np.where(get_cat_prod(arg_catidlist) > 0)[0]):
		get_book_list.append(list_asin_names[b])
	get_book_num = len(get_book_list)

	return render_template('testrec.html',**locals())

# Rest service tester

@app.route("/testrest")
def testrest():
	r1 = requests.get(request.url_root+ 'get_demoid?custid=1')
	r2 = requests.get(request.url_root+ 'get_purchases?custid=1')
	return r2.text


if __name__ == "__main__":
	app.run(threaded=True)