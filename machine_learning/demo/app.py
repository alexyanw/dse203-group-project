from flask import Flask, flash, redirect, render_template, request, session, abort, jsonify, url_for
import requests
import json
import numpy as np
app = Flask(__name__)

########################
# init start
########################

# What mapping represent general category
NUM_GENERAL_CATEGORY = 0
# Number of recommendaions to provide
NUM_RECOMMENDATIONS = 10

# load in dictionary with encoded structure
dict_hstruct = np.load('dict_dir.npy').item()

# load in category name encodings
list_cat_names = []
with open('cats.txt') as f:
    list_cat_names = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
list_cat_names = [x.strip() for x in list_cat_names] 

# load in customer x purchases matrix
matrix_cxp = np.load('cust_item_matrix.npy')

# load in customer x demographics matrix
matrix_cxd = np.load('demo_matrix.npy')

# load in depth x products x category lvls matrix
matrix_dxpxl = np.load('categories_indexed.npy')

# general co-occurrence matrix
matrix_ccm_g = np.load('ccm_general.npy')


########################
# general functions
########################

def header_link(list_cat,custid):
	base_link = "/testrec"
	list_link = []
	list_link.append(base_link + "?custid=" + str(custid) + "&list_categories=0")

	# if not just home link
	if list_cat[0] != NUM_GENERAL_CATEGORY:
		for i in range(len(list_cat)):
			temp_link = base_link + "?custid=" + str(custid)
			for j,cat in enumerate(list_cat[:i+1]):
				temp_link += "&list_categories=" + str(cat)
			list_link.append(temp_link)				

	list_name = []
	list_name.append("Home")
	for i, cat in enumerate(list_cat):
		list_name.append(str(list_cat_names[list_cat[i]]))

	string_link = "<a href=\"" + list_link[0] + "\">" + list_name[0] + "</a>"
	if list_cat[0] != NUM_GENERAL_CATEGORY:
		#print list_cat[0]
		for i in range(len(list_cat)):
			string_link += " > " + "<a href=\"" + list_link[i+1] + "\">" + list_name[i+1] + "</a>"
	return string_link

def sidebar_links(list_cat,custid):
	list_side_catid = []
	string_link = ""

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
		base_link = "/testrec" + "?custid=" + str(custid)
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

		#print list_side_catid
		for i in range(len(list_side_catid)):
			string_link += "<a href=\"" + list_link[i] + "\">" + list_name[i] + "</a><br>"
	else:
		string_link =""
	return string_link

########################
# mock rest queries
########################

@app.route('/get_demoid', methods=['GET'])
def get_demoid():
	# lookup table customerid input
	arg_custid = request.args.get('custid')
	array_demo = np.zeros(matrix_cxd.shape[1])
	if int(arg_custid) >= 0 and int(arg_custid)  < matrix_cxd.shape[0]:
		array_demo = matrix_cxd[int(arg_custid)]
	return jsonify(demographic_region=int(array_demo[0]), demographic_gender=int(array_demo[1]))

@app.route('/get_purchases', methods=['GET'])
def get_purchases():
	# lookup table customerid input
	arg_custid = request.args.get('custid')
	list_pur = []
	if int(arg_custid) >= 0 and int(arg_custid)  < matrix_cxd.shape[0]:
		list_pur = list(np.where(matrix_cxp[int(arg_custid)] > 0)[0])
	return jsonify(purchases=list_pur)

@app.route('/get_collabrec', methods=['GET'])
def get_collabrec():
	# read in variables
	arg_demographic_region = int(request.args.get('demographic_region'))
	arg_demographic_gender = int(request.args.get('demographic_gender'))
	arg_cat_list = list(map(int, request.args.getlist('list_categories')))
	arg_purchase_list = list(map(int, request.args.getlist('list_purchases')))
	arg_num_rec = int(request.args.get('num_rec'))
	# TODO read in seasonal and price filters

	# Select corrrect matrix based on demographics, currently using general
	# add up all co-occurrence rows
	rowsum = np.zeros(matrix_ccm_g.shape[0])
	for p in arg_purchase_list:
		rowsum += matrix_ccm_g[p,:]

	# Remove perviously purchased
	rowsum[arg_purchase_list] = 0

	# TODO filter by season, price, categories

	# Select top indexes
	indices = np.nonzero(rowsum)[0]
	toprec = indices[np.argsort(rowsum[indices])][-1 * arg_num_rec:][::-1]

	return jsonify(recommendations=list(toprec))

@app.route('/get_contentrec', methods=['GET'])
def get_contentec():
	# read in variables
	arg_cat_list = list(map(int, request.args.getlist('list_categories')))
	arg_purchase_list = list(map(int, request.args.getlist('list_purchases')))
	arg_num_rec = int(request.args.get('num_rec'))
	# TODO read in seasonal and price filters

	# Placeholder using co-occurrence instead of content
	# add up all co-occurrence rows
	rowsum = np.sum(matrix_ccm_g,axis=0)

	# Remove perviously purchased
	rowsum[arg_purchase_list] = 0

	# TODO filter by season, price, categories

	# Select top indexes
	indices = np.nonzero(rowsum)[0]
	toprec = indices[np.argsort(rowsum[indices])][-1 * arg_num_rec:][::-1]

	return jsonify(recommendations=list(toprec))

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
	header_ahref = header_link(arg_catidlist, arg_custid)
	sidebar_ahref = sidebar_links(arg_catidlist, arg_custid)

	# Get demographic info
	get_demoid = requests.get(request.url_root+ 'get_demoid?custid=' + str(arg_custid)).text

	# Get purchase info
	get_purchases = requests.get(request.url_root+ 'get_purchases?custid=' + str(arg_custid)).text

	# Collaborative Filtering
	# if no purchases don't use collaborative
	if len(json.loads(get_purchases)['purchases']) != 0:
		#Build query string for collaborative recommendation
		str_collabrec = request.url_root + 'get_collabrec?' + "&demographic_region=" + str(json.loads(get_demoid)['demographic_region']) + "&demographic_gender=" + str(json.loads(get_demoid)['demographic_gender']) + "&num_rec=" + str(NUM_RECOMMENDATIONS)
		for p in json.loads(get_purchases)['purchases']:
			str_collabrec += "&list_purchases=" + str(p)
		for c in arg_catidlist:
			str_collabrec += "&list_categories=" + str(c)
		# TODO add seasonal and price filters
		get_collabrec = requests.get(str_collabrec).text


	# Content Filtering
	# TODO write function
	# Build query string for content recommendation
	str_contentrec = request.url_root + 'get_contentrec?' + "&num_rec=" + str(NUM_RECOMMENDATIONS)
	for p in json.loads(get_purchases)['purchases']:
		str_contentrec += "&list_purchases=" + str(p)
	for c in arg_catidlist:
		str_contentrec += "&list_categories=" + str(c)
	# TODO add seasonal and price filters
	get_contentrec = requests.get(str_contentrec).text

	# Hybrid Combiner
	# TODO decide algorithm
	if len(json.loads(get_purchases)['purchases']) != 0:
		get_hybridrec = get_collabrec
		# TODO if not enough rec fill with content
	else:
		get_hybridrec = get_contentrec
		# TODO if not enough rec fill with collab

	#return render_template('testrec.html',cust_id=request.args.get('custid'),cat_id=request.args.get('catid'))
	return render_template('testrec.html',**locals())

# Rest service tester

@app.route("/testrest")
def testrest():
	r1 = requests.get(request.url_root+ 'get_demoid?custid=1')
	r2 = requests.get(request.url_root+ 'get_purchases?custid=1')
	return r2.text


if __name__ == "__main__":
	app.run(threaded=True)