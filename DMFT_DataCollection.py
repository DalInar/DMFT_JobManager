import os
import subprocess as sp
import argparse
import json

#Soft equivalence for dictionaries, returns true iff for all entires in 'a', 'b' has at least the same entries. So 'b' could have additional entries as well!
def equiv_dicts(a,b):
	for field in a.keys():
		if(not field in b.keys()):
			#print "Dict b does not contain the field "+str(field)+"!"
			return False
		elif(not b[field] == a[field]):
			#print "Entires for "+str(field)+" do not agree!"
			#print "a["+str(field)+"] = "+str(a[field])+",\tb["+str(field)+"] = "+str(b[field])
			return False
	return True

#Scan through the parameter sets, compare to log, and find the directory containing each (or warn that it does not exist)
def locate_param_sets(params, param_log):
	print "Locating parameter sets"
	print "Number of JK sets: "+str(len(params))
	a = param_log.keys()[0]
	for i in range(0, len(params)):
		for x in params[i]:
			found=False
			for y in param_log.keys():
				if(equiv_dicts(x,param_log[y])):
					found=True
					x["Location"] = y
			if(not found):
				print "Failed to locate this parameter set:"
				print x
				return False
	return True
	

#Given the target params from the input json file, construct a list of dictionaries of parameters to seach for in the log
def get_param_sets(params):
	#param_sets will be a list of lists: each sublist will be a set of jobs for a particular jackknife iteration
	param_sets = []
	
	proto_set = {}
	ind_params = params["Independent"]
	for field in ind_params:
		proto_set[field] = ind_params[field]
	
	print "Independent parameters:"
	print proto_set
	
	var_params = params["Variable"]
	if(len(var_params.keys()) > 1):
		print "Error!  There should only be 1 variable parameter (x-axis parameter).  Received: "
		print var_params
		return []
		
	temp_sets = []
	var_param_name = params["Variable"].keys()[0]
	print "Variable parameter name: "+var_param_name
	for value in params["Variable"][var_param_name]:
		print value
		param_set = proto_set.copy()
		param_set[var_param_name] = value
		temp_sets.append(param_set)
		
	if(not len(params["Jackknife"].keys()) > 0):
		print "Not running jackknife analysis"
		param_sets.append(temp_sets)
	else:
		print "Running jackknife analysis on: "
		print params["Jackknife"]
		num_jk_vars = len(params["Jackknife"].keys())
		num_jk_sets = len(params["Jackknife"][params["Jackknife"].keys()[0]])
		print "Num JK Vars: "+str(num_jk_vars)
		print "Num JK Sets: "+str(num_jk_sets)
		
		#Create a new list of parameter sets for each Jackknife iteration
		for i in range(0, num_jk_sets):
			jk_set = []
			for pset in temp_sets:
				new_set = pset.copy()
				for field in params["Jackknife"].keys():
					new_set[field] = params["Jackknife"][field][i]
				jk_set.append(new_set)
			param_sets.append(jk_set)	
	
	return param_sets
	
def read_data(param_sets, target):
	start_location = os.getcwd()
	for jk_set in param_sets:
		for param_set in jk_set:
			target_dir = param_set["Location"]
			target_file = param_set["FILENAME"]
			target_element = param_set["ELEMENT"]
			f = open(target_dir+"/"+target_file)
			
			f.close()
	return [[]]
	
def exec_data(param_sets, target):
	return [[]]
	
def acquire_data(param_sets, target):
	method = target["TYPE"]
	if(method == "READ"):
		return read_data(param_sets, target)
	elif(method == "EXEC"):
		return exec_data(param_sets, target)
	else:
		print "Error! Unknown data acquisition type!"
		print target
		return [[]]

def main():
	print "I collect data!"
	#Remember starting location
	start_location = os.getcwd()
	print("Starting at: "+start_location)
	
	#Get the name of the json file containing the parameters
	parser = argparse.ArgumentParser(description='Creates a batch file and parameter files that will perform a series of calculations for the given parameters')
	parser.add_argument('input_file',help='JSON file containing the parameters')
	args = parser.parse_args()	
	
	#Open and read the json file
	in_filename = args.input_file
	print 'Reading parameter file: ',in_filename
	json_data = open(in_filename)
	params = json.load(json_data)	
	json_data.close()
	print params.keys()
	
	#Read in the parameter job from DMFT_JobManager, which tells us which parameter sets exist and where they are
	in_filename = "parameter_log.json"
	print 'Reading parameter log: ', in_filename
	json_data = open(in_filename)
	param_log = json.load(json_data)	
	json_data.close()
	
	#Get the target parameter sets
	param_sets = get_param_sets(params);
	
	#Scan through the parameter sets, compare to log, and find the directory containing each (or warn that it does not exist)
	if(not locate_param_sets(param_sets, param_log)):
		print "Error!  Could not locate all of the requested parameter sets!"
		return
	
	print "Found the parameter sets"
	#print param_sets
	
	#Read or calculate the requested data
	print "Acquiring data"
	for data_target in params["Data"].keys():
		print "Getting "+data_target
		data = acquire_data(param_sets, params["Data"][data_target])
		print data
	
	
	
	
if __name__ == "__main__":
	main()
