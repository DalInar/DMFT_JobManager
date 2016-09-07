from contextlib import contextmanager
import os
import subprocess as sp
import argparse
import json
import math
import h5py

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

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
					#x = param_log[y].copy()
					#print param_log[y]
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
	
#Read the requested data from the provided file, at the indicated [line, pos] in param_set["ELEMENT"]
def read_data(param_sets, target, target_name):
	start_location = os.getcwd()
	data_vals=[]
	for jk_set in param_sets:
		jk_data = []
		for param_set in jk_set:
			target_dir = param_set["Location"]
			print target_dir
			target_file = target["FILENAME"]
			target_element = target["ELEMENT"]
			f = open(target_dir+"/"+target_file)
			line = f.readline()
			for i in range(0, target_element[0]-1):
				line = f.readline()
			line=line.split()
			line = [float(i) for i in line]
			data = line[target_element[1]]
			f.close()
			jk_data.append(data)
			param_set[target_name] = data
		data_vals.append(jk_data)
	return data_vals
	
def exec_data(param_sets, target, target_name):
	start_location = os.getcwd()
	data_vals = []
	for jk_set in param_sets:
		jk_data = []
		for param_set in jk_set:
			target_dir = param_set["Location"]
			print target_dir
			target_prog = target["PROG"]
			output_file = target["OUTPUT"]
			with cd(target_dir):
				sp.call(target_prog+" > energy.dat", shell=True)
				f=open("energy.dat","r")
				data = f.readline()
				f.close()
			data = data.split()
			data = [float(i) for i in data]
			jk_data.append(data)
			param_set[target_name] = data
		data_vals.append(jk_data)
	return data_vals
	
def get_bipartite(param_sets, target, target_name):
	start_location = os.getcwd()
	data_vals = []
	for jk_set in param_sets:
		jk_data=[]
		for param_set in jk_set:
			target_dir = param_set["Location"]
			lattice = param_set["LATTICE"]
			print target_dir
			output_file=target["OUTPUT"]
			with cd(target_dir):
				f=h5py.File("cluster.h5","r")
				bipart=f["/"+lattice+"/bipartite"].value
				print lattice+", bipart= "+str(bipart)
				f.close()
			jk_data.append(bipart)
		data_vals.append(jk_data)
	return data_vals
				
	
def run_old_energy_code(param_sets, target, target_name):
	start_location = os.getcwd()
	data_vals = []
	for jk_set in param_sets:
		jk_data = []
		for param_set in jk_set:
			target_dir = param_set["Location"]
			print target_dir
			target_prog = target["PROG"]
#			Allowed options:
#  --help                show this help
#  --directory arg       directory for GF
#  --nfreq arg           number of Matsubara freqs to integrate
#  --nsite arg           number of sites of cluster
#  --mu arg              chemical potential
#  --beta arg            inverse temperature
#  --U arg               interaction
			print param_set
			options = "--directory "+str(".")+" --nfreq "+str(param_set["NMATSUBARA"])+" --nsite "+str(param_set["SITES"])+" --mu "+str(param_set["MU"])+" --beta "+str(param_set["BETA"])+" --U "+str(param_set["U"])
			with cd(target_dir):
				sp.call(target_prog+" "+options+" > old_energy.dat", shell=True)
				f=open("old_energy.dat","r")
				data = f.readline()
				f.close()
			data = data.split()
			data = [float(i) for i in data]
			jk_data.append(data)
			param_set[target_name] = data
		data_vals.append(jk_data)
	return data_vals
	
def acquire_data(param_sets, target, target_name):
	method = target["TYPE"]
	if(method == "READ"):
		return read_data(param_sets, target, target_name)
	elif(method == "EXEC"):
		return exec_data(param_sets, target, target_name)
	elif(method == "OldEnergyCode"):
		return run_old_energy_code(param_sets, target, target_name)
	elif(method == "BIPARTITE"):
		return get_bipartite(param_sets, target, target_name)
	else:
		print "Error! Unknown data acquisition type!"
		print target
		return [[]]
	
def calculate_sum(data, partial, index):
	num_iter = len(data)
	num_data = len(data[0])
	#print data
	if(type(data[0][0]) is list):
		result = []
		for i in range(0, num_data):
			result.append([0]*len(data[0][0]))
	else:
		result = [0]*num_data
	
	if(partial):
		M=num_iter-1
	else:
		M=num_iter
	
	for i in range(0, num_iter):
		if(partial and i == index):
			continue
		for x in range(0, num_data):
			val = data[i][x]
			if(type(val) is list):
				for y in range(0, len(val)):
					result[x][y] += (1./M)*val[y]
			else:
				result[x] += (1./M)*val

	return result
	
def calculate_mean_error_jackknife(data_ave, data_bar, data_partialsums):
	num_iter = len(data_partialsums)
	num_data = len(data_partialsums[0])
	is_list = False
	len_list = 0
	if(type(data_ave[0]) is list):
		is_list = True
		len_list = len(data_ave[0])
		result = []
		error = []
		partialsquare = []
		for i in range(0, num_data):
			result.append([0]*len_list)
			error.append([0]*len_list)
			partialsquare.append([0]*len_list)
	else:
		result = [0]*num_data
		error = [0]*num_data
		partialsquare = [0]*num_data
		
	#Calculate JK mean
	for i in range(0, num_data):
		if(is_list):
			for j in range(0, len_list):
				result[i][j] = data_ave[i][j] - (num_iter-1)*(data_bar[i][j] - data_ave[i][j])
		else:
			result[i] = data_ave[i] - (num_iter-1)*(data_bar[i] - data_ave[i])
			
	#Calculate partialsum squares for JK error
	for i in range(0, num_iter):
		for j in range(0, num_data):
			if(is_list):
				for k in range(0, len_list):
					partialsquare[j][k] += data_partialsums[i][j][k]*data_partialsums[i][j][k]
			
			else:
				partialsquare[j] += data_partialsums[i][j]*data_partialsums[i][j]
				
	#Calculate JK error
	for i in range(0, num_data):
		if(is_list):
			for j in range(0, len_list):
				error[i][j] = math.sqrt(num_iter-1)*math.sqrt(abs(partialsquare[i][j]/num_iter-data_bar[i][j]*data_bar[i][j]))
		else:
			error[i] = math.sqrt(num_iter-1)*math.sqrt(abs(partialsquare[i]/num_iter-data_bar[i]*data_bar[i]))
	
		
	return (result, error)
	
def jackknife(data):
	#Calculate the jackknife mean and error
	if(not len(data) > 1):
		print "Error! Need more that 1 data set to do jackknife analysis"
		return ([],[],[])
	num_iter = len(data)
	num_data = len(data[0])
	print "Performing jackknife analysis on "+str(num_iter)+" iterations, each with "+str(num_data)+" elements."
	
	data_ave = []
	data_bar = []
	data_partialsums = [[]]*num_iter
	
	#Calculate all the sums needed for jackknife
	data_ave = calculate_sum(data, False, 0)
	for i in range(0, num_iter):
		data_partialsums[i] = calculate_sum(data, True, i)
	data_bar = calculate_sum(data_partialsums, False, 0)
	
	(mean_jk, error_jk) = calculate_mean_error_jackknife(data_ave, data_bar, data_partialsums)
	
	return (data_ave, mean_jk, error_jk)

def main():
	print "I collect data!"
	#Remember starting location
	start_location = os.getcwd()
	print("Starting at: "+start_location)
	
	#Get the name of the json file containing the parameters
	parser = argparse.ArgumentParser(description='Creates a batch file and parameter files that will perform a series of calculations for the given parameters')
	parser.add_argument('input_file',help='JSON file containing the parameters')
	parser.add_argument('output_file_prefix',help='Prefix for data output files')
	args = parser.parse_args()	
	
	#Open and read the json file
	in_filename = args.input_file
	out_fileprefix = args.output_file_prefix
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
	
	#Get the variable parameters (x-axis)
	var_param_name = params["Variable"].keys()[0]
	var_params = params["Variable"][var_param_name]
	print "Variable param: "+var_param_name
	print var_params
	
	#Read or calculate the requested data
	print "Acquiring data"
	for data_target in params["Data"].keys():
		print "Getting "+data_target
		output_file = out_fileprefix+data_target + ".dat"
		data = acquire_data(param_sets, params["Data"][data_target], data_target)
		
		if(len(data[0]) == 0):
			continue
		elif(len(data) > 1):
			(mean, results, jk_error) = jackknife(data)
		else:
			if(type(data[0][0]) is list):
				blank = [[0]*len(data[0][0])]*len(data[0])
				(mean, results, jk_error) = (data[0], blank, blank)
			else:
				(mean, results, jk_error) = (data[0], [0]*len(data[0]), [0]*len(data[0]))
		print output_file
		print "Results: ",mean
		print "Results: ",results
		print "Error: ", jk_error
		f = open(output_file, 'w')
		f.write("#"+var_param_name+"\t"+data_target+" (mean)\t"+data_target+" (jk)\tError\n")
		for i in range(0, len(results)):
			f.write(str(var_params[i])+"\t")
			if(type(mean[i]) is list):
				for x in mean[i]:
					f.write(str(x)+"\t")
				for x in results[i]:
					f.write(str(x)+"\t")
				for x in jk_error[i]:
					f.write(str(x)+"\t")
			else:
				f.write(str(mean[i])+"\t"+str(results[i])+"\t"+str(jk_error[i])+"\t")
			f.write("\n")
		f.close()
				
	
	
						
	data_log_filename = out_fileprefix+"DATALOG.json"
	data_log = open(data_log_filename,"w")
	log={}
	for jk_set in param_sets:
		for param_set in jk_set:
			print param_set
			log.update({param_set["Location"]:param_set})
	data_log.write(json.dumps(log))
	data_log.close()
	
if __name__ == "__main__":
	main()
