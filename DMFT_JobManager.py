#import sys
import os
import subprocess as sp
import argparse
import json

indep_combos=False

#Includes a walltime safety factor of 2
def write_batch_header(batch_file, processname, batch_parameters, total_time):
	if(batch_parameters["BATCH_TYPE"] == "SBATCH"):
		batch_file.write("#!/bin/bash\n")
		batch_file.write("#SBATCH -J "+processname+"\n")
		batch_file.write("#SBATCH -o "+processname+".o%j\n")
		batch_file.write("#SBATCH -e "+processname+".e%j\n")
		batch_file.write("#SBATCH -n "+batch_parameters["PROCS"]+"\n")
		batch_file.write("#SBATCH -p normal\n")
		batch_file.write("#SBATCH -t "+str(int(total_time * 2))+":00:00\n")
		batch_file.write("#SBATCH --mail-user="+batch_parameters["EMAIL"]+"\n")
		batch_file.write("#SBATCH --mail-type=begin\n")
		batch_file.write("#SBATCH --mail-type=end\n")
		batch_file.write("####  End PBS preamble\n")
		batch_file.write("\n")
		batch_file.write("#  Show list of CPUs you ran on, if you're running under PBS\n")
		batch_file.write("""if [ -n "$SLURM_NODELIST" ]; then cat $SLURM_NODELIST; fi\n""")
		batch_file.write("#  Change to the directory you submitted from\n")
		batch_file.write("""if [ -n "$SLURM_SUBMIT_DIR" ]; then cd $SLURM_SUBMIT_DIR; fi\n""")
		batch_file.write("pwd\n")
		batch_file.write("\n")
		batch_file.write("#  Put your job commands after this line\n")
		
	elif(batch_parameters["BATCH_TYPE"] == "PBS"):
		batch_file.write("####  PBS preamble\n")
		batch_file.write("#PBS -N "+processname+"\n")
		batch_file.write("#PBS -M "+batch_parameters["EMAIL"]+"\n")
		batch_file.write("#PBS -m abe\n")
		batch_file.write("#PBS -l procs="+str(batch_parameters["PROCS"])+",pmem="+str(batch_parameters["PMEM"])+",walltime="+str(int(total_time * 2+1))+":00:00\n")
		batch_file.write("#PBS -j oe\n")
		batch_file.write("#PBS -V\n")
		batch_file.write("#PBS -A "+batch_parameters["ALLOCATION"]+"\n")
		batch_file.write("#PBS -l qos=flux\n")
		batch_file.write("#PBS -q "+batch_parameters["QUEUE"]+"\n")
		batch_file.write("####  End PBS preamble\n")
		batch_file.write("\n")
		batch_file.write("#  Show list of CPUs you ran on, if you're running under PBS\n")
		batch_file.write("""if [ -n "$PBS_NODEFILE" ]; then cat $PBS_NODEFILE; fi\n""")
		batch_file.write("#  Change to the directory you submitted from\n")
		batch_file.write("""if [ -n "$PBS_O_WORKDIR" ]; then cd $PBS_O_WORKDIR; fi\n""")
		batch_file.write("pwd\n")
		batch_file.write("\n")
		batch_file.write("#  Put your job commands after this line\n")
		
	else:
		print "Unknown batch type! Exiting!\n"
		sys.exit()

def write_parameterfile(independent_parameters, phys_const_params, sim_const_params, phys_var_params, sim_var_params, job_id):
	paramfile = "paramfile"
	output = open(paramfile,'w')
	
	for field in independent_parameters.keys():
		output.write(field + " = " + str(independent_parameters[field])+'\n')
	
	for field in phys_const_params.keys():
		output.write(field + " = " + str(phys_const_params[field])+'\n')
		
	for field in phys_var_params.keys():
		output.write(field + " = " + str(phys_var_params[field][job_id])+'\n')
		
	for field in sim_const_params.keys():
		output.write(field + " = " + str(sim_const_params[field])+'\n')			

	for field in sim_var_params.keys():
		output.write(field + " = " + str(sim_var_params[field][job_id])+'\n')
		
	output.close()
	
def check_job_parameters(job_params, param_defs):
	#Check that the parameters are known to DMFT code, figure out what types they are
	#Warn if a required parameter is not defined
	avail_params = job_params.keys()
	defined_params = param_defs.keys()
	for field in param_defs:
		if(param_defs[field]["required"]==1 and not field in avail_params):
			print "Warning! Parameter "+field+" not defined, but it is required!"
			print "Description: "+param_defs[field]["description"]
	types = []
	for field in job_params:
		if(not field in defined_params):
			print "Unknown parameter! -> "+field
		else:
			if(not param_defs[field]["type"] in types):
				types.append(param_defs[field]["type"])
	
	return sorted(types)
	
def write_parameterfile_simple(job_parameters, directory):
	#Write the parameter file for DMFT
	
	json_data = open("parameter_definitions.json")
	param_defs = json.load(json_data)	
	json_data.close()
	
	#Check that the parameters are known to DMFT code, figure out what types they are
	types = check_job_parameters(job_parameters, param_defs)
	
	paramfile = directory+"/paramfile"
	output = open(paramfile,'w')
	
	#Write the parameter file, with params grouped according to type [sc], [solver], etc.
	for ptype in types:
		if(not ptype == ""):
			output.write("\n["+ptype+"]\n")
		for field in job_parameters.keys():
			if(param_defs[field]["type"] == ptype):
				output.write(field + " = " + str(job_parameters[field])+'\n')
		
	output.close()
	return paramfile
	
def setup_separate_jobs(ind_params, phys_const_params, sim_const_params, phys_var_params, sim_var_params, batch_params, log, num_var_params):

	submit_script = open("submit_script.sh","w")
	start_location = os.getcwd()
	#Get all of the independent parameter sets
	indep_dirs = [{}]
	
	#Do we want all combinatoric mixtures of indepedent parameters?
	if(indep_combos):
		for field in ind_params.keys():
			old_len = len(indep_dirs)
			for k in range(0,len(indep_dirs)):
				for param in ind_params[field]:
					temp=indep_dirs[k].copy()
					temp[field]=param
					indep_dirs.append(temp)
			del indep_dirs[0:old_len]
	else:	
		indep_dirs = []
		num_ind_jobs = len(ind_params[ind_params.keys()[0]])
		for k in range(0, num_ind_jobs):
			temp = {}
			for field in ind_params.keys():
				temp[field] = ind_params[field][k]
			indep_dirs.append(temp)
		
	#Create a directory for each set of independent parameters
	for indep_job in indep_dirs:
		dir_name="hub"
		print indep_job
		for field in indep_job:
			dir_name=dir_name+"_"+str(field)+"_"+str(indep_job[field])
		print "Creating independent param directory: "+dir_name
		command = 'mkdir ./'+dir_name
		sp.call(command,shell=True)
		
		#Create a directory for each variable parameter set, and a parameter and batch file
		for i in range(0, num_var_params):
			job_dict = indep_job.copy()
			directory = dir_name
			for field in phys_var_params.keys():
				directory = str(directory +"/"+field+"_"+str(phys_var_params[field][i]))
				job_dict[field]=phys_var_params[field][i]
			for field in sim_var_params.keys():
				job_dict[field]=sim_var_params[field][i]
				#directory = str(directory+"/"+field+"_"+str(sim_var_params[field][i]))
			print directory
			command = 'mkdir -p ./'+directory
			sp.call(command,shell=True)
			
			#Write parameter file
			job_dict.update(phys_const_params)
			job_dict.update(sim_const_params)
			paramfile = write_parameterfile_simple(job_dict, directory)
			#print job_dict
			job_dict.update(batch_params)
			
			#Update the parameter log with this job
			log.update({directory:job_dict})
			
			#Figure out how much time this job needs
			if("MAX_IT" in sim_const_params.keys()):
				Max_It = sim_const_params["MAX_IT"]
			else:
				Max_It = sim_var_params["MAX_IT"][i]	
		
			if("MAX_TIME" in sim_const_params.keys()):
				Max_Time = sim_const_params["MAX_TIME"]
			else:
				Max_Time = sim_var_params["MAX_TIME"][i]	
			total_time = Max_Time*Max_It
				
			#Write the batch file
			batchfile = open(directory+"/hubbard.pbs",'w')
			write_batch_header(batchfile, directory, batch_params, total_time/3600.0)
			batchfile.write(batch_params["DMFT_LOCATION"]+" paramfile\n")
			batchfile.close()
			
			#Add to submit_script
			submit_script.write("cd "+directory+"\n")
			submit_script.write("qsub hubbard.pbs\n")
			submit_script.write("cd "+start_location+"\n")
			
	submit_script.close()	
	

def setup_together_jobs():
	print("Not implemented yet!")

def main():
	print("Hi! I setup DMFT jobs from an input JSON file.")
	#Remember starting location
	start_location = os.getcwd()
	print("Starting at: "+start_location)
		
	#Get the name of the json file containing the parameters
	parser = argparse.ArgumentParser(description='Creates a batch file and parameter files that will perform a series of calculations on a system as progessively lower temperatures.')
	parser.add_argument('input_file',help='JSON file containing the parameters')
	args = parser.parse_args()	
	
	#Open and read the json file
	in_filename = args.input_file
	print 'Reading parameter file: ',in_filename
	json_data = open(in_filename)
	params = json.load(json_data)	
	json_data.close()
	
	#Get all the different parameters
	ind_params = params["independent parameters"]
	batch_params = params["batch parameters"]
	phys_const_params = params["physics parameters"]["constant"]
	sim_const_params = params["simulation parameters"]["constant"]
	phys_var_params = params["physics parameters"]["variable"]
	sim_var_params = params["simulation parameters"]["variable"]
	
	#Figure out how many independent parameter sets there are, and whether they are each run as single job
	jobs_dist = batch_params["JOBS_DISTRIBUTION"]
	ind_params_names = len(ind_params.keys())
	print("#independent parameter names: "+str(ind_params_names))
	
	if(indep_combos):
		num_ind_params=1
		for key in ind_params.keys():
			num_ind_params *= len(ind_params[key])
	else:
		num_ind_params = len(ind_params[ind_params.keys()[0]])
	print("#independent parameter sets: "+str(num_ind_params))
	
	#Figure out how many variable parameters there are, and then total number of parameter sets
	num_var_params = 1
	if(len(phys_var_params.keys()) > 0):
		num_var_params = len(phys_var_params[phys_var_params.keys()[0]])
	num_parameter_sets = num_ind_params*num_var_params;
	print("#total parameter sets: "+str(num_parameter_sets))
	
	#Setup (or load existing) parameter log file (JSON)
	log_name="parameter_log.json"
	log={}
	if os.path.isfile(log_name):
		log_file = open(log_name)
		log.update(json.load(log_file))
		log_file.close()
	
	#print log
	#print(json.dumps(log))	
	#temp={"job3":{"PATH":"adf","BETA":1}}
	#log.update(temp)
		
	#For future, if we wanted to add interpolation for col down, could do in TOGETHER
	if(jobs_dist=="SEPARATE"):
		print("JOBS_DISTRIBUTION=SEPARATE, so each parameter set will get a batch file")
		setup_separate_jobs(ind_params, phys_const_params, sim_const_params, phys_var_params, sim_var_params, batch_params, log, num_var_params)
	elif(jobs_dist=="TOGETHER"):
		print("JOBS_DISTRIBUTION=TOGETHER, so individual parameter sets will get one batch file to run all constituent parameter sets")
	else:
		print("Invalid option for JOBS_DISTRIBUTION, use SEPARATE or TOGETHER")	
		
	log_file = open(log_name,'w')
	log_file.write(json.dumps(log))		
	
	

if __name__ == "__main__":
	main()
