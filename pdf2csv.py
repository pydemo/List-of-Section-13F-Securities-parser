 #!/usr/bin/python 
"""
Usage:
  cd /Bic/scripts/oats/py27/bin/
  ./python pdf2csv.py --in_pdf_file --out_txt_file --out_csv_file 
  --in_pdf_file  : Input pdf file  (13flist2016q4.pdf)
  --out_txt_file : Output CSV file (13flist2016q4.txt)
  --out_csv_file : Output CSV file (13flist2016q4.csv)
  
"""



from __future__ import print_function
import os, time, sys
import urllib2 as urllib
import json
import logging
import pprint as pp
import atexit
from optparse import OptionParser
from pprint import pprint
import traceback
import hashlib
from subprocess import Popen, PIPE
def formatExceptionInfo(maxTBlevel=5):
	cla, exc, trbk = sys.exc_info()
	excName = cla.__name__
	try:
		excArgs = exc.__dict__["args"]
	except KeyError:
		excArgs = "<no args>"
	excTb = traceback.format_tb(trbk, maxTBlevel)
	#print(excName, excArgs, excTb)
	return ', '.join([excName, excArgs, ', '.join(excTb)])
e=sys.exit
DEFAULT_ENCODING = 'utf-8'
#////////////////////////
EXIT_FAILURE		= 1
EXIT_EMPTY_JSON_DOC= 99
EXIT_BAD_URL		= 100
EXIT_SUCCESS 		= 0
#////////////////////////
def create_symlink(from_dir, to_dir):
	if (os.name == "posix"):
		os.symlink(from_dir, to_dir)
	elif (os.name == "nt"):
		os.system('mklink /J %s %s' % (to_dir, from_dir))
	else:
		log.error('Cannot create symlink. Unknown OS.', extra=d)
def unlink(dirname):
	if (os.name == "posix"):
		os.unlink(dirname)
	elif (os.name == "nt"):
		os.rmdir( dirname )
	else:
		log.error('Cannot unlink. Unknown OS.', extra=d)
JOB_NAME,_=os.path.splitext(os.path.basename(__file__))
assert JOB_NAME, 'Job name is not set'
HOME= os.path.dirname(os.path.abspath(__file__))
ts=time.strftime('%Y%m%d_%a_%H%M%S')
ts=time.strftime('%Y%m%d_%a')
dr=os.path.dirname(os.path.realpath(__file__))
latest_dir =os.path.join(dr,'log',JOB_NAME,'latest')
ts_dir=os.path.join(dr,'log',JOB_NAME,ts)
config_home = os.path.join(HOME,'config')
latest_out_dir =os.path.join(HOME,'log',JOB_NAME,'output_latest')
ts_out_dir=os.path.join(HOME,'log',JOB_NAME,ts,'output')
latest_dir =os.path.join(HOME,'log',JOB_NAME,'latest')
log_dir = os.path.join(HOME, 'log',JOB_NAME)
ts_dir=os.path.join(log_dir, ts)

done_file= os.path.join(ts_dir,'DONE.txt')
job_status_file=os.path.join(ts_dir,'%s.%s.status.py' % (os.path.splitext(__file__)[0],JOB_NAME))	
if not os.path.exists(ts_dir):
	os.makedirs(ts_dir)
if not os.path.exists(ts_out_dir):
	os.makedirs(ts_out_dir)

if  os.path.exists(latest_out_dir):
	unlink(latest_out_dir)
#os.symlink(ts_out_dir, latest_out_dir)
create_symlink(ts_out_dir, latest_out_dir)
if  os.path.exists(latest_dir):	
	unlink(latest_dir)
create_symlink(ts_dir, latest_dir)	

DEBUG=0
	
d = {'iteration': 0,'pid':os.getpid(), 'rows':0}
FORMAT = '|%(asctime)-15s|%(pid)-5s|%(iteration)-2s|%(rows)-9s|%(message)-s'
FORMAT = '|%(asctime)-15s%(pid)-5s|%(name)s|%(levelname)s|%(message)s'



logging.basicConfig(filename=os.path.join(ts_dir,'%s_%s.log' % (JOB_NAME,ts)),level=logging.INFO,format=FORMAT)
log = logging.getLogger(JOB_NAME)
log.setLevel(logging.DEBUG)


exit_status={}
exit_status['Exception']=None
def save_status():
	global job_status_file, exit_status, opt, d

	p = pp.PrettyPrinter(indent=4)
	with open(job_status_file, "w") as py_file:			
		py_file.write('status=%s' % (p.pformat(exit_status)))
		#log.info (job_status_file, extra=d)
DEFAULT_PDF= '13flist2016q4.pdf'
DEFAULT_TXT= '13flist2016q4.txt'
DEFAULT_CSV= '13flist2016q4.csv'
DEFAULT_LOG_TIMESTAMP = ts
DEFAULT_LOG_RETENTION_DAYS	= 3	
DEFAULT_PDFTOTEXT_LOCATION = './pdftotext/bin32/pdftotext'	
DEFAULT_START_PAGE = 3
def er(msg):
	log.error(msg, extra=d)
def ok(msg):
	log.info(msg, extra=d)

	
		
if __name__ == "__main__":	
	atexit.register(save_status)
	parser = OptionParser()
	parser.add_option("-p", "--in_pdf_file", dest="in_pdf_file", type=str, default=DEFAULT_PDF)
	parser.add_option("-t", "--out_txt_file", dest="out_txt_file", type=str, default=DEFAULT_TXT)
	parser.add_option("-c", "--out_csv_file", dest="out_csv_file", type=str, default=None)
	parser.add_option("-d", "--col_delim", dest="col_delim", type=str, default='|')
	parser.add_option("-e", "--add_header",  action="store_true", dest="add_header", default=False, help="Add header record to output CSV file.")
	parser.add_option("-v", "--pdftotext_location", dest="pdftotext_location", type=str, default=DEFAULT_PDFTOTEXT_LOCATION)
	parser.add_option("-f", "--start_from_page", dest="start_from_page", type=int, default=DEFAULT_START_PAGE)
	
	
	parser.add_option("-x", "--delete_existing_out_file",  action="store_true", dest="delete_existing_out_file", default=True,
				  help="Delete existing out file before parse.")
	parser.add_option("-j", "--job_name", dest="job_name", type=str, default=JOB_NAME)
	parser.add_option("-l", "--log_timestamp", dest="log_timestamp", type=str, default=DEFAULT_LOG_TIMESTAMP)
	parser.add_option("-r", "--log_retention_days", dest="log_retention_days", type=str, default=DEFAULT_LOG_RETENTION_DAYS)
	parser.add_option("-L", "--hide_log_output",  action="store_true", dest="hide_log_output", default=False, help="Suppress terminal log messages.")
	
	(opt, args) = parser.parse_args()
	parser_dir =os.path.dirname(opt.pdftotext_location)
	parser_name =os.path.basename(opt.pdftotext_location)
	if not opt.out_csv_file:
		out_dir=os.path.dirname(os.path.abspath(opt.in_pdf_file))
		out_pdf_fn=os.path.basename(os.path.abspath(opt.in_pdf_file))
		assert out_pdf_fn.endswith('.pdf'), 'Wrong PDF file name.'
		
		out_txt_fn=out_pdf_fn[:-4]+'.txt'
		out_csv_fn=out_pdf_fn[:-4]+'.csv'
		
		out_txt= os.path.join (out_dir,out_txt_fn) 
		
		
		out_csv= os.path.join (out_dir,out_csv_fn) 
	else:
		#out_csv_file_name =os.path.basename(opt.out_csv_file)
		out_txt= os.path.abspath(opt.out_txt_file) 
		#out_csv= os.path.join(latest_dir,out_csv_file_name )
		
		out_csv= os.path.abspath(opt.out_csv_file)
	#e(0)
	if not opt.hide_log_output:
		ch = logging.StreamHandler(sys.stdout)
		ch.setLevel(logging.DEBUG)
		formatter = logging.Formatter(FORMAT)
		ch.setFormatter(formatter)
		log.addHandler(ch)
	assert os.path.isfile(opt.pdftotext_location), '"pdftofile" does not exists at "%s"' % opt.pdftotext_location
	assert os.path.isfile(opt.in_pdf_file), 'Source PDF file does not exists at "%s"' % opt.in_pdf_file
	cmd=('%s -f %d -layout %s %s' % (parser_name,opt.start_from_page,opt.in_pdf_file, out_txt)).split(' ')
	log.info('Executing command:', extra=d)
	cmdl=' '.join(cmd)
	print ('-'*len(cmdl))
	print ('cd %s' % parser_dir)
	print (cmdl)
	print ('-'*len(cmdl))
	if 1:
		import os
		os.chdir(parser_dir)
		p= Popen(cmd, stdout=PIPE,stderr=PIPE,env=os.environ)
		output, err = p.communicate()
		if output:
			map(ok, output.split(os.linesep))
		if err:
			map(er, err.split(os.linesep))
		status = p.wait()
		log.info('Status: %d' % status, extra=d)
	#
	#
	#
	os.chdir(HOME)
	
	#e(0)
	with open(out_csv, "wb") as csvfh:
		text=None
		print (os.path.abspath(out_txt))
		assert os.path.isfile(os.path.abspath(out_txt)), 'Out file does not exists.\n%s' % out_txt
		
		with open(os.path.abspath(out_txt), "rb") as txtfh:
			text = txtfh.read()
		pages=text.split(b'\f')
		log.info('Number of pages: %d ' % len(pages), extra=d)
		cn=['CUSIP NO','ISSUER NAME','ISSUER DESCRIPTION','STATUS']
		
		header =None
		if opt.add_header:
			cols=['COL1','COL2','COL3','COL4','COL5','COL6','COL7']
			csvfh.write(opt.col_delim.join(cols))			
			csvfh.write(os.linesep)
		for page in pages:
			#print (page)
			#e(0)
			csv_page=[]
			cpos=[]

			for row in [x for x in page.split(os.linesep) if x and not x.strip().startswith('Total Count:')][2:]:
				#print(row.startswith(cn[0]),row)
				print(row)
				if row.startswith(cn[0]):
					header=row
					for c, col in enumerate(cn):
						#print(c)
						cpos.append(row.find(cn[c]))
						#csv_row
					
					#print(cpos)
				else:
					assert cpos, 'Position list is not set.'
					#print('-'*30)
					#print (header)
					#print('-'*30)
					
					#pprint(row)
					prev_pos =0
					csv_row=[]
					print(cpos)
					for i,pos in enumerate(cpos):
						if i==len(cpos)-1:
							#print (pos,row[prev_pos:])
							colval=row[prev_pos:].strip()
							#print (i,colval)
							ends=['ADDED','DELETED']
							found_end=False
							for END in ends:
								if colval.endswith(END):	
									#a1=colval[len(colval)-len(END)].strip()
									#print ('end', a1)
									#csv_row.append(a1)
									csv_row.append(END)									
									found_end =True
									break
							if not found_end:
								csv_row.append(colval)
								csv_row.append('')
							
						else:
							#print (pos,row[prev_pos:cpos[i+1]])
							
							colval=row[prev_pos:cpos[i+1]].strip()
							#print (i,colval)
							if prev_pos==0:
								firs_col_vals = colval.split(' ')
								for val in firs_col_vals:
									print('append', val)
									csv_row.append(val)
								if len(firs_col_vals)==3:
									#fix missing *
									csv_row.append('')
							else:
								csv_row.append(colval)
							prev_pos=cpos[i+1]
					if csv_row[0] in ['74347W'] and  csv_row[1] in ['21'] :
						pprint(csv_row)
						#print(' '.join(csv_row))
						#e(0)
					#e(0)
					csv_page.append(csv_row)
			#pprint(csv_page)
			csvfh.write(os.linesep.join([opt.col_delim.join(x) for x in csv_page]))
			if len(csv_page)>1:csvfh.write(os.linesep)
			#e(0)
		csvfh.write(os.linesep)
	log.info('CSV file:', extra=d)
	print(out_csv)
 
