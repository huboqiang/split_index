from __future__ import division
import re,sys,os
import cPickle as pickle
import numpy as np
import subprocess,time
from optparse   import OptionParser


def prepare_optparser():
   usage ="""usage: %s [options] <input.sam> [chrom] [proj_path] [Out_prefix]

   Using -h or --help for more information

Example:
   python %s -t 10 -l 150 -b 0 test11.fq.gz test22.fq.gz TTAGGC,TGACCA,GCCAAT,CAGATC,ACTTGA,GATCAG
   
   """ % (sys.argv[0],sys.argv[0])

   description = " Split fastq file by the barcode "

   optparser = OptionParser(version="%s v0.2 20140528" % (sys.argv[0]),description=description,usage=usage,add_help_option=False)
   optparser.add_option("-t", "--phreads",      default=5  , help="\nPhreads uses, [default: %default]")
   optparser.add_option("-l", "--read_leng",    default=101, help="\nRead length [default: %default]")
   optparser.add_option("-b", "--barcode_error",default=1  , help="\nError in barcodes [default: %default]")
   
   optparser.add_option("-h","--help",      action="help",       help="\nShow this help message and exit.")
   return optparser
   

def main():
   prepare_optparser()
   (options,args) = prepare_optparser().parse_args()
   try:
      infile_1    =  args[0]
      infile_2    =  args[1]
      bar_seq     =  args[2]
      
      phreads        = int(options.phreads)
      read_leng      = int(options.read_leng)
      barcode_error  = int(options.barcode_error)
      
   except IndexError:
      prepare_optparser().print_help()
      sys.exit(1)
      
   l_bar_seq = bar_seq.split(",")
   shell_info = "/WPS1/huboqiang/bin/split_index/index_split -t %d -l %d -b %d %s %s %s" % ( phreads,read_leng,barcode_error,  infile_1,infile_2,bar_seq )
   print shell_info
   p = subprocess.Popen(shell_info,shell='True')
   while 1:
      run_cnt = 0
      if p.poll() is None:
         run_cnt += 1
         time.sleep(1)
      if run_cnt == 0:
         break
   
   merge_sh       = "merge.sh"
   merge_work_sh  = "merge_work.sh"
   f_merge_sh     = open( merge_sh     ,"w" )
   f_merge_work_sh= open( merge_work_sh,"w" )

   shell_info = """
merge_fq=$1
shift

cat $@ >$merge_fq && rm $@
   """
   print >>f_merge_sh, shell_info
   
   l_bar_seq.append("undef")
   for bar_cod in l_bar_seq:
      l_file1 = []
      l_file2 = []
      for i in range(0,phreads):
         l_file1.append( "%s.%s.block%d.gz" % (infile_1,bar_cod,i) )
         l_file2.append( "%s.%s.block%d.gz" % (infile_2,bar_cod,i) )
      list_file1 = " ".join( l_file1 )
      list_file2 = " ".join( l_file2 )
      
      out_file1 = "%s.%s.fq.gz" % ( ".".join( infile_1.split('.')[:-2] ),bar_cod )
      out_file2 = "%s.%s.fq.gz" % ( ".".join( infile_2.split('.')[:-2] ),bar_cod )
      
      print >>f_merge_work_sh, "sh %s  %s %s" % (merge_sh,out_file1,list_file1)
      print >>f_merge_work_sh, "sh %s  %s %s" % (merge_sh,out_file2,list_file2)
   
   f_merge_sh.close()
   f_merge_work_sh.close()
   
   shell_work = "sh %s" % ( merge_work_sh )
#   shell_work = "perl /data/Analysis/huboqiang/bin/qsub-sge.pl --resource vf=200m --maxjob 100 %s" % ( merge_work_sh )
   print shell_work

   p = subprocess.Popen(shell_work,shell='True')
   while 1:
      run_cnt = 0
      if p.poll() is None:
         run_cnt += 1
         time.sleep(3)
      if run_cnt == 0:
         break
   
if __name__ == '__main__':
   main()
