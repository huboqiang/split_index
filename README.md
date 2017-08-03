Demultiplexes a fastq.

Usage:

```
Usage: index_split.py [options] fastq1 fastq2 barcod1,barcode2,...,barcodeN

   Using -h or --help for more information

Example:
   python index_split.py -t 10 -l 150 -b 0 test11.fq.gz test22.fq.gz TTAGGC,TGACCA,GCCAAT,CAGATC,ACTTGA,GATCAG



 Split fastq file by the barcode

Options:
  --version             show program's version number and exit
  -t PHREADS, --phreads=PHREADS
                         Phreads uses, [default: 5]
  -l READ_LENG, --read_leng=READ_LENG
                         Read length [default: 101]
  -b BARCODE_ERROR, --barcode_error=BARCODE_ERROR
                         Error in barcodes [default: 1]
  -h, --help             Show this help message and exit.
```
