ReadStackCorrector is a software to correct substitution sequencing errors in experiments with deep coverage, specifically intended for Illumina sequencing reads. It is released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) as a Free and Open Source Software Project.

More details about ReadStackCorrector you can get under: https://github.com/ice91/ReadStackCorrector

requirement: hadoop cluster


step 1: convert *.fastq to *.sfq

e.g. java Fastq2Sfq Ecoli.fastq Ecoli.sfq


step 2: upload *.sfq to HDFS

e.g. hadoop fs -put Ecoli.sfq Ecoli


step 3: exceute the ReadStackCorrector

e.g. hadoop jar ReadStackCorrector.jar -in Ecoli -out Ecoli_ec


step 4: download *.fasta from HDFS

e.g. hadoop fs -cat Ecoli_ec_file/* > Ecoli_ec.fasta
