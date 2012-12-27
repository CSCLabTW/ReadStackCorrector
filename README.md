ReadStackCorrector is a software to correct substitution sequencing errors in experiments with deep coverage, specifically intended for Illumina sequencing reads. It is released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) as a Free and Open Source Software Project.

More details about ReadStac you can get under: https://github.com/ice91/ReadStackCorrector

requirement: hadoop cluster

step 1: convert *.fastq to *.sfq
e.g. java Fastq2Sfq E_coli.fastq E_coli.sfq

step 2: upload *.sfq to HDFS
e.g. hadoop fs -put E_coli.sfq E_coli

step 3: exceute the ReadStackCorrector
e.g. hadoop jar ReadStackCorrector.jar -in Ec10k -out Ec10k_ec