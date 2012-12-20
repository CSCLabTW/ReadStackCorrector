ReadStackCorrector is a software to correct substitution sequencing errors in experiments with deep coverage, specifically intended for Illumina sequencing reads. It is released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) as a Free and Open Source Software Project.

More details about ReadStac you can get under: https://github.com/ice91/ReadStackCorrector

requirement: hadoop cluster

e.g hadoop fs -put data/Ec10k.sim.sfq Ec10k

hadoop jar ReadStackCorrector.jar -in Ec10k -out Ec10k_ec