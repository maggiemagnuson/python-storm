cd $(dirname $0)
petrel submit --config topology.yaml --logdir `pwd`
#petrel submit --extrastormcp=`pwd` --config topology.yaml