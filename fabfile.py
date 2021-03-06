from fabric.api import *

env.hosts = ["sp17-cs425-g20-01.cs.illinois.edu", "sp17-cs425-g20-02.cs.illinois.edu", 
			 "sp17-cs425-g20-03.cs.illinois.edu", "sp17-cs425-g20-04.cs.illinois.edu", 
			 "sp17-cs425-g20-05.cs.illinois.edu", "sp17-cs425-g20-06.cs.illinois.edu", 
			 "sp17-cs425-g20-07.cs.illinois.edu", "sp17-cs425-g20-08.cs.illinois.edu"]
#env.hosts = ["sp17-cs425-g20-09.cs.illinois.edu", "sp17-cs425-g20-10.cs.illinois.edu"]
env.user = "bharuka2"

# def push(msg):
# 	local('git add . && git commit -m "%s"' % msg, capture=False)
# 	local('git push -u origin master', capture=False)

def install():
	with cd('~'):
		run('sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm')
		run('sudo yum -y install python36u')

def clone():
	with cd('~'):
		run('git clone git@gitlab.engr.illinois.edu:bharuka2/distributed_mp2.git')

def deploy():
	with cd('/home/bharuka2/distributed_mp2'):
		run('git pull')
