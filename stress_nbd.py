import subprocess
import argparse
import threading
import sys
from datetime import datetime
from time import sleep
import csv
import random

parser = argparse.ArgumentParser(description='Process nbd server options/')
parser.add_argument('-s', '--server', help='nbd-server address')
parser.add_argument('-b', '--blockdev', default='/dev/nbd2', help='block device you wish to use .e.g. /dev/nbd2')
parser.add_argument('-n', '--nbdtype', help='type of nbd-server e.g. nbd-tool.2.1.3')
parser.add_argument('-t', '--test', help='type of type you would like to run')
parser.add_argument('--setup', dest='setup', default=False, help="Set if throughput tests need to be setup")
parser.add_argument('-p', '--port', default='')

args = parser.parse_args()

#Tests can be run in the format: python stress_nbd.py -s xrtuk-14-12.xenrt.citrite.net -b /dev/nbd2 -t repeat
#                                python stress_nbd.py -s <server> -b <block device> -t <test_type> 

#Core functions
def write_to_csv(filename, contents):
    csvfile = open(filename, 'a')
    rowwriter = csv.writer(csvfile)
    rowwriter.writerow(contents)
    csvfile.close() 

def server_connect(ip, server, blockdev, name):
    try:
    	modprobe_command = "modprobe nbd"
    	ssh_host(ip, modprobe_command)
    	read_command = "nbd-client -N %s %s %s" %(name, server, blockdev)
    	print read_command
    	startTime = datetime.now()
    	connection = ssh_host(ip, read_command)
    	print "Connected"
    	write_to_csv("multi.csv", [ip, blockdev])
    	print "Connected to server from %s %s" %(ip, blockdev)
    except:
        print "Connection on %s to block device %s failed" %(ip, blockdev)

def server_read(ip, blockdev, num=2):
	try:
		startTime = datetime.now()
		print "Reading blocks on %s %s" %(ip, blockdev)
		check_blocks = "dd if=%s of=./output%s bs=64K" %(blockdev, num)
		blocks = ssh_host(ip, check_blocks)
		ssh_host(ip, "sync")
		read_time = [datetime.now() - startTime, datetime.now(), ip, blockdev]
		write_to_csv("time.csv", read_time)
		server_disconnect(ip, blockdev)
		delete_command = "rm output%s" %num
		ssh_host(ip, delete_command)
		return read_time
	except:
		print "Copy blocks on %s from block device %s failed" %(ip, blockdev)
		server_disconnect(ip, blockdev)

#Server read without the disconnect for tests where we just want continuous reads
def server_read_alt(ip, blockdev, num=2):
	try:
		startTime = datetime.now()
		print "Reading blocks on %s %s" %(ip, blockdev)
		check_blocks = "dd if=%s of=./output%s bs=64K" %(blockdev, num)
		blocks = ssh_host(ip, check_blocks)
		ssh_host(ip, "sync")
		read_time = [datetime.now() - startTime, datetime.now(), ip, blockdev]
		write_to_csv("time.csv", read_time)
		delete_command = "rm output%s" %num
		ssh_host(ip, delete_command)
		return read_time
	except:
		print "Copy blocks on %s from block device %s failed" %(ip, blockdev)

def server_disconnect(ip, blockdev):
	try:
		disconnect_command = "nbd-client -d %s" %blockdev
		ssh_host(ip, disconnect_command)
		print "Disconnected %s %s successfully" %(ip, blockdev)
	except:
		print "Disconnect of %s %s failed" %(ip, blockdev)

#Records the CPU and Memory usage on the host and no. of open FDs
def host_stats(server, nbd_type):
	command = "ps --no-headers -o %%cpu,rss -C %s; lsof -c %s | wc -l" %(nbd_type, nbd_type)
	stats = (ssh_host(server, command)).split()
	stats.append(datetime.now())
	write_to_csv("stats.csv", stats)

def ssh_host(ip, command):
	username = "root"
	password = "xenroot"
	return subprocess.check_output(['sshpass', '-p', password, 'ssh', '-oStrictHostKeyChecking=no', '%s@%s' %(username, ip), command])

def one_connect(ip, server, blockdev, name):
	for i in xrange(0, 13):
		block = blockdev + str(i)
		server_connect(ip, server, block, name)

def continual_read(ip, server, blockdev, name):
	for i in xrange(0, 13):
		block = blockdev + str(i)
		t = threading.Thread(target=server_read_alt, args=(ip, block, i,))
		t.start()


#Setup functions
def setup_client(ip):
	try:
	    install_client = "apt-get --assume-yes install nbd-client"
	    ssh_host(ip, install_client)
	    start_nbd = "modprobe nbd"
	    ssh_host(ip, start_nbd)
	except:
		pass

def setup_throughput_tests(ip):
	setup_command = "apt-get --assume-yes install dpkg-dev; apt-get --assume-yes build-dep nbd-server; apt-get --assume-yes source -b nbd-server; chmod u+x /root/nbd-3.15.2/tests/run/nbd-tester-client"
	ssh_host(ip, setup_command)


#Test functions
#Test utilizing the nbd-tester-client
def throughput_test(ip, server, blockdev, name):
	if name == "nbd-server":
		port = "10809"
	else:
		port = ""
	test_command = "cd /root/nbd-3.15.2/tests/run/; ./nbd-tester-client %s %s -N %s" %(server, port, blockdev)
	print test_command
	print "------------------------------------------------------------"
	print "For %s the results from the nbd-tester-client with server type %s are:\n" %(ip, name)
	ssh_host(ip, test_command)
	print "------------------------------------------------------------"
	scp_command = "sudo sshpass -p xenroot scp root@%s:/root/log-%s.txt ~/log.txt" %(ip, ip)

def repeated_connect(ip, server, blockdev, name, port, nbd_type):
    x = 0 
    server = "%s %s" %(server, port)
    while x < 100:
    #while True:
    	startTime = datetime.now()
        try:
            print server_connect(ip, server, blockdev, name)
            print server_disconnect(ip, blockdev)
            read_time = server_read(ip, blockdev)
        except:
            print "Connection was refused after %s connections" %x
            break
        x += 1
        print "Connection %s: It took %s seconds to connect and read data" %(x, datetime.now() - startTime)
        
        read_time = read_time + [datetime.now() - startTime, x, nbd_type]
        write_to_csv("report.csv", read_time)
        if x > 0:
        	sleep_time = random.randint(0,5)
        	sleep(sleep_time)

def simultaneous_connect(ip, server, blockdev, name):
    threads=[]
    for i in xrange(0,16):
    	block = blockdev + str(i)
        server_connect(ip, server, block, name)
        t = threading.Thread(target=server_read, args=(ip, block, i,))
        threads.append(t)
    return threads

def make_log(name):
	make_command = "touch %s"
	subprocess.check_output([make_command])

if __name__ == '__main__':
	password = 'xenroot'
	username = 'root'
	server = args.server
	port = args.port
	blockdev = args.blockdev
	nbd_type = args.nbdtype
	test_type = args.test
	#
	#These are the IPs of the debian VMs I have orchestrated
	local_ips = ["10.62.114.2", "10.62.114.1"]
	dt56_ips = ["10.71.77.216", "10.71.77.89"]
	ips = ["10.62.114.2", "10.62.114.3", "10.62.114.1", "10.71.77.216", "10.71.77.89", "10.62.114.9", "10.62.114.11", "10.62.114.12", "10.62.114.13", "10.62.114.14", "10.62.114.17", "10.62.114.16", "10.62.114.18", "10.62.114.19", "10.62.114.21", "10.62.114.22", "10.62.114.24", "10.62.114.23", "10.62.114.25", "10.62.114.26"]

	#This is the VDI you wish to copy
	vdi ="a07555a0-defe-487e-82a6-38a53ca2e05d"
	name = "nbd://%s:%s@%s/%s" %(username, password, server, vdi)
	threads = []

	#Add headings to logs
	multi = ["IP", "Blockdev"]
	write_to_csv("multi.csv", multi)
	stats = ["CPU%", "Memory (KB)", "Open FDs", "Time"]
	write_to_csv("stats.csv", stats)
	time = ["Connection Time", "Time", "IP", "Blockdev"]
	write_to_csv("time.csv", time)

	#This test will setup the nbd-client and throughput test on a client, not that it can be unreliable
	if test_type == "setup":
		for ip in ips:
			setup_client(ip)
			setup_throughput_tests(ip)


#--------------TEST CASES--------------------------
	#This option runs the nbd-tester-client throughput tests
	if test_type == "tp":
		for ip in local_ips:
			throughput_test(ip, server, blockdev, nbd_type)

	#This rests repeatedly connects, reads and disconnects with one connection per IP to a host. Thread for each IP runs simultaneously but not neccesarily in sync
	if test_type == "repeat":
		log_file = "repeat-%s" %datetime.now()
		#make_log(log_file)
		report = ["Read time", "Time", "IP", "Blockdev", "Total connection time", "Connection no.", "Type"]
		write_to_csv("report.csv", report)

		X ="""
		for ip in local_ips:
			repeated_connect(ip, server, blockdev, name, port, nbd_type)"""

		for ip in local_ips:
			t = threading.Thread(target=repeated_connect, args=(ip, server, blockdev, name, port, nbd_type))
			threads.append(t)
			t.start()
		while t.is_alive():
			host_stats(server, nbd_type)
			sleep(1)

	#This test connects, reads and disconnects with 16 connections per IP to one host simultaneously
	if test_type == "sim":
		for a in xrange(1, 1000):
			print "Trying read with %s simultaneous connections" %a
			write_to_csv("time.csv", ["%s connections" %a])
			threads = [] 
			#for b in xrange(0, len(ips)+1):
			for ip in ips:
				ip_threads = simultaneous_connect(ip, server, "/dev/nbd", name)
				threads = threads + ip_threads
			for t in threads:
				t.start()

			while t.is_alive():
				host_stats(server, nbd_type)
				sleep(1)
			#Adding in sleep to let it settle down		
			sleep(5)

	#This test connects then continuously reads with one connection per IP to a host.
	if test_type == "continual":
		for ip in ips:
			one_connect(ip, server, "/dev/nbd", name)
		while True:
			for ip in ips:
				continual_read(ip, server, "/dev/nbd", name)

