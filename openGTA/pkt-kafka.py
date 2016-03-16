import logging
logging.getLogger("scapy.runtime").setLevel(logging.ERROR)
from scapy.all import *
from kurator import zk_broker_list
from kafka import KafkaClient, KeyedProducer, RoundRobinPartitioner, SimpleProducer
from kazoo.client import KazooClient 
from datetime import datetime
import argparse
import json


#defining entrance arguments
def make_parser():

	parser = argparse.ArgumentParser()
	parser.add_argument('-t', '--topic', help='topic to produce to', dest='topic')
	parser.add_argument('-z', '--zookeeper', help='zookeeper server', dest='zookeeper')
	parser.add_argument('-l', '--local', help='print packet instead of sending to kafka', dest='local', action='store_true', default=False)
	parser.add_argument('-d', '--debug', help='enable debug messages', dest='debug', action='store_true', default=False)
	parser.add_argument('-i', '--interface', help='interface to listen on', dest='interface', required=True)
	return parser

#in case of a invalid artument
def valid_args(args):
	if args.topic != None and args.zookeeper != None:
		return True
	else:
		return args.local

#sendig to zookeeper
def produce_callback(packet):
	'''
	callback function executed for each capture packet
	'''
	global packet_count
	global producer
	packet_count += 1
	msg = str(packet.summary)
	packet_jason=parseAndPost(packet)
	res = producer.send_messages(topic,packet_jason)
 #  res = producer.send(topic, packet_count, msg)


	if debug:
		print 'Sent %s' % packet_count
		print formatted(msg)
		print repr(msg)
		print res
	elif packet_count % 100 == 0:
		print 'Sent %s packets' % packet_count


#cleaning the packet
def cleanPacket(p):
	p = str(p)
	# Clean up packet payload from scapy output
	clean = p.split('Raw')[0].split("Padding")[0].replace('|','\n').strip('<')\
		.strip('bound method Ether.show of ').replace('>','').replace('[<','[')\
		.replace('\n<','<').replace('<','\n') 
	try:
		pay=p.split('Raw')[1]
		return cleanPayload(pay)
	except IndexError:
		return 'null'

#clean the payload
def cleanPayload(paylo):
	real=paylo.split("load")
	try:
		part=real[1].split("|")
		payload_final=part[0].split("=")
		return payload_final[1]
	except IndexError:
		return 'null'

	# return clean

def parseAndPost(rawPacket):
		# If we can't parse the packet, we don't want to end the sniffing.
		# Packet will be printed out to the console if there's an error for debugging
		try:
			l2 = rawPacket.summary().split("/")[0].strip()
			l3 = rawPacket.summary().split("/")[1].strip()
			srcIP, dstIP, L7protocol, size, ttl, srcMAC, dstMAC, L4protocol, srcPort, dstPort, payload =\
				"---","---","---","---","---","---","---","---","---","---","---"
			#payload = cleanPayload(rawPacket[0].show)
			if rawPacket.haslayer(Ether):
				srcMAC = rawPacket[0][0].src
				dstMAC = rawPacket[0][0].dst
			elif rawPacket.haslayer(Dot3):
				srcMAC = rawPacket[0][0].src
			 	srcIP = rawPacket[0][0].src
			 	dstMAC = rawPacket[0][0].dst
			 	dstIP = rawPacket[0][0].dst
			 	if rawPacket.haslayer(STP):
			 		L7protocol = 'STP'
			#	 	payload = cleanPayload(rawPacket[STP].show)
			if rawPacket.haslayer(Dot1Q):
				l3 = rawPacket.summary().split("/")[2].strip()
				l4 = rawPacket.summary().split("/")[3].strip().split(" ")[0]
			if rawPacket.haslayer(ARP):
			 	srcMAC = rawPacket[0][0].src
			 	srcIP = rawPacket[0][0].src
			 	dstMAC = rawPacket[0][0].dst
			 	dstIP = rawPacket[0][0].dst
			 	L7protocol = 'ARP'
			 	#payload = cleanPayload(rawPacket[0].show)
			 	payload = cleanPayload(rawPacket[0])
			# else if rawPacket.haslayer(CDP):
			# 	#dostuff
			#else if rawPacket.haslayer(DHCP):
			# 	#dostuff
			# else if rawPacket.haslayer(DHCPv6):
			# 	#dostuff
			elif (rawPacket.haslayer(IP) or rawPacket.haslayer(IPv6)):
				l4 = rawPacket.summary().split("/")[2].strip().split(" ")[0]
				srcIP = rawPacket[0][l3].src
				dstIP = rawPacket[0][l3].dst
				if l3 == 'IP':
					size = rawPacket[0][l3].len
					ttl = rawPacket[0][l3].ttl
				elif l3 == 'IPv6':
					size = rawPacket[0][l3].plen
					ttl = rawPacket[0][l3].hlim
				L7protocol = rawPacket.lastlayer().summary().split(" ")[0].strip()
				if rawPacket.haslayer(ICMP):
					L7protocol = rawPacket.summary().split("/")[2].strip().split(" ")[0]
					payload = rawPacket[ICMP].summary().split("/")[0][5:]
				if rawPacket.haslayer(TCP):
					srcPort = rawPacket[0][l4].sport
					dstPort = rawPacket[0][l4].dport
					L7protocol = rawPacket.summary().split("/")[2].strip().split(" ")[0]
					L4protocol = rawPacket.summary().split("/")[2].strip().split(" ")[0]
				elif rawPacket.haslayer(UDP):
					srcPort = rawPacket[0][l4].sport
					dstPort = rawPacket[0][l4].dport
					L7protocol = rawPacket.summary().split("/")[2].strip().split(" ")[0]
					L4protocol = rawPacket.summary().split("/")[2].strip().split(" ")[0]
			else:
				srcMAC = "<unknown>"
				dstMAC = "<unknown>"
				l4 = "<unknown>"
				srcIP = "<unknown>"
				dstIP = "<unknown>"
				payload = cleanPacket(rawPacket[0].show)
				
			packet = {"timestamp": str(datetime.now())[:-2],\
					"srcIP": str(srcIP),\
					"dstIP": str(dstIP),\
					"L7protocol": str(L7protocol),\
					"size": str(size),\
					"ttl": str(ttl),\
					"srcMAC": str(srcMAC),\
					"dstMAC": str(dstMAC),\
					"L4protocol": str(L4protocol),\
					"srcPort": str(srcPort),\
					"dstPort": str(dstPort),\
					"payload": str(cleanPacket(rawPacket[0].show))\
					}
			
			data=json.dumps(packet)
			return  data
		except:
			# Debug: if packet error, print out the packet to see what failed
			print cleanPayload(rawPacket[0].show)
			return "Packet Issue, review packet printout for problem"


def local_out(packet):
  return parseAndPost(packet) + '\n'

def main():

	parser = make_parser()
	args = parser.parse_args()
	
	if not valid_args(args):
		parser.print_help()
		return

	if args.local:
		sniff(iface=args.interface, store=0, prn=local_out)
		return
	
	zk = KazooClient(args.zookeeper)
	zk.start()
	
	kafka = KafkaClient(zk_broker_list(zk))

	# the sniff callback only takes one parameter which is the packet
	# so everything else must be global
	global producer
	#producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)
	producer = SimpleProducer(kafka)#, async=True)

	global packet_count
	packet_count = 0

	global topic
	topic = args.topic

	global debug
	debug = args.debug

	sniff(iface=args.interface, store=0, prn=produce_callback)


if __name__ == '__main__':
	main()


