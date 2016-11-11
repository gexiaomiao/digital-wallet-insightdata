
import json
import time
import numpy as np
#import pandas as pd
import csv

class Connection:
	def __init__(self):
		"""
		Initialize the object that store the relation between different id
		"""
		self.map ={}

	def addConnection(self,id_A,id_B):
		"""
		add each records into database
		"""
		if self.map.has_key(id_A) and self.map.has_key(id_B):
			if id_A in self.map[id_B]:
				return
		else:
			if not self.map.has_key(id_A):
				self.map[id_A] = set()
			if not self.map.has_key(id_B):
				self.map[id_B] = set()

		self.map[id_A].add(id_B)
		self.map[id_B].add(id_A)

	def printFriends(id_A):
		if self.map.has_key(id_A):
			print self.map[id_A]
			return True
		else:
			return False

	def LoadBatchData(self,batch_data_file):
		try:
			with open(batch_data_file,"r") as reader:
				next(reader, None) 			#skip the header of the csv file
				failed_count =0
				for i, line in enumerate(reader):
					if i%10000 == 0 and i!=0:
						sys.stdout.write('\rBatch data Process:\t{0}lines'.format(i))
						sys.stdout.flush()
					try:
						line_split = line.split(',')
						id_A,id_B = line_split[1].strip(),line_split[2].strip()
						if id_A.isdigit() and id_B.isdigit():
							self.addConnection(id_A,id_B)
						else:
							failed_count +=1
					except:
						failed_count +=1
					
		except:
			print "can't find the batch_payment file"
			raise

		print "\nBatch data loaded!"
		print "Line reading:\t\t {0} lines sucessed;\t{1} lines failed\n".format(i+1-failed_count,failed_count)

	def TestStreamData(self,stream_data_file,output_data_1,output_data_2,output_data_3):
		try:
			with open(stream_data_file,"r") as reader:


				fw1 = open(output_data_1,'w')
				fw2 = open(output_data_2,'w')
				fw3 = open(output_data_3,'w')

				next(reader, None) 			#skip the header of the csv file
				failed_count =0
				max_time =0
				for i, line in enumerate(reader):
					if i%10000 == 0 and i!=0:
						sys.stdout.write('\rStream data Process:\t{0}lines'.format(i))
						sys.stdout.flush()
					try:
						line_split = line.split(',')
						id_A,id_B = line_split[1].strip(),line_split[2].strip()

						if id_A.isdigit() and id_B.isdigit():
							start_time = time.time()
							# degree1,degree2,degree4 = self.degrees(id_A,id_B)
							degree1 = self.degree1(id_A,id_B)
							degree2 = self.degree2(id_A,id_B)
							degree4 = self.degree4(id_A,id_B)
							max_time = max(max_time,time.time()-start_time)
							if not degree1: # means if A and B are not directly firends
								self.addConnection(id_A,id_B)


							output1 = 'trusted\n' if degree1 else 'unverified\n'
							output2 = 'trusted\n' if degree2 else 'unverified\n'
							output3 = 'trusted\n' if degree4 else 'unverified\n'

							fw1.write(output1)
							fw2.write(output2)
							fw3.write(output3)

						else:
							failed_count +=1

					except:
						failed_count +=1

				fw1.close()
				fw2.close()
				fw3.close()

		except:
			print "can't find the stream_payment file or output files"
			raise 

		print "\nFinished Streaming data testing"
		print "Line reading:\t\t {0} lines sucessed;\t {1} lines failed\n".format(i+1-failed_count,failed_count)
		print "Max latency caused by searching is :\t{0}\tseconds\n".format(max_time)			

	def degree1(self,id_A,id_B):
		if id_A ==id_B:
			return True
		if self.map.has_key(id_A) and self.map.has_key(id_B):
			return id_B in self.map[id_A]


	def degree2(self,id_A,id_B):
		if self.degree1(id_A,id_B):
			return True
		elif self.map.has_key(id_A) and self.map.has_key(id_B):
			A_friends = self.map[id_A]
			B_friends = self.map[id_B]
			if set.intersection(A_friends,B_friends):
				return True
		
		return False


	def degree3(self,id_A,id_B):
		if self.degree2(id_A,id_B):
			return True
		elif self.map.has_key(id_A) and self.map.has_key(id_B):
			A_friends = self.map[id_A]
			B_friends = self.map[id_B]
			if len(A_friends)>len(B_friends):
				A_friends , B_friends = B_friends , A_friends #make sure we expand the smaller set

			A_2_friends =set()
			for friends in A_friends:
				A_2_friends.update(self.map[friends])

			if set.intersection(A_2_friends,B_friends):
				return True

		return False



	def degree4(self,id_A,id_B):
		if self.degree3(id_A,id_B):
			return True
		elif self.map.has_key(id_A) and self.map.has_key(id_B):
			A_friends = self.map[id_A]
			B_friends = self.map[id_B]
			if len(A_friends)>len(B_friends):
				A_friends , B_friends = B_friends , A_friends #make sure we expand the smaller set
			A_2_friends =set()
			for friends in A_friends:
				A_2_friends.update(self.map[friends])

			if len(A_2_friends)<len(B_friends):
				A_2_friends , B_friends = B_friends , A_2_friends #make sure we expand the smaller set

			B_2_friends =set()
			for friends in B_friends:
				B_2_friends.update(self.map[friends])

			if set.intersection(A_2_friends,B_2_friends):
				return True

		return False


	def degrees(self,id_A,id_B):
		if id_A ==id_B:
			return [True,True,True]
		if self.map.has_key(id_A) and self.map.has_key(id_B):
			"""
			first order connection?
			"""
			A_friends = self.map[id_A]
			if id_B in A_friends:
				return [True,True,True]

			"""
			not 1st degree connection
			then construct the friedns of B, then determine
			second order connection?
			"""
			B_friends = self.map[id_B]		
			if set.intersection(A_friends,B_friends):
				return [False,True,True]


			"""
			not 2nd degree connection:
			then construct the 2nd degree friends of A, then determine
			third order connection? 
			"""	
			# later I can optimize that the one with less friend get 2nd degree first


			A_2_friends =set()
			for friends in A_friends:
				A_2_friends.update(self.map[friends])

			if set.intersection(A_2_friends,B_friends):
				return [False,False,True]

			"""
			not 3rd degree connection:
			then construct the 2nd degree friends of B, then determine
			4th order connection? 
			"""

			B_2_friends =set()
			for friends in B_friends:
				B_2_friends.update(self.map[friends])

			if set.intersection(A_2_friends,B_2_friends):
				return [False,False,True]


		"""		
		not above situation will return all false 
		"""
		return [False, False, False]

	def savedatabase(self,direct):
		with open (direct,'w') as fp:
			json.dump(self.map, fp)





def main(batch_data_file,stream_data_file,output_data_1,output_data_2,output_data_3):
	ConnectionList = Connection()
	ConnectionList.LoadBatchData(batch_data_file)
	ConnectionList.TestStreamData(stream_data_file,output_data_1,output_data_2,output_data_3)
	return 

if __name__=='__main__':
	import sys
	main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
						























