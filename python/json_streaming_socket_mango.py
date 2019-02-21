From Pyspark import Sparkcontext, SparkConf
From Pyspark.streaming import streamingcontext
From Pymongo import MongoClient
import json

def Success_rec(data_suc):
	game_Id = (data_suc['gameID'])
	player_Id= (data_suc['playerID'])
	device_Id= (data_suc['deviceID'])
	Identity_type= (data_suc['idtype'])
	Time_Id= (data_suc['created'])
	return(game_Id,player_Id,device_Id,Identity_type,Time_Id)

def Fail_rec(data_fal):
	Type_Id= (data_fal['type'])
	error_data= (data_fal['errormessage'])
	Time_Id= (data_fal['created'])
	return(Type_Id,error_data,Time_Id)

def Success_save(tup):
	game_Id= tup[0]
	player_Id= tup[1]
	device_Id= tup[2]
	identity_type= tup[3]
	Time_Id= tup[4]
	connection= MongoClient()
	test_db= connection.get_database('gamedata')
	success_data=test_db.get_collection('success_logs')
	success_data.Update({"gameID":game_Id},{"playerID":player_Id},{"deviceID":device_Id},
						{"Identity_type":idtype},{"Timestamp":Time_Id},upsert=True)
	Connection.close()

def Fail_save(tupf):
	type= tupf[0]
	error= tupf[1]
	time= tupf[2]
	connection= MongoClient()
	test_db= connection.get_database('gamedata')
	fail_data=test_db.get_collection('fail_logs')
	fail_data.Update({"type":type},{"errormessage":error},{"Timestamp":time},upsert=True)
	
	Connection.close()

if __name__="__main__":
	conf= SparkConf().setAppName("Json Streaming and saving on MongoDB")
	sc= Sparkcontext(conf=conf)
	ssc= streamingcontext(sc,2)
	ssc.checkpoint("checkpoint")
	data_file= ssc.socketTextStream("localhost",9000)
	data= json.load(data_file)
	success= data.filter(lambda X : "2000" in X['status'])
	Success_record= success.map(Success_rec)
	
	Fail= data.filter(lambda X :"4000" in X['status'])
	Fail_record=Fail.map(Fail_rec)
	
	Success_record.ForeachRDD(lambda rdd: rdd.foreach(Success_save))
	Fail_record.ForeachRDD(lambda rdd: rdd.foreach(Fail_save_save))
	
	ssc.start()
	ssc.await termination()