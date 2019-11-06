import click
import redis
import json
import logging
import os
@click.command()
@click.argument('action')
@click.option('--search', help="Key search patter. eg *txt")
def main(action, search):
   fileSet = "setFile.txt"
   fileHash = "hashFile.txt"
   fileString = "stringFile.txt"
   exIp = redis.StrictRedis(host='localhost', port=6379, db=0)  # update your redis settings
   imIp = redis.StrictRedis(host='localhost', port=6379, db=3)
   if action == 'dump':
       keylist = []
       value_list = []
       try:
           keylist = exIp.keys(search)
           for key in keylist:
               key_type = exIp.type(key)
               if key_type == "set":
                   dumpSet(key, fileSet, exIp)
               elif key_type == "hash":
                   dumpHash(key, fileHash, exIp)
               elif key_type == "string":
                   dumpString(key, fileString, exIp)
               else:
                   print key
       except Exception as e:
           print e
           print "dump"
   if action == 'load':
       try:
           loadHash(fileHash, imIp)
           loadSet(fileSet, imIp)
           loadString(fileString, imIp)
       except Exception as e:
           print e
def dumpSet(key, filePath, exIp):
   value_list = []
   out = {}
   try:
       with open(filePath, 'a') as f:
           value_set = exIp.smembers(key)
           value_list = list(value_set)
           for value in value_list:
               value = value + os.linesep
               out.update({key: value})
               json.dump(out, f)
               out.update()
               f.write('\n')
               out.clear()
       f.close()
   except Exception as e:
       print "dump set"
       print(e)
   print('Set Type Dump Successful')
def dumpHash(key, filePath, exIp):
   out = {}
   out.update({key: exIp.hgetall(key)})
   try:
       with open(filePath, 'a') as outfile:
           json.dump(out, outfile)
           outfile.write('\n')
           print('Dump Successful')
   except Exception as e:
       print(e)
       print "dump hash"
def dumpString(key, filePath, exIp):
   out = {}
   u = exIp.get(key).decode('utf-8')
   out.update({key: u})
   try:
       with open(filePath, 'a') as outfile:
           json.dump(out, outfile)
           outfile.write('\n')
           print('Dump Successful')
   except Exception as e:
       print(e)
       print "dump string"
def loadSet(fileSet, imIp):
   try:
       f = open(fileSet, "r")
       lines = f.readlines()
       for line in lines:
           data = json.loads(line)
           for key in data:
               imIp.sadd(key, data[key].strip())
       print "data load"
   except Exception as e:
       print e
       print "load set"
def loadHash(fileHash, imIp):
   try:
       f = open(fileHash, "r")
       lines = f.readlines()
       for line in lines:
           data = json.loads(line)
           for key in data:
               for i in data[key]:
                   imIp.hset(key, i, data[key][i])
       print('Data loaded into redis successfully')
   except Exception as e:
       print(e)
       print "load hash"
def loadString(fileString, imIp):
   try:
       f = open(fileString, "r")
       lines = f.readlines()
       for line in lines:
           data = json.loads(line)
           for key in data:
               imIp.getset(key, data.get(key))
       print('Data loaded into redis successfully')
   except Exception as e:
       print(e)
       print "load string"
if __name__ == '__main__':
   log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
   logging.basicConfig(level=logging.INFO, format=log_fmt)
   main()

