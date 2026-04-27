import socket  
import asyncio
from tarfile import data_filter
import time
import argparse
import os
import math
import hashlib

database={}
global_channels={}
users={"default":{"password_hash":None}}
global_key_versions={}
master_replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"


async def background_conn(master_reader,master_writer,database):

    offset=0

    while True:

        data=await master_reader.read(1024)

        if data ==b"":
            break
        
        data_list=data.split(b"*")

        for chunk in data_list:

            if chunk==b"":
                continue

            clean_command=b"*"+chunk
            part=clean_command.split(b"\r\n")

            if len(part)>2 and part[2].upper()==b"SET":
                key=part[4]
                value=part[6]

                database[key]={"value":value,"expiry_time":None}

            if len(part) > 4 and part[2].upper() == b"REPLCONF" and part[4].upper() == b"GETACK":

                offset_str=str(offset)
                ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n".encode()
                master_writer.write(ack_response)
                await master_writer.drain()

            offset+=len(clean_command)


def calculate_geohash(longitude, latitude):
    lat_min, lat_max = -85.05112878, 85.05112878
    lon_min, lon_max = -180.0, 180.0
    score = 0
    
    for _ in range(26):
        # 1. Longitude bit
        score <<= 1
        lon_mid = (lon_min + lon_max) / 2
        if longitude >= lon_mid:
            score |= 1
            lon_min = lon_mid
        else:
            lon_max = lon_mid
            
        # 2. Latitude bit
        score <<= 1
        lat_mid = (lat_min + lat_max) / 2
        if latitude >= lat_mid:
            score |= 1
            lat_min = lat_mid
        else:
            lat_max = lat_mid
            
    return score

def decode_geohash(score):
    lat_min, lat_max = -85.05112878, 85.05112878
    lon_min, lon_max = -180.0, 180.0
    
    # Extract bits from 51 down to 0
    for i in range(51, -1, -1):
        bit = (score >> i) & 1
        
        if i % 2 == 1:  # Odd bits belong to Longitude
            lon_mid = (lon_min + lon_max) / 2
            if bit == 1:
                lon_min = lon_mid
            else:
                lon_max = lon_mid
        else:           # Even bits belong to Latitude
            lat_mid = (lat_min + lat_max) / 2
            if bit == 1:
                lat_min = lat_mid
            else:
                lat_max = lat_mid
                
    # The final coordinate is the dead center of the remaining bounding box
    longitude = (lon_min + lon_max) / 2
    latitude = (lat_min + lat_max) / 2
    
    return longitude, latitude

def calculate_distance(lon1, lat1, lon2, lat2):
    # Earth's radius exactly as Redis defines it
    R = 6372797.560856 
    
    # Convert all coordinates from degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c
   


async def process_command(parts,writer,database,role,replicas,master_state,my_replica_profile,server_config,client_subs,global_channels):

      
      command=parts[2].lower()
      


      if command==b"set":
                key=parts[4]
                value=parts[6]
                expiry_time=None

                if key not in global_key_versions:
                    global_key_versions[key]=1
                else:
                    global_key_versions[key]+=1
                

                if len(parts)>8 and parts[8].lower()==b"px":
                    ms_to_live=int(parts[10])
                    expiry_time=time.time()+(ms_to_live/1000)
                database[key]={"value":value,"expiry_time":expiry_time}
                writer.write(b"+OK\r\n")
                await writer.drain()

                propagate_command=b"\r\n".join(parts)
                
                for my_replica_profile in replicas:
                    rep_writer = my_replica_profile["writer"]
                    rep_writer.write(propagate_command)
                    await rep_writer.drain()

                master_state["offset"] += len(propagate_command)

      if command==b"incr":
            key=parts[4]

            record=database.get(key,0)

            if record==0:
                current_value=0
                expiry=None
            else:
                try:
                   current_value=int(record["value"])
                except ValueError:
                    error_msg = b"-ERR value is not an integer or out of range\r\n"
                    writer.write(error_msg)
                    await writer.drain()
                    return
                    

                expiry=record["expiry_time"]
            new_value=current_value+1

            database[key]={"value":str(new_value).encode(),"expiry_time":expiry}

            response=f":{new_value}\r\n".encode()
            writer.write(response)
            await writer.drain()

      if command==b"get":
            key=parts[4]
            value=database.get(key,[])
            entry=database.get(key)
            dir_path=""

            if not entry:

                if server_config["dir"] is None or server_config["dbfilename"] is None:
                    writer.write(b"$-1\r\n")
                    await writer.drain()
                    return
                else:
                   dir_path=os.path.join(server_config["dir"],server_config["dbfilename"])

                if os.path.exists(dir_path):
                    rbd_data=dbfile_manager(dir_path)

                    if key in rbd_data:
                        entry=rbd_data[key]

                        value=entry["value"]
                        expiry=entry["expiry"]

                        if expiry is not None:
                            # Get current time in milliseconds
                            current_time = int(time.time() * 1000)
                            
                            if current_time > expiry:
                                # The key has expired! Send Null Bulk String.
                                writer.write(b"$-1\r\n")
                                await writer.drain()
                                return


                        val_len=len(value)

                        response = b"$" + str(val_len).encode() + b"\r\n" + value + b"\r\n"
                        writer.write(response)
                        await writer.drain()

                    else:
                        response=b"$-1\r\n"
                        writer.write(response)
                        await writer.drain()
                else:
                     response=b"$-1\r\n"
                     writer.write(response)
                     await writer.drain()
                return


            else:
                now = time.time()
                expiry = entry.get("expiry_time") 

                if expiry is not None and now > expiry:
                   del database[key]
                   writer.write(b"$-1\r\n")
                else:
                  val = entry["value"]
                  response = b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                  writer.write(response)

      if command==b"info":

        master_repl="master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        offset="master_repl_offset:0"

        info_text=f"# Replication\r\nrole:{role}\r\n{master_repl}\r\n{offset}"

        response=f"${len(info_text)}\r\n{info_text}\r\n".encode()
        writer.write(response)

      await writer.drain()


      if command==b"replconf" and parts[4].lower()==b"ack":
        my_replica_profile["offset"]=int(parts[6])
        
      
      elif command==b"replconf" and parts[4].lower()!=b"ack":
        writer.write(b"+OK\r\n")
        await writer.drain()
     



      if command==b"psync":
        writer.write(f"+FULLRESYNC {master_replid} 0\r\n".encode())

        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_file=bytes.fromhex(empty_rdb_hex)
        header=f"${str(len(rdb_file))}\r\n".encode()
        response=header+rdb_file
        writer.write(response)
        replicas.append(my_replica_profile)

       

      await writer.drain()


      if command==b"wait":
        no_of_replica=int(parts[4])
        timeout=int(parts[6])

        if master_state["offset"]==0:
            # Fix 1: Properly format the integer length without stringifying the bytes object
            response = f":{len(replicas)}\r\n".encode()
            writer.write(response)
            await writer.drain()
            return

        else:
            for replica_profile in replicas:
                rep_writer=replica_profile["writer"]
                rep_writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
                await rep_writer.drain()

            start_time=time.time()
            while True:
                count=0

                for replica_profile in replicas:
                   # Ensure offset comparison is integer-based
                   if int(replica_profile["offset"]) >= master_state["offset"]:
                      count += 1
                
                
                if count >= no_of_replica:
                    break
                
                if timeout > 0 and (time.time() - start_time) * 1000 >= timeout:
                    break
                
               
                await asyncio.sleep(0.05)

           
            response=f":{count}\r\n".encode()
            writer.write(response)
            await writer.drain()

      if command==b"config":
        if parts[4].lower()==b"get":

             key=parts[6].lower().decode()
             key_len=len(parts[6])

             ans=server_config[key]
             ans_len=len(ans)

             if key=="" or ans=="":
                response=b"*0\r\n"
                writer.write(response)
                await response.drain()

             response=f"*2\r\n${key_len}\r\n{key}\r\n${ans_len}\r\n{ans}\r\n".encode()
             writer.write(response)
             await writer.drain()
       
      if command==b"keys" and parts[4]==b"*":

        if server_config["dir"] is None or server_config["dbfilename"] is None:
                writer.write(b"*0\r\n")
                await writer.drain()
        else:
            dir_path=os.path.join(server_config["dir"],server_config["dbfilename"])

            my_keys=dbfile_manager(dir_path)

            response = f"*{len(my_keys)}\r\n".encode()

            for k in my_keys.keys():
                    response += b"$" + str(len(k)).encode() + b"\r\n" + k + b"\r\n"

            writer.write(response)
            await writer.drain()


      if command==b"subscribe":

            for i in range(4,len(parts)-1,2):

                key=parts[i]
                key_len=len(key)

                channel_name=parts[i]
                client_subs.add(channel_name)

                if  key not in global_channels:
                    global_channels[key]=[]
                    
                if writer not in global_channels[key]:
                    global_channels[key].append(writer)

                sub_len=len(client_subs)

                response=b"*3\r\n$9\r\nsubscribe\r\n$"+str(key_len).encode()+b"\r\n"+key+b"\r\n:"+str(sub_len).encode()+b"\r\n"
                writer.write(response)
                await writer.drain()
     
      if command==b"publish":

          channel_name=parts[4]
          message_content=parts[6]

          if channel_name in global_channels:
              no_sub=len(global_channels[channel_name])
              channel_len=len(channel_name)
              msg_len=len(message_content)

              for listener_conn in global_channels[channel_name]:
                  content = b"*3\r\n$7\r\nmessage\r\n$" + str(channel_len).encode() + b"\r\n" + channel_name + b"\r\n$" + str(msg_len).encode() + b"\r\n" + message_content + b"\r\n"
                  listener_conn.write(content)
                  await listener_conn.drain()

          else:
             no_sub=0
        
          response=b":"+str(no_sub).encode()+b"\r\n"
          writer.write(response)
          await writer.drain()


      if command==b"unsubscribe":

        for i in range(4,len(parts)-1,2):
            channel_name=parts[i]
            channel_len=len(channel_name)

            if channel_name in client_subs:
                client_subs.remove(channel_name)
                

            if channel_name in global_channels and writer in global_channels[channel_name]:
                    global_channels[channel_name].remove(writer)

            sub_len=len(client_subs)
            response=b"*3\r\n$11\r\nunsubscribe\r\n$"+str(channel_len).encode()+b"\r\n"+channel_name+b"\r\n:"+str(sub_len).encode()+b"\r\n"
            writer.write(response)
            await writer.drain()
            
      if command==b"zadd":

              key=parts[4]
              scores=parts[6].decode()
              member=parts[8]

              if key not in database:
                database[key]=[]
              
              score=float(scores)
              added_count=1

              for i,data in enumerate(database[key]):
                if member == data[1]:
                    database[key][i]=(score,member)
                    added_count=0
                    break
              if added_count==1:
                 database[key].append((score,member))


              database[key].sort()
              response=b":"+str(added_count).encode()+b"\r\n"
              writer.write(response)
              await writer.drain()

      if command==b"zrank":

        key=parts[4]
        member=parts[6]

        if key not in database:
            response=b"$-1\r\n"
            writer.write(response)
            await writer.drain()
            return

        for index,data in enumerate(database[key]):

            if member==data[1]:
                response=b":"+str(index).encode()+b"\r\n"
                writer.write(response)
                await writer.drain()
                return
        response = b"$-1\r\n"
        writer.write(response)
        await writer.drain()
   

      if command==b"zrange":

        key=parts[4]
        start=int(parts[6])
        stop=int(parts[8])

        if key not in database:
            response=b"*0\r\n"
            writer.write(response)
            await writer.drain()
            return

        L=len(database[key])
        if stop<0:
            stop=stop+L
            if stop<0:
                stop=0

        if start<0:
            start=start+L
            if start<0:
                start=0

        member_list=database[key][start:stop+1]
        response=b"*"+str(len(member_list)).encode()+b"\r\n"

        for score_mem in member_list:
            member=score_mem[1]
            response+=b"$"+str(len(member)).encode()+b"\r\n"+member+b"\r\n"
        writer.write(response)
        await writer.drain()


      if command==b"zcard":

        key=parts[4]

        if key not in database:
            response=b":0\r\n"
            writer.write(response)
            await writer.drain()
            return

        count=len(database[key])
        response=b":"+str(count).encode()+b"\r\n"
        writer.write(response)
        await writer.drain()

      if command==b"zscore":

        key=parts[4]
        member=parts[6]

        if key not in database:
            response=b"$-1\r\n"
            writer.write(response)
            await writer.drain()
            return

        for index,data in enumerate(database[key]):

            if member==data[1]:
                score=str(data[0]).encode()
                response=b"$"+str(len(score)).encode()+b"\r\n"+score+b"\r\n"
                writer.write(response)
                await writer.drain()
                return

        response=b"$-1\r\n"
        writer.write(response)
        await writer.drain()

      if command==b"zrem":

        key=parts[4]
        member=parts[6]

        if key not in database:
            response=b"$-1\r\n"
            writer.write(response)
            await writer.drain()
            return

        for index,data in enumerate(database[key]):
            if member==data[1]:
                database[key].pop(index)
                response=b":1\r\n"
                writer.write(response)
                await writer.drain()
                return
        
        response=b":0\r\n"
        writer.write(response)
        await writer.drain()

      if command==b"geoadd":

        key=parts[4]
        lon=parts[6].decode()
        lat=parts[8].decode()
        member=parts[10]

        longitude=float(lon)
        latitude=float(lat)
        

        if longitude<-180.0 or longitude>180.0 or latitude<-85.05112878 or latitude>85.05112878:
                response=b"-ERR invalid longitude,latitude pair\r\n"
                writer.write(response)
                await writer.drain()
                return

        
        if key not in database:
            database[key]=[]
              
        score=calculate_geohash(longitude, latitude)
        added_count=1

        for i,data in enumerate(database[key]):
             if member == data[1]:
                    database[key][i]=(score,member)
                    added_count=0
                    break
        if added_count==1:
            database[key].append((score,member))

        database[key].sort()
        response = b":" + str(added_count).encode() + b"\r\n"
        writer.write(response)
        await writer.drain()

      if command==b"geopos":

        key=parts[4]
        members=[parts[i] for i in range(6,len(parts)-1,2)]
        response=b"*"+str(len(members)).encode()+b"\r\n"

        if key not in database:
            for _ in members:
              response+=b"*-1\r\n"
            writer.write(response)
            await writer.drain()
            return

        else:
            for mem in members:
                found=False
                for index,data in enumerate(database[key]):

                    if mem==data[1]:
                        found=True
                        score=int(data[0])

                        lon, lat = decode_geohash(score)
                        lon_str = str(lon)
                        lat_str = str(lat)

                        lon_bytes = lon_str.encode()
                        lat_bytes = lat_str.encode()
                        break

                if found==True:
                    response+=b"*2\r\n"
                    response += b"$" + str(len(lon_bytes)).encode() + b"\r\n" + lon_bytes + b"\r\n"
                    response += b"$" + str(len(lat_bytes)).encode() + b"\r\n" + lat_bytes + b"\r\n"
                else:
                    response+=b"*-1\r\n"
            writer.write(response)
            await writer.drain()

     
      if command==b"geodist":

        key=parts[4]
        member1 = parts[6]
        member2 = parts[8]

        if key not in database:
                writer.write(b"$-1\r\n")
                await writer.drain()
                return

        score1 = None
        score2 = None

        for data in database[key]:
                if data[1] == member1:
                    score1 = int(data[0])
                if data[1] == member2:
                    score2 = int(data[0])

                if score1 is not None and score2 is not None:
                    break

        if score1 is None or score2 is None:
                writer.write(b"$-1\r\n")
                await writer.drain()
                return
        lon1, lat1 = decode_geohash(score1)
        lon2, lat2 = decode_geohash(score2)

        distance = calculate_distance(lon1, lat1, lon2, lat2)
        dist_str = f"{distance:.4f}".encode()

        response = b"$" + str(len(dist_str)).encode() + b"\r\n" + dist_str + b"\r\n"
        writer.write(response)
        await writer.drain()


      if command==b"geosearch":

        key=parts[4]
        long=parts[8].decode()
        lat=parts[10].decode()
        rad=parts[14].decode()
        uni=parts[16].decode()

        cen_long=float(long)
        cen_lat=float(lat)
        radius=float(rad)
        unit=uni.lower()

        radius_meters = 0.0

        if unit == "m":
          radius_meters = radius 
        elif unit == "km":
            radius_meters = radius * 1000.0 
        elif unit == "mi":
            radius_meters = radius * 1609.34 
        elif unit == "ft":
            radius_meters = radius * 0.3048

        if key not in database:
            response=b"*0\r\n"
            writer.write(response)
            await writer.drain()
            return

        matching_members=[]
        for data in database[key]:

            score=int(data[0])
            long,lat=decode_geohash(score)

            distance=calculate_distance(long,lat,cen_long,cen_lat)

            if distance<=radius_meters:
                matching_members.append(data[1])

        response=b"*"+str(len(matching_members)).encode()+b"\r\n"

        for member in matching_members:

            response+=b"$"+str(len(member)).encode()+b"\r\n"+member+b"\r\n"
        writer.write(response)
        await writer.drain()



      if command==b"acl":

        if parts[4].lower()==b"whoami":
            response=b"$7\r\ndefault\r\n"
            writer.write(response)
            await writer.drain()

        if parts[4].lower()==b"getuser":
            username=parts[6]

            if users["default"]["password_hash"] is None:
                 response=b"*4\r\n"+b"$5\r\nflags\r\n"+b"*1\r\n$6\r\nnopass\r\n"+b"$9\r\npasswords\r\n"+b"*0\r\n"
            else:
                password=users["default"]["password_hash"]
                response=b"*4\r\n"+b"$5\r\nflags\r\n"+b"*0\r\n"+b"$9\r\npasswords\r\n"+b"*1\r\n$64\r\n"+password.encode()+b"\r\n"


            writer.write(response)
            await writer.drain()

        if parts[4].lower()==b"setuser":

            password=parts[8][1:]
            hashed_pass=hashlib.sha256(password).hexdigest()
            users["default"]["password_hash"]=hashed_pass

            writer.write(b"+OK\r\n")
            await writer.drain()
    

#helper for rdbfile key retrival
def read_length(rdbfile):

    first_byte = rdbfile.read(1)[0]
    first_two_bits = first_byte >> 6

    if first_two_bits == 0:
        return first_byte & 0x3F

    elif first_two_bits == 1:
        next_byte = rdbfile.read(1)[0]
        return ((first_byte & 0x3F) << 8) | next_byte

    elif first_two_bits == 2:
        return int.from_bytes(rdbfile.read(4), "big")

    elif first_two_bits == 3:
        # This is the special flag that says "an integer is coming, not a string"
        return (True, first_byte & 0x3F)


def read_string(rdbfile):
    length_data = read_length(rdbfile)

    if isinstance(length_data, tuple) and length_data[0] is True:
        format_type = length_data[1]

        if format_type == 0:
            return str(rdbfile.read(1)[0]).encode()
        elif format_type == 1:
            return str(int.from_bytes(rdbfile.read(2), "little")).encode()
        elif format_type == 2:
            return str(int.from_bytes(rdbfile.read(4), "little")).encode()

    return rdbfile.read(length_data)


#key=dir ans=rdbfile name
def  dbfile_manager(dir_path):

    with open(dir_path,"rb") as file:

        head=file.read(9)
        rdb_data={}

        while True:

            byte=file.read(1)

            if byte==b"":
                break

            if byte[0] == 251:
                hash_table_size = read_length(file)
                expiry_size = read_length(file)

                for _ in range(hash_table_size):
                   first_byte = file.read(1)

                   if first_byte == b"":
                        break
                   expiry_time = None

                   if first_byte[0] == 252:
                        expiry_bytes = file.read(8)
                        expiry_time = int.from_bytes(expiry_bytes, "little")
                        
                        # We still need to read 1 more byte to clear the value type flag!
                        value_type = file.read(1)
                    
                   elif first_byte[0] == 253:
                        expiry_bytes = file.read(4)
                        expiry_time = int.from_bytes(expiry_bytes, "little") * 1000
                        value_type = file.read(1)

                   else:
                        value_type = first_byte
                   key = read_string(file)
                   value = read_string(file)
                    
                   rdb_data[key] = {"value": value, "expiry": expiry_time}
                    

                break
        
    return rdb_data


        


        

async def handle_client(reader,writer,role,replicas,master_state,server_config,global_channels):
    
    in_transaction=False
    command_queue=[]
    watched_keys={}

    my_replica_profile={"writer":writer,"offset":0}
    client_subs=set()

    if users["default"]["password_hash"] is None:
        is_authenticated=True
    else:
        is_authenticated=False
 

    while True:

        data = await reader.read(1024)
        if not data:
            break
        print(f"Raw data received: {data}")

        
        parts=data.split(b"\r\n")
        command=parts[2].lower()

        if is_authenticated==False and command != b"auth":
            response=b"-NOAUTH Authentication required.\r\n"
            writer.write(response)
            await writer.drain()
            continue

        if in_transaction and command not in [b"exec", b"multi", b"discard"]:

           if command==b"watch":
              response=b"-ERR WATCH inside MULTI is not allowed\r\n"
              writer.write(response)
              await writer.drain()
              continue
 
           command_queue.append(parts)
           writer.write(b"+QUEUED\r\n")
           await writer.drain()
           continue          

        if command==b"auth":

            username=parts[4].decode()
            password=parts[6]
            hashed_pass=hashlib.sha256(password).hexdigest()

            if username not in users or hashed_pass is None or hashed_pass!=users[username]["password_hash"]:
                response=b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
            else:
               response=b"+OK\r\n"
               is_authenticated = True
        
            writer.write(response)
            await writer.drain()
            continue
            


        if len(parts)<3:
            continue
        vip=[b"subscribe",b"unsubscribe",b"psubscribe",b"punsubscribe",b"ping",b"quit"]

        if len(client_subs) > 0 and command not in vip:

            vipcommand=command.decode()    
            response=f"-ERR can't execute '{(vipcommand)}'\r\n".encode()
            writer.write(response)
            await writer.drain()

            continue

        if in_transaction and command not in [b"exec",b"multi",b"discard"]:
            command_queue.append(parts)
            writer.write(b"+QUEUED\r\n")
            await writer.drain()
            continue

        if command == b"multi":
            in_transaction=True
            writer.write(b"+OK\r\n")
            await writer.drain()
        
        elif command==b"exec":
            if not in_transaction:
                writer.write(b"-ERR EXEC without MULTI\r\n")
            else:
                
                abort_transaction=False
                for key,version in watched_keys.items():

                    if version!=global_key_versions[key]:
                        abort_transaction=True
                        break
                    
                if abort_transaction==True:
                    writer.write(b"*-1\r\n")
                else:
                    writer.write(f"*{len(command_queue)}\r\n".encode())
                    for comm in command_queue:
                        await process_command(comm, writer, database, role, replicas, master_state, my_replica_profile, server_config, client_subs, global_channels)
                in_transaction = False
                watched_keys.clear()
                command_queue = []

            await writer.drain()



        elif command==b"discard":
              
              if not in_transaction:
                writer.write(b"-ERR DISCARD without MULTI\r\n")
              else:
                writer.write(b"+OK\r\n")
                in_transaction=False
                command_queue=[]

              await writer.drain()

        else:
            await process_command(parts,writer,database,role,replicas,master_state,my_replica_profile,server_config,client_subs,global_channels)        



        if command==b"watch":
            key=parts[4]
            

            if key not in global_key_versions:
                global_key_versions[key]=0
            
            watched_keys[key]=global_key_versions[key]

            response=b"+OK\r\n"
            writer.write(response)
            await writer.drain()





        if command==b"ping":

            if len(client_subs)>0:
                  response=b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"
                  writer.write(response)
                  await writer.drain()
            else: 
               writer.write(b"+PONG\r\n")
               await writer.drain()

        if command==b"rpush":
            keys=parts[4]
            

            if keys not in database:
                database[keys]=[]
            for i in range(6,len(parts)-1,2):
                element=parts[i]
                database[keys].append(element)

            lis_len=len(database[keys])
            response=b":"+str(lis_len).encode()+b"\r\n"
            writer.write(response)
            await writer.drain()


        if command==b"lrange":
            keys=parts[4]
            start=int(parts[6])
            stop=int(parts[8])

            my_list=database.get(keys,[])
            
            if stop==-1:
                result_slice=my_list[start:]
            else:
               result_slice=my_list[start:stop+1]
            response=f"*{len(result_slice)}\r\n".encode()

            for item in result_slice:

                response+=b"$"+str(len(item)).encode()+b"\r\n"+item+b"\r\n"
            
            writer.write(response)
            await writer.drain()

        if command==b"lpush":
            keys=parts[4]

            if keys not in database:
                database[keys]=[]

            for i in range(6,len(parts)-1,2):
                element=parts[i]
                database[keys].insert(0,element)

            lis_len=len(database[keys])
            response=b":"+str(lis_len).encode()+b"\r\n"
            writer.write(response)
            await writer.drain()
            
        if command==b"llen":
            keys=parts[4]
            lis_len=len(database.get(keys,[]))
            
            if keys not in database:
                response=b":"+str(0).encode()+b"\r\n"
                writer.write(response)
                await writer.drain()
            else:
               response=b":"+str(lis_len).encode()+b"\r\n"
               writer.write(response)
               await writer.drain()
 
        if command==b"lpop":
            keys=parts[4]
            list_key=database.get(keys,[])

            if len(parts)>7:
               stop=int(parts[6].decode())
               pop_list=[]

               for _ in range(min(stop,len(list_key))):
                  elements=list_key.pop(0)
                  pop_list.append(elements)
            
               response=f"*{len(pop_list)}\r\n".encode()

               for item in pop_list:
                 if isinstance(item,str):
                    item=item.encode()
                 response+=b"$"+str(len(item)).encode()+b"\r\n"+item+b"\r\n"
               writer.write(response)
            
            else:
                if not list_key:
                    writer.write(b"$-1\r\n")
                else:
                    item = list_key.pop(0)
                    if isinstance(item, str): item = item.encode()
                    writer.write(b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n")

        await writer.drain()

        if command==b"blpop":
            keys=parts[4]
            timeout=float(parts[6].decode())
            start_time = asyncio.get_event_loop().time()

            while True:
                my_list=database.get(keys,[])

                if my_list:
                    item=my_list.pop(0)
                    response=b"*2\r\n"
                    response+=b"$"+str(len(keys)).encode()+b"\r\n"+keys+b"\r\n"
                    response+=b"$"+str(len(item)).encode()+b"\r\n"+item+b"\r\n"

                    writer.write(response)
                    await writer.drain()
                    break
                elapsed = asyncio.get_event_loop().time() - start_time
                if timeout > 0 and elapsed >= timeout:
                     # Return Null Array after timeout
                     writer.write(b"*-1\r\n")
                     await writer.drain()
                     break

                await asyncio.sleep(0.05)

        if command == b"xadd":
            stream_key = parts[4]
            raw_id = parts[6]
            
            if stream_key not in database:
                database[stream_key] = []

            # 1. Get last ID from database FIRST (needed for all generation types)
            last_ms, last_seq = -1, -1
            if database[stream_key]:
                last_entry = database[stream_key][-1]
                last_id_parts = last_entry["id"].split(b"-")
                last_ms, last_seq = int(last_id_parts[0]), int(last_id_parts[1])

            # 2. Handle FULL Auto-generation (*)
            if raw_id == b"*":
                new_ms = int(time.time() * 1000)
                if new_ms == last_ms:
                    new_seq = last_seq + 1
                else:
                    new_seq = 0
                raw_id = f"{new_ms}-{new_seq}".encode()
            
            # 3. Handle Partial (1-*) or Explicit (1-1)
            else:
                stream_id = raw_id.split(b"-")
                new_ms = int(stream_id[0])
                new_seq_raw = stream_id[1]

                if new_seq_raw == b"*":
                    if new_ms == 0:
                        new_seq = 1
                    elif new_ms == last_ms:
                        new_seq = last_seq + 1
                    else:
                        new_seq = 0
                    raw_id = f"{new_ms}-{new_seq}".encode()
                else:
                    new_seq = int(new_seq_raw)

            # 4. Rule: 0-0 Check
            if new_ms == 0 and new_seq == 0:
                writer.write(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                await writer.drain()
                continue

            # 5. Rule: Strictly Incremental Check
            if (new_ms, new_seq) <= (last_ms, last_seq):
                writer.write(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                await writer.drain()
                continue

            # 6. Parse Entry Data
            entry_data = {}
            for i in range(8, len(parts) - 1, 4):
                if i + 2 < len(parts):
                    entry_data[parts[i]] = parts[i+2]
            
            database[stream_key].append({"id": raw_id, "values": entry_data})

            # 7. Success Response
            response = b"$" + str(len(raw_id)).encode() + b"\r\n" + raw_id + b"\r\n"
            writer.write(response)
            await writer.drain()

        if command==b"xrange":

           stream_key=parts[4]
           start_id=parts[6]
           stop_id=parts[8]

           if start_id==b"-":
              start_ms=-1
              start_seq=-1
           else:
                s_parts=start_id.split(b"-")
                start_ms=int(s_parts[0])

                if len(s_parts)>1:
                    start_seq=int(s_parts[1])
                else:
                    start_seq=0

           if stop_id==b"+":
                stop_ms=float('inf')
                stop_seq=float('inf')
           else:
                e_parts=stop_id.split(b"-")
                stop_ms=int(e_parts[0])

                if len(e_parts)>1:
                   stop_seq=int(e_parts[1])
                else:
                   stop_seq=float('inf')

           my_stream=database.get(stream_key,[])
           filtered_entires=[]

           for entry in my_stream:
              entry_id=entry["id"].split(b"-")
              entry_ms=int(entry_id[0])
              entry_seq=int(entry_id[1])

              if (start_ms, start_seq) <= (entry_ms, entry_seq) <= (stop_ms, stop_seq):
                filtered_entires.append(entry)

           response=f"*{len(filtered_entires)}\r\n".encode()

           for entry in filtered_entires:
              response+=b"*2\r\n"
              response+=b"$"+str(len(entry["id"])).encode() + b"\r\n" + entry["id"] + b"\r\n"

              kv_list=[]
              for k, v in entry["values"].items():
                kv_list.append(k)
                kv_list.append(v)
            
              response += f"*{len(kv_list)}\r\n".encode()

              for item in kv_list:
                response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
        
           writer.write(response)
           await writer.drain()
        
        if command == b"xread":
            timeout = None
            args = []
            
            # 1. Parse arguments (handle BLOCK vs no BLOCK)
            if parts[4].lower() == b"block":
                timeout = int(parts[6])
                for i in range(10, len(parts) - 1, 2):
                   args.append(parts[i])
            else:
                for i in range(6, len(parts) - 1, 2):
                  args.append(parts[i])

            num_streams = len(args) // 2
            stream_keys = args[:num_streams]
            start_ids = args[num_streams:]


            for i in range(len(start_ids)):
                if start_ids[i]==b"$":
                    my_stream=database.get(stream_keys[i],[])
                    if my_stream:
                        latest_stream=my_stream[-1]
                        latest_id=latest_stream["id"]
                        start_ids[i]=latest_id
                    else:
                        start_ids[i]=b"0-0"


            start_time = asyncio.get_event_loop().time()
            
            # 2. The Waiting Room
            while True:
                streams_with_data = []

                for i in range(len(stream_keys)):
                    current_key = stream_keys[i]
                    current_id = start_ids[i]
                    
                    # Grab stream from the global database
                    my_stream = database.get(current_key, [])

                    ids = current_id.split(b"-")
                    start_ms = int(ids[0])
                    start_seq = int(ids[1]) 
 
                    matching_entries = [] 
                    for entry in my_stream:
                        entry_id = entry["id"].split(b"-")
                        entry_ms = int(entry_id[0])
                        entry_seq = int(entry_id[1])  

                        # Only keep entries strictly GREATER than the requested ID
                        if (entry_ms, entry_seq) > (start_ms, start_seq):
                            matching_entries.append(entry) 
                            
                    if matching_entries:
                        streams_with_data.append((current_key, matching_entries)) 

                # 3. Exit Conditions
                if streams_with_data:
                    break

                if timeout == None:
                    break
               
                if timeout > 0:
                    current_time = asyncio.get_event_loop().time()
                    elapsed_time = (current_time - start_time) * 1000

                    if elapsed_time >= timeout:
                        if not streams_with_data:
                            break
                        
                # Pause slightly to let XADD run in the background
                await asyncio.sleep(0.1)

            # 4. Build and Send Response
            if not streams_with_data:
                # If no data found at all, send Null Bulk String
                response = b"*-1\r\n"
                writer.write(response)
                await writer.drain()

            else:
                # If we found data, build the complex nested RESP array
                response = f"*{len(streams_with_data)}\r\n".encode()

                for stream_key, entries in streams_with_data:
                    response += b"*2\r\n"
                    response += b"$" + str(len(stream_key)).encode() + b"\r\n" + stream_key + b"\r\n"
                    
                    response += f"*{len(entries)}\r\n".encode()

                    for entry in entries:
                        response += b"*2\r\n"
                        response += b"$" + str(len(entry["id"])).encode() + b"\r\n" + entry["id"] + b"\r\n"
                        
                        kv_list = []
                        for k, v in entry["values"].items():
                            kv_list.append(k)
                            kv_list.append(v)
                        
                        response += f"*{len(kv_list)}\r\n".encode()
                        for item in kv_list:
                            if isinstance(item, str): item = item.encode()
                            response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
               
                writer.write(response)
                await writer.drain()
        
        

        if command==b"echo":
            argument=parts[4]
            length=len(argument)

            response=b"$"+str(length).encode()+b"\r\n"+argument+b"\r\n"
            writer.write(response)
            #ensures data is moved from buffer to client
            await writer.drain() 
                



        if command==b"type":
            keys=parts[4]


            if keys not in database:
                writer.write(b"+none\r\n")
            else:
                value=database[keys]

                if isinstance(value,list):
                    if len(value)>0 and isinstance(value[0],dict):
                        writer.write(b"+stream\r\n")

                    else:
                        writer.write(b"+list\r\n")
                    
                    
                else:
                    writer.write(b"+string\r\n")
            await writer.drain()

        






       
        
                  
    
    await writer.drain()
        
          
    writer.close()

async def main():

    master_state={"offset":0}
    
    parser=argparse.ArgumentParser()
    parser.add_argument("--port",default=6379,type=int)
    parser.add_argument("--replicaof",nargs="+")
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")


    args=parser.parse_args()

    server_config={"dir":args.dir,"dbfilename":args.dbfilename}

    server_port=args.port

    if args.replicaof:
        role="slave"
    else:
        role="master"
    
    replicas=[]
    server = await asyncio.start_server(lambda r,w:handle_client(r,w,role,replicas,master_state,server_config,global_channels),"localhost",server_port)



    if args.replicaof:
        rep=args.replicaof[0].split(" ")
        master_port=rep[1]
        hostname=rep[0]

        master_reader,master_writer=await asyncio.open_connection(hostname,master_port)

        

        ping_command=b"*1\r\n$4\r\nPING\r\n"
        master_writer.write(ping_command)
        await master_writer.drain()
        await master_reader.read(1024)

        replconf=f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(args.port))}\r\n{args.port}\r\n".encode()
        master_writer.write(replconf)
        await master_writer.drain()
        await master_reader.read(1024)

        capa="*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode()
        master_writer.write(capa)
        await master_writer.drain()
        await master_reader.read(1024)

        psync=b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_writer.write(psync)
        await master_writer.drain()

        await master_reader.readuntil(b"\n")
        rdb_length_line = await master_reader.readuntil(b"\n")
        rdb_length = int(rdb_length_line[1:-2])
        await master_reader.readexactly(rdb_length)
        

        asyncio.create_task(background_conn(master_reader,master_writer,database))



    
    

    
    

    async with server:
        await server.serve_forever()

    


if __name__ == "__main__":
    asyncio.run(main())

