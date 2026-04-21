import socket  
import asyncio
import time
import argparse
database={}
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

        
            


async def process_command(parts,writer,database,role,replicas,master_state,my_replica_profile):

      command=parts[2].lower()
      if command==b"set":
                key=parts[4]
                value=parts[6]
                expiry_time=None
                

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

            if not entry:
                writer.write(b"$-1\r\n")

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


        

async def handle_client(reader,writer,role,replicas,master_state):
    
    in_transaction=False
    command_queue=[]

    my_replica_profile={"writer":writer,"offset":0}

    while True:

        data = await reader.read(1024)
        if not data:
            break
        print(f"Raw data received: {data}")

        
        parts=data.split(b"\r\n")
        command=parts[2].lower()
        if len(parts)<3:
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
                writer.write(f"*{len(command_queue)}\r\n".encode())
                

                for comm in command_queue:
                    await process_command(comm,writer,database,role,replicas,master_state,my_replica_profile)
                in_transaction=False
                command_queue=[]

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
            await process_command(parts,writer,database,role,replicas,master_state,my_replica_profile)        

            

        if command==b"ping":
            writer.write(b"+PONG\r\n")

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


    args=parser.parse_args()

    server_port=args.port

    if args.replicaof:
        role="slave"
    else:
        role="master"
    
    replicas=[]
    server = await asyncio.start_server(lambda r,w:handle_client(r,w,role,replicas,master_state),"localhost",server_port)



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

