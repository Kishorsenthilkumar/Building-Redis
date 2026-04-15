import socket  
import asyncio
import time
database={}

async def handle_client(reader,writer):


    while True:

        data = await reader.read(1024)
        if not data:
            break
        print(f"Raw data received: {data}")

        
        parts=data.split(b"\r\n")
        command=parts[2].lower()
        if len(parts)<3:
            continue

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


        if command==b"xread":

            args=[]
            for i in range(6,len(parts)-1,2):
                args.append(parts[i])

            num_streams=len(args)//2
            stream_keys=args[:num_streams]
            start_ids=args[num_streams:]

            streams_with_data=[]

            for i in range(len(stream_keys)):
                current_key=stream_keys[i]
                current_id=start_ids[i]
                my_stream=database.get(current_key,[])

                ids=current_id.split(b"-")
                start_ms=int(ids[0])
                start_seq=int(ids[1]) 

                matching_entries=[] 
                for entry in my_stream:
                    entry_id=entry["id"].split(b"-")
                    entry_ms=int(entry_id[0])
                    entry_seq=int(entry_id[1])  

                    if (entry_ms,entry_seq)>(start_ms,start_seq):
                        matching_entries.append(entry) 
                if matching_entries:
                        streams_with_data.append((current_key,matching_entries)) 

            if not streams_with_data:
                writer.write(b"$-1\r\n")
                await writer.drain()
                continue   
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





        if command==b"get":
            key=parts[4]
            value=database[key]
            entry=database.get(key)

            if not entry:
                writer.write(b"$-1\r\n")

            else:
                now = time.time()
                expiry = entry.get("expiry_time") # Use the same name you used in SET

                if expiry is not None and now > expiry:
                   # IT'S EXPIRED
                   del database[key]
                   writer.write(b"$-1\r\n")
                else:
                      # IT'S VALID: Get the actual bytes from the dictionary
                  val = entry["value"]
                  response = b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
                  writer.write(response)

        
                  
    
    await writer.drain()
        
          
    writer.close()

async def main():
    
    server = await asyncio.start_server(handle_client,"localhost",6379)

    async with server:
        await server.serve_forever()

    


if __name__ == "__main__":
    asyncio.run(main())
