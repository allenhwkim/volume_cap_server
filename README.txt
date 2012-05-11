A volumecap distribution system consists of the following seven components

  1. primary_master
  2. secondary_master
  3. meta_handler
  4. interval_handler
  5. local_handler
  6. remote_handler
  7. volumecap_server

The above system maintain and serves following data
  1. meta
    volumecap definition; id,start,end,interval,total
    for example, volumecap id 1111 starts at tomorrow and ends at end of this month everyday
    with the total of 100, that means we server amount 100 every day.

  2. interval
    The actual serving unit of meta data. An interval is created every interval(day,week,month)
    and is not valid after the interval is expired.
    for example, id 1111, in the above example, will have interval of today, which starts at 
    the begining of today and ends at the end of today to server 100 for today only.

  3. volumecap
    Volumecap is a slice of interval, which is distributed to many volume cap server.
    Form the above example, a volumecap server will serve 5 out of 100 interval to the clients.
    When 5 is reached to the bottom, it will ask another 5 of interval, and continues to serve.

Primary Master
  . monitor secondary master, and if possible, start the secondary master
  . Duplicate volumecap data updates to the secondary master
  . Four handlers are together with the primary master
    1) meta handler
    2) interval handler
    3) local handler
    4) remote handler
  
Secondary Master
  . monitor primary master, and if it is down, start as the primary master
  . keeps the copy of volumecap data, received from the primary master

Meta Handler
  . Process the requests of meta data update
  . Updatea meta data

Interval Handler
  . Process the requests of intervals, that are from meta handler
  . Creates and updates interval data when meta changes
  . Create intervals automatically by following the interval of meta data
  . Sends requests to local handler to distribute interval updates

Local Handler
  . Process broadcast requests from interval handler
  . Process topup/return requests from volumecap servers
  . Requests more amount interval to the remote handler when it reaches to the bottom

Remote Handler
  . Send local interval updates to other data center remote handlers(through RabbitMQ)
  . Processes interval updates of other data center(through RabbitMQ)
  . Processes topup reqeusts from local handler
  . Processes topup requests from remote handler(through RabbitMQ)

Volumecap Server(s)
  . front-end of volumecap clients.
  . processes interval broadcasts messages from local handler
  . send/receive topup requests to/from local handler
