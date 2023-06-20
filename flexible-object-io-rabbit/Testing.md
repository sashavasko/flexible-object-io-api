### Install rabbit in a docker container on a server :
<pre>sudo docker pull rabbitmq:3-management</pre>
### Start rabbit in a docker container on a server :
<pre>sudo docker run -d --hostname test-rabbit-host -P --name test-rabbit rabbitmq:3-management</pre>
### Check rabbit ports : 
<pre>sudo docker ps</pre>
The port bound to 15672/tcp is the management port on that server

The port bound to 5672/tcp is the port that should be used by consumers and producers


### Stop Rabbit : 
<pre>sudo docker stop [ContainerId]</pre>
### Remove container : 
<pre>sudo docker rm [ContainerId]</pre>
