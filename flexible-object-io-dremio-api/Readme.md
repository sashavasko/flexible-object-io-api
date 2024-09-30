### Testing

Make sure Docker is installed rootless :
https://docs.docker.com/engine/security/rootless/
run it with binding to local port:
```
export DOCKERD_ROOTLESS_ROOTLESSKIT_FLAGS="-p 0.0.0.0:2375:2375/tcp"
dockerd-rootless.sh -H tcp://0.0.0.0:2375
```

```
docker pull dremio/dremio-oss
docker run -p 9047:9047 -p 31010:31010 -p 45678:45678 dremio/dremio-oss
```

Add the following to services section of /opt/dremio/conf/dremio.conf file :

```
flight: {
    enabled: true,
    port: 32010,
    auth.mode: "legacy.arrow.flight.auth"
  }
```
