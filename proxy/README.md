# database-proxy

## Build & dockerize

Don't forget to change version number.

```bash
true \
  && source .env \
  && ./release.sh -v "0.1.0-2-SNAPSHOT" -i $DOCKER_IMAGE
```
