# opentracker from Alpine's community repository (see README.md for why).
FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
RUN apk add --no-cache opentracker
EXPOSE 6969/tcp 6969/udp
ENTRYPOINT ["opentracker", "-p", "6969", "-P", "6969"]
