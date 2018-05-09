FROM centurylink/ca-certs
ADD build/aq /usr/bin/
ENTRYPOINT ["/usr/bin/aq"]
