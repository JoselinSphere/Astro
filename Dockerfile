FROM quay.io/astronomer/astro-runtime:12.5.0

COPY tnsnames.ora /usr/lib/oracle/tnsnames.ora
 
ENV TNS_ADMIN=/usr/lib/oracle
