FROM rocker/shiny

RUN apt-get update && apt-get install --yes git sudo curl libssl-dev libxml2-dev

#install devtools
RUN Rscript -e "install.packages('digest')"
RUN Rscript -e "install.packages('memoise')"
RUN Rscript -e "install.packages('devtools')"
RUN Rscript -e "install.packages('Rcpp')"
RUN Rscript -e "install.packages('plyr')"
RUN Rscript -e "install.packages('bindrcpp')"
RUN Rscript -e "install.packages('stringi')"
RUN Rscript -e "install.packages('reshape2')"
RUN Rscript -e "install.packages('glue')"
RUN Rscript -e "install.packages('tidyselect')"
RUN Rscript -e "install.packages('dplyr')"
RUN Rscript -e "install.packages('tidyr')"
RUN Rscript -e "install.packages('broom')"

RUN Rscript -e "install.packages('dbplyr')"

RUN Rscript -e "install.packages('yaml')"
RUN Rscript -e "install.packages('promises')"
RUN Rscript -e "install.packages('base64enc')"
RUN Rscript -e "install.packages('htmltools')"
RUN Rscript -e "install.packages('backports')"
RUN Rscript -e "install.packages('mime')"
RUN Rscript -e "install.packages('httpuv')"
RUN Rscript -e "install.packages('jsonlite')"
RUN Rscript -e "install.packages('sparklyr')"
#install sequila
RUN Rscript -e "devtools::install_github('ZSI-Bio/bdg-sparklyr-sequila')"

#install spark (installed by .onLoad when package loaded)
RUN Rscript -e "library(sequila)"

#install ggplot2
RUN Rscript -e "install.packages('ggplot2')"

#install ggplot2
RUN Rscript -e "library(ggplot2)"
RUN Rscript -e "library(dplyr)"
RUN Rscript -e "library(shiny)"


#install jdk8
RUN apt-get install --yes gnupg2
##A quick & dirty fix for failing Oracle JDK installer
RUN if [ ! -d /usr/share/man/man1 ]; then  mkdir -p /usr/share/man/man1; fi
RUN \
   echo "===> add webupd8 repository..."  && \
   echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list  && \
   echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list  && \
   apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886  && \
   apt-get update  && \
   \
   \
   echo "===> install Java"  && \
   echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections  && \
   echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections  && \
   cd /var/lib/dpkg/info && \
   DEBIAN_FRONTEND=noninteractive  apt-get install -y --force-yes oracle-java8-installer oracle-java8-set-default  && \
   \
   \
   echo "===> clean up..."  && \
   rm -rf /var/cache/oracle-jdk8-installer  && \
   apt-get clean  && \
   rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

#copy test data
COPY NA12878.slice.bam /tmp/NA12878.slice.bam
COPY warmcache.scala /tmp/warmcache.scala

COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

ENV SPARK_HOME /root/spark/spark-2.2.1-bin-hadoop2.7

ENV BGD_VERSION 0.4-SNAPSHOT
RUN /root/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --packages org.biodatageeks:bdg-sequila_2.11:${BGD_VERSION} \
-i /tmp/warmcache.scala --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

RUN chmod a+x /root
RUN chmod a+x /root/spark
RUN chmod a+x /root/spark/spark-2.2.1-bin-hadoop2.7
RUN chmod a+x /root/spark/spark-2.2.1-bin-hadoop2.7/bin
RUN chmod +x /root/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-shell

USER shiny

RUN /root/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --packages org.biodatageeks:bdg-sequila_2.11:${BGD_VERSION} \
-i /tmp/warmcache.scala --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/

COPY ./myApp/ /srv/shiny-server/myApp/
