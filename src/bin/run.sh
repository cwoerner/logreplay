#!/bin/sh


jlibpath=./lib/java/arch

jrunscript=`which jrunscript`; 

if [ ! -z "$jrunscript" -a -x $jrunscript ]; then
    jlibpath=`jrunscript -e 'print(java.lang.System.getProperty("java.library.path"));'`:$jlibpath
else
    echo "overriding java.librar.path: no jrunscript in path ($PATH)"
fi;

java -Djava.library.path=$jlibpath -classpath ./lib/java/*:./src/resources  com.demandbase.logreplay.HAProxyLogReplayer
