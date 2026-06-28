# MVN compile was faiiling 

reason: though the java home was set as 21 the SDKMAN was running in java 17 so explictly pointed to java 21 by using,

export JAVA_HOME=$(/usr/libexec/java_home -v 21)

