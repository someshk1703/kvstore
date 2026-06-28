# Setting java 21 and alias for java 17 and java 8

you set-up java 21, 
nano ~/.zshrc
## pasted the below,
export NVM_DIR="$HOME/.nvm"
[ -s "/opt/homebrew/opt/nvm/nvm.sh" ] && \. "/opt/homebrew/opt/nvm/nvm.sh"
[ -s "/opt/homebrew/opt/nvm/etc/bash_completion.d/nvm" ] && \. "/opt/homebrew/opt/nvm/etc/bash_completion.d/nvm"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

#Set default Java to 21 using the dynamic utility
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"

#Quick-switch shortcuts (Aliases)
alias java8="export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8); java -version"
alias java17="export JAVA_HOME=\$(/usr/libexec/java_home -v 17); java -version"
alias java21="export JAVA_HOME=\$(/usr/libexec/java_home -v 21); java -version"

#Added by Antigravity
export PATH="/Users/a3123264/.antigravity/antigravity/bin:$PATH"

#THIS MUST BE AT THE END OF THE FILE FOR SDKMAN TO WORK!!!
export SDKMAN_DIR="$HOME/.sdkman"
[[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
export PATH="$HOME/.local/bin:$PATH"

then we do, 
source ~/.zshrc

we can run java21, java17 or java8 as command to switch branches.

# Building maven 

mvn archetype:generate \ 
-DgroupId=com.somesh \ 
-DartifactId=kvstore \ 
-DarchetypeArtifactId=maven-archetype-quickstart \ 
-DinteractiveMode=false

## Removing pre-built app.java and appTest.java

rm -rf src/main/java/com/somesh/App.java
rm -rf src/test/java/com/somesh/AppTest.java

# Creating our own directory

mkdir - p mkdir -p src/main/java/com/somesh/kvstore/{engine,memory,persistence,protocol,server,replication,cluster} 

point to note is we can send array of values to create multiple directories together.

# Use touch to create files 

touch src/main/java/com/somesh/kvstore/engine/KVStore.java
touch src/main/java/com/somesh/kvstore/engine/ValueEntry.java
touch src/main/java/com/somesh/kvstore/engine/CommandExecutor.java

touch src/main/java/com/somesh/kvstore/memory/LRUCache.java
touch src/main/java/com/somesh/kvstore/memory/ExpiryManager.java

touch src/main/java/com/somesh/kvstore/persistence/AOFWriter.java
touch src/main/java/com/somesh/kvstore/persistence/SnapshotManager.java
touch src/main/java/com/somesh/kvstore/persistence/CrashRecovery.java

touch src/main/java/com/somesh/kvstore/protocol/CommandParser.java
touch src/main/java/com/somesh/kvstore/protocol/ResponseSerializer.java
touch src/main/java/com/somesh/kvstore/protocol/Command.java

touch src/main/java/com/somesh/kvstore/server/TcpServer.java
touch src/main/java/com/somesh/kvstore/server/ClientHandler.java

touch src/main/java/com/somesh/kvstore/replication/ReplicationManager.java
touch src/main/java/com/somesh/kvstore/replication/ReplicaConnection.java
touch src/main/java/com/somesh/kvstore/replication/RingBuffer.java

touch src/main/java/com/somesh/kvstore/cluster/ConsistentHashRing.java

## then again 

mkdir -p src/main/resources                         
mkdir -p data
mkdir -p logs

touch .gitigore

