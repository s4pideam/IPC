#!/bin/bash

extract_runtime() {
    local output="$1"
    local runtime=$(echo "$output" | tail -n 1 | grep -oE '[0-9]+(\.[0-9]+)?')
    echo "$runtime"
}

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <#Client> <ChunkSize> <file>"
    exit 1
fi

# Assign arguments to variables
clientNumbers="$1"
chunkSize="$2"
filePath="$3"

# Run the Java command
echo "Simulating Threads"
output=$(java -jar ./target/ipc.jar threads "$clientNumbers" "$chunkSize" "$filePath")
threadRuntime=$(extract_runtime "$output")
echo "$threadRuntime"

echo "Simulating Pipes"
output=$(java -jar ./target/ipc.jar pipes "$clientNumbers" "$chunkSize" "$filePath")
pipesRuntime=$(extract_runtime "$output")
echo "$pipesRuntime"


echo "Simulating NamedPipes"
output=$(java -jar ./target/ipc.jar np "$clientNumbers" "$chunkSize" "$filePath")
namedPipesRuntime=$(extract_runtime "$output")
echo "$namedPipesRuntime"


echo "Simulating TCP"
output=$(java -jar ./target/ipc.jar tcp "$clientNumbers" "$chunkSize" "$filePath")
tcpRuntime=$(extract_runtime "$output")
echo "$tcpRuntime"


echo "Simulating UnixDomainSockets"
output=$(java -jar ./target/ipc.jar uds "$clientNumbers" "$chunkSize" "$filePath")
udsRuntime=$(extract_runtime "$output")
echo "$udsRuntime"


cat <<-'EOF'
 ____                                             
/ ___| _   _ _ __ ___  _ __ ___   __ _ _ __ _   _ 
\___ \| | | | '_ ` _ \| '_ ` _ \ / _` | '__| | | |
 ___) | |_| | | | | | | | | | | | (_| | |  | |_| |
|____/ \__,_|_| |_| |_|_| |_| |_|\__,_|_|   \__, |
                                            |___/ 
EOF

                                                                                                           
echo "Threads: $threadRuntime"
echo "Pipes: $pipesRuntime"
echo "Named Pipes: $namedPipesRuntime"
echo "TCP: $tcpRuntime"
echo "Unix Domain Sockets: $udsRuntime"


if [ ! -f "results.csv" ]; then
    # If it doesn't exist, create it and write the header
    echo "Client Numbers,Chunk,File,Threads,Pipes,Named Pipes,TCP,UDS" >> results.csv
fi
echo "$clientNumbers,$chunkSize,$filePath,$threadRuntime,$pipesRuntime,$namedPipesRuntime,$tcpRuntime,$udsRuntime" >> results.csv
